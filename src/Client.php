<?php

namespace MarkKimsal\Mqtt;

use Amp\Uri\Uri;
use Amp\Deferred;
use Amp\Promise;
use Amp\Success;
use Evenement\EventEmitterTrait;
use Evenement\EventEmitterInterface;
use Monolog\Logger;
use Monolog\Handler\NullHandler;
use function Amp\call;


class Client implements EventEmitterInterface {
	use EventEmitterTrait;

	/** @var Deferred[] */
	protected $deferreds;

	/** @var Deferred[] */
	protected $deferredsById = [];

	/** @var Connection */
	protected $connection;

	/** @var array */
	protected $topicList = [];

	/** @var int */
	protected $timeout = 0;

	/** @var string */
	public $clientId = '';

	/** @var array */
	protected $queue = [];

	/** @var logger */
	protected $logger;

	protected $connackReceived    = false;
	protected $isConnected        = false;
	protected $connackPromisor    = null;
	protected $autoAckPublish     = true;
	protected $enableCleanSession = false;
	protected $username           = '';
	protected $password           = '';

	public function __construct(string $uri, ?Logger $logger = null) {

		if($logger == null){
			$this->logger = new Logger("MQTTNULLLOGGER");
			$this->logger->pushHandler(new NullHandler());
		} else {
			$this->logger = $logger;
		}
		
		$this->applyUri($uri);

		$this->deferreds = [];

		$this->connection = new Connection($uri);
		

		$this->connection->on("response", function ($response) {
			if ($pid = $response->getId()) {
				$this->logger->debug("D/Client: Message ($pid) got: ".get_class($response));
				$deferred = $this->deferredsById[$pid];
				//must unset here because
				//some packets send new packets with same ID
				//during their onResolve
				unset($this->deferredsById[$pid]);
			} else {
				$this->logger->debug("D/Client: Response is untracked deferred: ".get_class($response));
				$deferred = array_shift($this->deferreds);
			}

			if ($response->isFailure() || $response instanceof \Throwable) {
				$deferred->fail($response);
			} else {
				$deferred->resolve($response);
			}
		});

		$this->connection->on("message", function ($response) {
			$this->emit('message', [$response]); //up the chain

			if ($this->autoAckPublish) {
				$this->acknowledge($response);
			}
		});

		$this->connection->on('close', function (\Throwable $error = null) {
			if ($this->connackPromisor) {
				$this->connackPromisor->fail(new \Exception('closing socket'));
			}
			$this->isConnected     = false;
			$this->connackReceived = false;
			
			// Fail any outstanding promises
			while ($this->deferreds) {
				/** @var Deferred $deferred */
				$deferred = array_shift($this->deferreds);
				if ($error) {
					$deferred->fail($error);
				} else {
					$deferred->fail(new \Exception("Connection closed"));
				}
			}
		});

		$this->connection->on('error', function (\Throwable $error = null) {
			if ($error) {
				// Fail any outstanding promises
				while ($this->deferreds) {
					/** @var Deferred $deferred */
					$deferred = array_shift($this->deferreds);
					$deferred->fail($error);
				}
			}
		});

		if (count($this->topicList) && !empty($this->topicList)) {
			$this->connection->on("connect", function () {
				$promiseList = $this->subscribeToAll($this->topicList, function($err, $resp) {
					#$this->logger->debug("Got subscribe to all response.\n";
				});
			});
		}

		$this->connection->on("open", function () {
			//$this->logger->debug("D/Client: socket is open.\n";
		});

		$this->connection->on("connect", function ($response) {
			//$this->logger->debug("D/Client: connack received: ".get_class($response));
			$this->connackReceived = true;
			$this->isConnected     = true;
			$this->connackPromisor->resolve();
			$this->connackPromisor = null;
			$this->flushQueue();
		});
	}

	public function connect($callback = NULL) {
		if ($this->connackPromisor) {
			return $this->connackPromisor->promise();
		}
		if ($this->isConnected) {
			return new Success();
		}

		$this->connackPromisor = new Deferred();
		$p = $this->connackPromisor->promise();

		$connPromise = $this->connection->connect();
		$connPromise->onResolve(function ($err, $result) use ($callback){
			if ($err) {
				$connackPromisor = $this->connackPromisor;
				$this->connackPromisor = null;
				$connackPromisor->fail(new \Exception('socket failed: '. $err));
				return;
			}
			$packet = new Packet\Connect();
			if (!$this->enableCleanSession && !$this->clientId) {
				$this->logger->debug("W/Client: Establishing a session without a clientId is not allowed. Enabling clean session");
				$this->enableCleanSession = true;
			}
			if ($this->username) {
				$packet->setUsername($this->username);
			}
			if ($this->password) {
				$packet->setPassword($this->password);
			}

			if ($this->clientId) {
				$packet->setClientId($this->clientId);
			}
			if ($this->timeout) {
				$packet->setTimeout($this->timeout);
			}
			if ($this->enableCleanSession) {
				$packet->withCleanSession();
			}

			$packet->setVersion311();

			$this->send($packet , $callback);
		});
		return $p;
	}

	public function subscribeToAll($topics, $callback = NULL) {
		if (!is_array($topics)) {
			$topics = array($topics);
		}

		$promiseList = [];
		foreach ($topics as $t) {
			$promiseList[] = $this->subscribe($t, $callback);
		}
		return $promiseList;
	}

	public function subscribe($topic, $callback = NULL) {
		$packet = new Packet\Subscribe();
		$packet->setTopic($topic);
		return $this->send( $packet , $callback);
	}

	public function publish($msg, $topic, $qos=0, $callback=NULL) {
		if (! $msg instanceof Packet\Publish) {
			$packet = new Packet\Publish();
			$packet->setMessage($msg);
			$packet->setQos($qos);
		} else {
			$packet = $msg;
		}
		$packet->setTopic($topic);
		if ($qos < 1) {
			return $this->sendAndForget( $packet , $callback );

		} else if ($qos == 1) {
			return $this->send($packet, $callback);
			
		} else if ($qos == 2) {
			$client = $this;

			$deferred = new Deferred();
			//wrap final callback in pubrel auto-generating callback
			$sendp = $this->send( $packet , function($err, $result) use($client, $deferred, $callback) {
				if ($err) {
					$callback($err);
					$deferred->fail($err);
					return;
				}
				$packet = new Packet\Pubrel();
				$packet->setId( $result->getId() );
				$pubcomp = $client->send( $packet );
				$pubcomp->onResolve(function($err, $result) use ($deferred) {
					if ($err) {
						$deferred->fail($err);
					} else {
						$deferred->resolve($result);
					}
				});
			});
			$qosPromise = $deferred->promise();
			if($callback != null){
				$qosPromise->onResolve($callback);
			}
			return $qosPromise;
		}
		
	}

	public function publishRetain($msg, $topic, $qos=0, $callback=NULL) {
		$packet = new Packet\Publish();
		$packet->setMessage($msg);
		$packet->setRetain(true);
		return $this->publish($packet, $topic, $qos, $callback);
	}

	public function acknowledge($packet) {
		$qos = $packet->getQos();
		if ($qos == 0) {
			return;
		}
		if ($qos == 1) {
			$response = new Packet\Puback();
			$response->setId($packet->getId());
			$this->sendAndForget($response);
		}
		if ($qos == 2) {
			$response = new Packet\Pubrec();
			$response->setId($packet->getId());
			$client = $this;
			$this->send($response, function($err, $result) use($client) {
				if ($err) {
					throw new \Exception($err);
				}
				$packet = new Packet\Pubcomp();
				$packet->setId( $result->getId() );
				$client->sendAndForget( $packet );
			});
		}
	}

	private function applyUri(string $uri) {
		$newuri = new Uri($uri);
		if (strlen($newuri->getQueryParameter("topics"))) {
			$this->topicList = explode(',', $newuri->getQueryParameter("topics"));
		}
		$this->clientId  = $newuri->getQueryParameter("clientId");
		$this->timeout   = (int)$newuri->getQueryParameter("timeout");

		$this->enableCleanSession = (bool)$newuri->hasQueryParameter("cleanSession");
		$this->username           = $newuri->getQueryParameter("username");
		$this->password           = $newuri->getQueryParameter("password");
	}

	public function enableAutoAck() {
		$this->autoAckPublish = true;
	}

	public function disableAutoAck() {
		$this->autoAckPublish = false;
	}

	private function sendAndForget($packet, callable $callback = null): Promise {
		if (! $this->isConnected) {
			$this->connect();
		}
		if (! $this->connackReceived && !($packet  instanceof Packet\Connect)) {
			$d = new Deferred();
			$p = $d->promise();
			if ($callback) {
				$p->onResolve($callback);
			}
			$this->queue[] = [$packet, $callback, $d];
			return $p;

		}
		if($pid = $packet->getId()) {
			$this->logger->debug("D/Client: Message ($pid) sending: ".get_class($packet));
		}
		$p = $this->_asyncsend($packet);
		if ($callback) {
			$p->onResolve($callback);
		}
		return $p;
	}

	public function send($packet, callable $callback = null): Promise {
		if (! $this->isConnected) {
			$this->connect();
		}

		$deferred = new Deferred();
		if($pid = $packet->getId()) {

			$this->logger->debug("D/Client: Message ($pid) sending: ".get_class($packet));
			$this->deferredsById[$pid] = $deferred;
		} else {
			$this->logger->debug("D/Client: Adding untracked deferred for packet: ". get_class($packet));
			$this->deferreds[] = $deferred;
		}
		$promise = $deferred->promise();
		if ($callback) {
			$promise->onResolve($callback);
		}

		if (! $this->connackReceived && !($packet  instanceof Packet\Connect)) {
			$this->queue[] = [$packet, $callback];
			return $promise;
		}

		$this->_asyncsend($packet, $promise);
		return $promise;
	}

	/**
	 * Send packets that were queued before we got CONACK
	 * If they are packets which will not have any ack then we
	 * will resolve their deferreds right here after sending.
	 */
	protected function flushQueue() {
		foreach ($this->queue as $_idx => $_struct) {
			$p = $this->_asyncsend($_struct[0]);

			//sometimes we have fire and forget packets for which
			//we will never get a response, just resolve these.
			if (isset($_struct[2]) && $_struct[2] instanceof Deferred) {
				$def = $_struct[2];
				$def->resolve(null, null);
			}
			unset($this->queue[$_idx]);
		}
	}

	protected function _asyncsend($packet, $promise=NULL) {
		return call(function () use ($packet, $promise) {
			yield $this->connection->send($packet);
			if ($promise instanceof \Amp\Promise) yield $promise;
		});
	}
}

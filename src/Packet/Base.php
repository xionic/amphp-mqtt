<?php

namespace MarkKimsal\Mqtt\Packet;

use function MarkKimsal\Mqtt\dumphex;

class Base {

	public $id = '';
	private static $idsInUse = [];

	public function fromNetwork($hdr, $data) {
	}

	public function setId($id) {
		$this->id = $id;
		return $this->getId();
	}

	public function getId() {
		return $this->id;
	}

	public function isFailure() {
		return FALSE;
	}

	public function dumphex($data) {
		dumphex($data);
	}

	public static function genId(){
		$tryCount = 0;
		while(in_array(($id = rand(1,65535)), self::$idsInUse)){
			$tryCount++;
			if($tryCount > 65535){
				throw new \Exception("MQTT Base could not generate an ID - all in use"); 
			}
		}
		return $id;
	}

	/**
	 * Encode length in dynamic byte packed format
	 */
	public function encodeLength($len) {
		$bytes = '';
		do {
			$encoded = $len % 128;
			$len     = floor($len / 128);
			if ($len > 0 ) {
				$encoded = $encoded | 0x80;
			}
			$bytes .= pack('c', $encoded);
		} while ($len > 0);
		return $bytes;
	}
}

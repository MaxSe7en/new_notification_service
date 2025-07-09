<?php
require 'vendor/autoload.php';

$client = new Predis\Client([
    'scheme' => 'tcp',
    'host'   => '192.168.1.51',//127.0.0.1
    'port'   => 6379,
]);

try {
    echo $client->ping();
} catch (Exception $e) {
    echo "Redis error: " . $e->getMessage();
}

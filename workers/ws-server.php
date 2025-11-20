<?php
require_once __DIR__ . '/../vendor/autoload.php';

use Ratchet\Http\HttpServer;
use Ratchet\WebSocket\WsServer;
use Ratchet\Server\IoServer;
use App\Workers\RatchetServer;

$server = IoServer::factory(
    new HttpServer(
        new WsServer(
            new RatchetServer()
        )
    ),
    9502
);

echo "Started Notification Worker...\n";
$server->run();


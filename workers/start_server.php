<?php
// File: /workers/websocket_server.php
require_once __DIR__ . '/../vendor/autoload.php';

$server = new App\Workers\WebSocketServer([
    // 'ssl_cert_file' => '/path/to/cert.pem',
    // 'ssl_key_file' => '/path/to/key.pem',
    // Other custom configs
]);

$server->start();

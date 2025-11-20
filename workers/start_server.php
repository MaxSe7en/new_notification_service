<?php
require_once __DIR__ . '/../vendor/autoload.php';

use App\Workers\WebSocketServer;
// File: /workers/websocket_server.php

$server = new WebSocketServer([
    // 'ssl_cert_file' => '/path/to/cert.pem',
    // 'ssl_key_file' => '/path/to/key.pem',
    // Other custom configs
]);

$server->start();

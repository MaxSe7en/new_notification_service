<?php
// File: /bin/websocket-server.php
require_once __DIR__ . '/../vendor/autoload.php';

use App\Workers\ReactWebSocketServer;

// Configuration
$config = [
    'host' => $_ENV['WS_HOST'] ?? '0.0.0.0',
    'port' => $_ENV['WS_PORT'] ?? 9502,
    'heartbeat_idle_time' => 120,
    'heartbeat_check_interval' => 60,
    'max_connections' => 1024,
];

// Create and start server
$server = new ReactWebSocketServer($config);
$server->start();

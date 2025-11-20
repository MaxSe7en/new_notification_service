<?php
// File: /workers/test_autoload.php
require __DIR__ . '/../vendor/autoload.php';

use App\Workers\WebSocketServer;

if (class_exists(WebSocketServer::class)) {
    echo "Class loaded successfully!\n";
} else {
    echo "Class still not found\n";
}


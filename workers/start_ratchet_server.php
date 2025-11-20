<?php
// File: /workers/start_ratchet_server.php
require_once __DIR__ . '/../vendor/autoload.php';
error_reporting(E_ALL);
ini_set('display_errors', '1');
use App\Workers\RatchetWebSocketServer;

// Set up error handling
set_error_handler(function ($severity, $message, $file, $line) {
    throw new ErrorException($message, 0, $severity, $file, $line);
});

// Handle graceful shutdown
pcntl_signal(SIGTERM, function () {
    echo "Received SIGTERM, shutting down gracefully...\n";
    exit(0);
});

pcntl_signal(SIGINT, function () {
    echo "Received SIGINT, shutting down gracefully...\n";
    exit(0);
});

try {
    $server = new RatchetWebSocketServer([
        'host' => '0.0.0.0',
        'port' => 9502,
        // 'ssl_cert' => '/etc/letsencrypt/live/winsstarts.com/fullchain.pem',
        // 'ssl_key' => '/etc/letsencrypt/live/winsstarts.com/privkey.pem',
        // 'enable_ssl' => true,
    ]);

    echo "Starting Ratchet WebSocket server...\n";
    $server->start();
    
} catch (Exception $e) {
    echo "Server failed to start: " . $e->getMessage() . "\n";
    exit(1);
}

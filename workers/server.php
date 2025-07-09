<?php
// server.php
require_once __DIR__ . '/../vendor/autoload.php';

use Ratchet\MessageComponentInterface;
use Ratchet\ConnectionInterface;
use Ratchet\Server\IoServer;
use Ratchet\Http\HttpServer;
use Ratchet\WebSocket\WsServer;

class MyWebSocketServer implements MessageComponentInterface {
    protected $clients;

    public function __construct() {
        $this->clients = new \SplObjectStorage; // Stores all connected clients
        echo "WebSocket server started...\n";
    }

    public function onOpen(ConnectionInterface $conn) {
        $this->clients->attach($conn); // Add new connection to the list
        echo "New connection! ({$conn->resourceId})\n";
    }

    public function onMessage(ConnectionInterface $from, $msg) {
        // Example: Broadcast message to all connected clients
        foreach ($this->clients as $client) {
            if ($from !== $client) { // Don't send to the sender
                $client->send($msg);
            }
        }
        echo "Connection {$from->resourceId} sent: {$msg}\n";
    }

    public function onClose(ConnectionInterface $conn) {
        $this->clients->detach($conn); // Remove connection from the list
        echo "Connection {$conn->resourceId} has disconnected\n";
    }

    public function onError(ConnectionInterface $conn, \Exception $e) {
        echo "An error occurred on connection {$conn->resourceId}: {$e->getMessage()}\n";
        $conn->close();
    }
}

// Run the server
$port = 9502; // Choose your desired port
$server = IoServer::factory(
    new HttpServer(
        new WsServer(
            new MyWebSocketServer()
        )
    ),
    $port
);

echo "Listening on port {$port}\n";
$server->run();

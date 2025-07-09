<?php
// server.php
require_once __DIR__ . '/../vendor/autoload.php';

use Ratchet\MessageComponentInterface;
use Ratchet\ConnectionInterface;
use Ratchet\Server\IoServer;
use Ratchet\Http\HttpServer;
use Ratchet\WebSocket\WsServer;
use React\EventLoop\Loop;
use React\Redis\Factory;
use React\Socket\SocketServer;

class MyNotificationServer implements MessageComponentInterface {
    protected \SplObjectStorage $clients; // Stores all connected WebSocket clients
    protected array $userConnections; // Maps user IDs to ConnectionInterface objects

    public function __construct() {
        $this->clients = new \SplObjectStorage;
        $this->userConnections = []; // Initialize the user ID to connection map
        echo "Notification WebSocket server started...\n";
    }

    /**
     * Called when a new client connects to the WebSocket server.
     *
     * @param ConnectionInterface $conn The new connection.
     */
    public function onOpen(ConnectionInterface $conn) {
        $this->clients->attach($conn); // Add new connection to the list of all clients
        echo "New connection! ({$conn->resourceId})\n";
        // At this point, the user is connected but not yet identified.
        // They are expected to send a 'register' message soon.
    }

    /**
     * Called when a message is received from a client.
     *
     * @param ConnectionInterface $from The connection from which the message was received.
     * @param string $msg The message content.
     */
    public function onMessage(ConnectionInterface $from, $msg) {
        // Attempt to decode the message as JSON. Notifications typically use structured data.
        $data = json_decode($msg, true); // Decode as associative array

        if (json_last_error() !== JSON_ERROR_NONE) {
            echo "Received invalid JSON from {$from->resourceId}: {$msg}\n";
            $from->send(json_encode(['status' => 'error', 'message' => 'Invalid JSON format']));
            return;
        }

        // Check for a 'type' field to determine the action
        if (!isset($data['action'])) {
            echo "Message from {$from->resourceId} missing 'type' field: {$msg}\n";
            $from->send(json_encode(['status' => 'error', 'message' => 'Message type missing']));
            return;
        }

        switch ($data['action']) {
            case 'register':
                // Client wants to register their user ID with this connection
                if (isset($data['userId'])) {
                    $userId = (string) $data['userId'];
                    // Store the connection associated with the user ID
                    $this->userConnections[$userId] = $from;
                    // Optionally, store the user ID on the connection object for easy lookup on close
                    $from->userId = $userId;
                    echo "User '{$userId}' registered with connection {$from->resourceId}\n";
                    $from->send(json_encode(['status' => 'success', 'message' => 'Registered as ' . $userId]));
                } else {
                    echo "Register message from {$from->resourceId} missing 'userId'\n";
                    $from->send(json_encode(['status' => 'error', 'message' => 'User ID missing for registration']));
                }
                break;

            case 'ping':
                // Respond to a client ping to keep the connection alive
                $from->send(json_encode(['type' => 'pong']));
                echo "Received ping from {$from->resourceId}, sent pong.\n";
                break;

            case 'send_notification':{
                // Client wants to send a notification to a specific user
                if (isset($data['userId']) && isset($data['message'])) {
                    $userId = (string) $data['userId'];
                    $message = (string) $data['message'];

                    // Check if the user is connected
                    if (isset($this->userConnections[$userId])) {
                        $connection = $this->userConnections[$userId];
                        // Send the notification to the specified user
                        $notificationPayload = json_encode([
                            'type' => 'notification',
                            'message' => $message,
                            'timestamp' => time()
                        ]);
                        $connection->send($notificationPayload);
                        echo "Sent notification to user '{$userId}' (Connection {$connection->resourceId}): {$message}\n";
                    } else {
                        echo "User '{$userId}' not found or not connected.\n";
                        $from->send(json_encode(['status' => 'error', 'message' => 'User not connected']));
                    }
                } else {
                    echo "send_notification message from {$from->resourceId} missing 'userId' or 'message'\n";
                    $from->send(json_encode(['status' => 'error', 'message' => 'User ID or message missing']));
                }
                break;
            }
            default:
                // Handle unknown message types or log them
                echo "Received unknown message type '{$data['type']}' from {$from->resourceId}: {$msg}\n";
                $from->send(json_encode(['status' => 'error', 'message' => 'Unknown message type']));
                break;
        }
    }

    /**
     * Called when a client disconnects.
     *
     * @param ConnectionInterface $conn The disconnected connection.
     */
    public function onClose(ConnectionInterface $conn) {
        $this->clients->detach($conn); // Remove connection from the list of all clients

        // If the connection had a registered user ID, remove it from the userConnections map
        if (isset($conn->userId) && isset($this->userConnections[$conn->userId])) {
            unset($this->userConnections[$conn->userId]);
            echo "Connection {$conn->resourceId} (User '{$conn->userId}') has disconnected\n";
        } else {
            echo "Connection {$conn->resourceId} has disconnected\n";
        }
    }

    /**
     * Called when an error occurs on a connection.
     *
     * @param ConnectionInterface $conn The connection where the error occurred.
     * @param \Exception $e The exception that occurred.
     */
    public function onError(ConnectionInterface $conn, \Exception $e) {
        echo "An error occurred on connection {$conn->resourceId}: {$e->getMessage()}\n";
        $conn->close(); // Close the connection on error
    }

    /**
     * Sends a notification message to a specific user.
     * This method can be called externally (e.g., from a Redis subscriber).
     *
     * @param string $userId The ID of the user to send the notification to.
     * @param string $message The notification message content.
     * @return bool True if the notification was sent, false otherwise (user not found).
     */
    public function sendNotificationToUser(string $userId, string $message): bool
    {
        if (isset($this->userConnections[$userId])) {
            $connection = $this->userConnections[$userId];
            $notificationPayload = json_encode([
                'type' => 'notification',
                'message' => $message,
                'timestamp' => time()
            ]);
            $connection->send($notificationPayload);
            echo "Sent notification to user '{$userId}' (Connection {$connection->resourceId}): {$message}\n";
            return true;
        } else {
            echo "User '{$userId}' not found or not connected.\n";
            return false;
        }
    }
}

$port = 9502; // Choose your desired port for WebSocket connections

// Get the ReactPHP event loop
$loop = Loop::get();

// Create a socket server
$socket = new SocketServer("0.0.0.0:$port", [], $loop);

// Create the WebSocket server instance
$notificationServer = new MyNotificationServer();

// Create the IoServer with the socket server
$webSock = new IoServer(
    new HttpServer(
        new WsServer(
            $notificationServer // Pass our custom notification server component
        )
    ),
    $socket // Pass the socket server to IoServer
);

echo "WebSocket server listening on port {$port}\n";

// Run the event loop
$loop->run();

echo "WebSocket server listening on port {$port}\n";
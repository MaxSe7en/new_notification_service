<?php
// File: /workers/WebSocketServer.php
namespace App\Workers;

use Ratchet\MessageComponentInterface;
use Ratchet\ConnectionInterface;
use Ratchet\Server\IoServer;
use Ratchet\Http\HttpServer;
use Ratchet\WebSocket\WsServer;
use Ratchet\RFC6455\Messaging\MessageInterface;
use React\EventLoop\Loop;
use React\Socket\Server as ReactServer;
use App\Services\RedisService;
use App\Config\DatabaseAccessors;
use App\Models\NotificationModel;
use App\Exceptions\Console;
use Predis\Client;

class ReactWebSocketServer implements MessageComponentInterface
{
    private $clients;
    private $userConnections;
    private RedisService $redisService;
    private NotificationModel $notificationModel;
    private array $config;
    private $loop;
    private $heartbeatTimers;
    private $server;

    public function __construct(array $config = [])
    {
        $this->config = array_merge([
            'host' => '0.0.0.0',
            'port' => 9502,
            'heartbeat_idle_time' => 120, // seconds
            'heartbeat_check_interval' => 60, // seconds
            'max_connections' => 1024,
        ], $config);

        $this->clients = new \SplObjectStorage();
        $this->userConnections = [];
        $this->heartbeatTimers = [];
        $this->redisService = new RedisService();
        $this->notificationModel = new NotificationModel();
        $this->loop = Loop::get();
        
        $this->initializeRedisStructures();
        $this->verifyKeyTypes();
    }

    public function start(): void
    {
        $this->verifyKeyTypes();
        
        // Create the WebSocket server
        $wsServer = new WsServer($this);
        $httpServer = new HttpServer($wsServer);
        
        // Create React socket server
        $reactServer = new ReactServer($this->config['host'] . ':' . $this->config['port'], $this->loop);
        $this->server = new IoServer($httpServer, $reactServer, $this->loop);

        // Setup periodic timers
        $this->setupTimers();

        // Register server instance
        $this->registerServerInstance();

        Console::info("WebSocket server started on ws://{$this->config['host']}:{$this->config['port']}");
        
        // Start the server
        $this->server->run();
    }

    public function onOpen(ConnectionInterface $conn): void
    {
        try {
            $userId = $this->validateAndGetUserId($conn);
            
            // Store connection
            $this->clients->attach($conn);
            $this->userConnections[$userId] = $conn;
            $conn->userId = $userId;
            $conn->lastActivity = time();

            // Store connection in Redis
            $this->redisService->executeWithRetry(function ($client) use ($userId, $conn) {
                $client->hset('ws_connections', $userId, $conn->resourceId);
                $client->hset('ws_connection_map', $conn->resourceId, $userId);
                $client->setex("ws_connection:{$conn->resourceId}", $this->config['heartbeat_idle_time'], $userId);
            });

            // Start heartbeat timer for this connection
            $this->startHeartbeat($conn);

            // Send initial data
            $this->sendInitialData($userId, $conn);

            Console::info("New connection: User {$userId} connected");

        } catch (\Exception $e) {
            Console::error("Connection error: " . $e->getMessage());
            $conn->close();
        }
    }

    public function onMessage(ConnectionInterface $from, $msg): void
    {
        try {
            $data = json_decode($msg, true);
            if (!$data) {
                throw new \InvalidArgumentException("Invalid JSON format");
            }

            $userId = $from->userId ?? null;
            if (!$userId) {
                $from->close();
                return;
            }

            // Update connection activity
            $from->lastActivity = time();
            $this->redisService->executeWithRetry(function ($client) use ($from) {
                $client->expire("ws_connection:{$from->resourceId}", $this->config['heartbeat_idle_time']);
            });

            $this->handleMessage($data, $from, $userId);

        } catch (\Exception $e) {
            Console::error("Message handling error: " . $e->getMessage());
            $from->send(json_encode([
                'type' => 'error',
                'message' => 'Internal server error'
            ]));
        }
    }

    public function onClose(ConnectionInterface $conn): void
    {
        try {
            $userId = $conn->userId ?? null;
            
            // Remove from local storage
            $this->clients->detach($conn);
            if ($userId && isset($this->userConnections[$userId])) {
                unset($this->userConnections[$userId]);
            }

            // Clear heartbeat timer
            if (isset($this->heartbeatTimers[$conn->resourceId])) {
                $this->loop->cancelTimer($this->heartbeatTimers[$conn->resourceId]);
                unset($this->heartbeatTimers[$conn->resourceId]);
            }

            // Remove from Redis
            $this->redisService->executeWithRetry(function ($client) use ($conn, $userId) {
                if ($userId) {
                    $client->hdel('ws_connections', $userId);
                }
                $client->hdel('ws_connection_map', $conn->resourceId);
                $client->del("ws_connection:{$conn->resourceId}");
            });

            Console::info("Connection closed: User {$userId} disconnected");

        } catch (\Exception $e) {
            Console::error("Connection close error: " . $e->getMessage());
        }
    }

    public function onError(ConnectionInterface $conn, \Exception $e): void
    {
        Console::error("WebSocket error: " . $e->getMessage());
        $conn->close();
    }

    private function setupTimers(): void
    {
        // Health check timer
        $this->loop->addPeriodicTimer($this->config['heartbeat_check_interval'], function () {
            $this->checkConnectionHealth();
        });

        // Process queued notifications timer
        $this->loop->addPeriodicTimer(5, function () {
            $this->processQueuedNotifications();
        });

        // Cleanup stale servers timer
        $this->loop->addPeriodicTimer(60, function () {
            $this->cleanupStaleServers();
        });
    }

    private function startHeartbeat(ConnectionInterface $conn): void
    {
        $this->heartbeatTimers[$conn->resourceId] = $this->loop->addPeriodicTimer(30, function () use ($conn) {
            if ($conn->getState() === ConnectionInterface::STATE_OPEN) {
                $conn->send(json_encode(['type' => 'ping']));
            } else {
                if (isset($this->heartbeatTimers[$conn->resourceId])) {
                    $this->loop->cancelTimer($this->heartbeatTimers[$conn->resourceId]);
                    unset($this->heartbeatTimers[$conn->resourceId]);
                }
            }
        });
    }

    private function validateAndGetUserId(ConnectionInterface $conn): int
    {
        $queryString = $conn->httpRequest->getUri()->getQuery();
        parse_str($queryString, $query);
        
        if (!isset($query['userId']) || !is_numeric($query['userId'])) {
            throw new \InvalidArgumentException("Invalid user ID");
        }

        $userId = (int) $query['userId'];

        // Optional: Add additional validation (e.g., token verification)

        return $userId;
    }

    private function sendInitialData(int $userId, ConnectionInterface $conn): void
    {
        try {
            // Send connection acknowledgement
            $conn->send(json_encode([
                'type' => 'connection',
                'status' => 'connected',
                'message' => 'WebSocket connection established'
            ]));

            // Send initial notification count
            $counts = $this->notificationModel->getNotificationCounts((string) $userId);
            $conn->send(json_encode([
                'type' => 'notification_count',
                'data' => $counts
            ]));

        } catch (\Exception $e) {
            Console::error("Initial data error: " . $e->getMessage());
        }
    }

    private function handleMessage(array $data, ConnectionInterface $conn, int $userId): void
    {
        switch ($data['action'] ?? '') {
            case 'ping':
                $conn->send(json_encode(['type' => 'pong']));
                break;

            case 'get_notifications':
                $counts = $this->notificationModel->getNotificationCounts((string) $userId);
                $conn->send(json_encode([
                    'type' => 'notification_count',
                    'data' => $counts
                ]));
                break;

            case 'mark_read':
                if (isset($data['notification_id'])) {
                    $this->markNotificationRead($userId, $data['notification_id']);
                }
                break;

            default:
                $conn->send(json_encode([
                    'type' => 'error',
                    'message' => 'Unknown action'
                ]));
        }
    }

    private function markNotificationRead(int $userId, $notificationId): void
    {
        // Process in background - you might want to implement a queue system
        try {
            // Handle notification read logic here
            Console::info("Marking notification {$notificationId} as read for user {$userId}");
        } catch (\Exception $e) {
            Console::error("Error marking notification as read: " . $e->getMessage());
        }
    }

    private function processQueuedNotifications(): void
    {
        $this->redisService->executeWithRetry(function ($client) {
            // Get batch of queued notifications
            $notifications = $client->lrange('notification_queue', 0, 99);
            if (empty($notifications)) {
                return;
            }

            foreach ($notifications as $notification) {
                $data = json_decode($notification, true);
                if (!$data) {
                    continue;
                }

                $userId = $data['user_id'] ?? null;
                $message = $data['message'] ?? '';
                $event = $data['event'] ?? 'notification';

                if ($userId) {
                    $this->sendDirectNotification($userId, $message, $event);
                }

                // Remove processed notification
                $client->lrem('notification_queue', 1, $notification);
            }
        });
    }

    public function sendDirectNotification(int $userId, string $message, string $event): bool
    {
        // Check if user is connected locally
        if (isset($this->userConnections[$userId])) {
            $conn = $this->userConnections[$userId];
            if ($conn->getState() === ConnectionInterface::STATE_OPEN) {
                $payload = json_encode([
                    'type' => 'notification',
                    'event' => $event,
                    'message' => $message,
                    'timestamp' => time()
                ]);
                
                $conn->send($payload);
                return true;
            }
        }

        // User not connected locally, queue for later or check Redis
        return $this->redisService->executeWithRetry(function ($client) use ($userId, $message, $event) {
            $fd = $client->hget('ws_connections', $userId);
            if (!$fd) {
                // User not connected anywhere, queue for later
                $client->rpush('notification_queue', json_encode([
                    'user_id' => $userId,
                    'message' => $message,
                    'event' => $event,
                    'timestamp' => time()
                ]));
                return false;
            }

            // Check if connection still exists
            if (!$client->exists("ws_connection:{$fd}")) {
                $client->hdel('ws_connections', $userId);
                return false;
            }

            return true;
        });
    }

    public function broadcastNotification(string $message, string $event): void
    {
        $payload = json_encode([
            'type' => 'broadcast',
            'event' => $event,
            'message' => $message,
            'timestamp' => time()
        ]);

        // Broadcast to all connected clients
        foreach ($this->clients as $client) {
            if ($client->getState() === ConnectionInterface::STATE_OPEN) {
                $client->send($payload);
            }
        }
    }

    private function checkConnectionHealth(): void
    {
        $now = time();
        $timeout = $this->config['heartbeat_idle_time'];

        foreach ($this->clients as $client) {
            if (isset($client->lastActivity)) {
                if ($now - $client->lastActivity > $timeout) {
                    Console::info("Closing stale connection for user {$client->userId}");
                    $client->close();
                }
            }
        }

        // Also check Redis for cleanup
        $this->redisService->executeWithRetry(function ($client) use ($now, $timeout) {
            $fds = $client->hgetall('ws_connection_map');
            if (!is_array($fds)) {
                $fds = [];
            }

            foreach ($fds as $fd => $userId) {
                try {
                    $lastActive = $client->ttl("ws_connection:{$fd}");
                    if ($lastActive === false || $lastActive <= 0) {
                        Console::info("Cleaning up stale connection for user {$userId} (fd: {$fd})");
                        $client->hdel('ws_connections', $userId);
                        $client->hdel('ws_connection_map', $fd);
                        $client->del("ws_connection:{$fd}");
                    }
                } catch (\Exception $e) {
                    Console::error("Error cleaning up connection {$fd}: " . $e->getMessage());
                }
            }
        });
    }

    private function registerServerInstance(): void
    {
        $this->redisService->executeWithRetry(function ($client) {
            $client->hset('ws_servers', gethostname() . ':' . $this->config['port'], time());
        });
    }

    private function initializeRedisStructures(): void
    {
        $this->redisService->executeWithRetry(function ($client) {
            // Ensure these keys exist as hashes
            if (!$client->exists('ws_connections')) {
                $client->hset('ws_connections', 'initialized', time());
            }
            if (!$client->exists('ws_connection_map')) {
                $client->hset('ws_connection_map', 'initialized', time());
            }
        });
    }

    private function verifyKeyTypes(): void
    {
        $this->redisService->executeWithRetry(function ($client) {
            $type = $client->type('ws_connections');
            if ($type !== 'hash') {
                $client->del('ws_connections');
                $client->hset('ws_connections', 'initialized', time());
            }

            $type = $client->type('ws_connection_map');
            if ($type !== 'hash') {
                $client->del('ws_connection_map');
                $client->hset('ws_connection_map', 'initialized', time());
            }
        });
    }

    private function cleanupStaleServers(): void
    {
        $this->redisService->executeWithRetry(function ($client) {
            $servers = $client->hgetall('ws_servers');
            $now = time();
            $timeout = 300; // 5 minutes

            foreach ($servers as $address => $lastSeen) {
                if ($now - $lastSeen > $timeout) {
                    $client->hdel('ws_servers', $address);

                    // Cleanup connections from stale server
                    $pattern = "server:{$address}:*";
                    $iterator = null;
                    do {
                        $result = $client->scan($iterator, ['match' => $pattern, 'count' => 100]);
                        $iterator = $result[0];
                        $keys = $result[1];

                        if (!empty($keys)) {
                            $client->del($keys);
                        }
                    } while ($iterator > 0);
                }
            }
        });
    }

    // Public method to send notifications from external code
    public function queueNotification(int $userId, string $message, string $event = 'notification'): void
    {
        $this->redisService->executeWithRetry(function ($client) use ($userId, $message, $event) {
            $client->rpush('notification_queue', json_encode([
                'user_id' => $userId,
                'message' => $message,
                'event' => $event,
                'timestamp' => time()
            ]));
        });
    }
}
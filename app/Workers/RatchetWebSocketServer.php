<?php
// File: /app/Workers/RatchetWebSocketServer.php
namespace App\Workers;


use Ratchet\MessageComponentInterface;
use Ratchet\ConnectionInterface;
use Ratchet\Http\HttpServer;
use Ratchet\Server\IoServer;
use Ratchet\WebSocket\WsServer;
use React\EventLoop\Loop;
use React\Socket\Server as SocketServer;
use React\Socket\ConnectionInterface as ReactConnection;
use App\Services\RedisService;
use App\Models\NotificationModel;
use App\Exceptions\Console;
#[AllowDynamicProperties]
class RatchetWebSocketServer implements MessageComponentInterface
{
       protected \SplObjectStorage $clients;
    protected array $userConnections;
    protected RedisService $redisService;
    protected NotificationModel $notificationModel;
    protected \React\EventLoop\LoopInterface $loop;
    protected array $config;
    protected array $connectionData;

    public function __construct(array $config = [])
    {
        $this->clients = new \SplObjectStorage();
        $this->userConnections = [];
        $this->connectionData = [];
        $this->redisService = new RedisService();
        $this->notificationModel = new NotificationModel();
        $this->loop = Loop::get();
        
        // $this->config = array_merge([
        //     'host' => '0.0.0.0',
        //     'port' => 9502,
        //     'enable_ssl' => false,
        // ], $config);
    }

    public function start(): void
    {
        try {
            $socket = new SocketServer(
                "0.0.0.0:9502",
                $this->loop
            );

            // Handle SSL if enabled
            // if ($this->config['enable_ssl'] ?? false) {
            //     $socket = new SecureServer($socket, $this->loop, [
            //         // 'local_cert' => $this->config['ssl_cert'],
            //         // 'local_pk' => $this->config['ssl_key'],
            //         'verify_peer' => false,
            //         'allow_self_signed' => true,
            //     ]);
            // }

            // Create WebSocket server
            $webSocket = new WsServer($this);
            $webSocket->setStrictSubProtocolCheck(false);
            
            $httpServer = new HttpServer($webSocket);
            $server = new IoServer($httpServer, $socket, $this->loop);

            $this->setupTimers();
            Console::info("Server started on 0.0.0.0:9502");
            
            $server->run();
        } catch (\Throwable $e) {
            Console::error("Server start failed: " . $e->getMessage());
            throw $e;
        }
    }

    public function onOpen(ConnectionInterface $conn): void
    {
        try {
            $this->clients->attach($conn);
            $this->connectionData[$this->getConnectionId($conn)] = [
                'userId' => null,
                'lastActivity' => time()
            ];

            $userId = $this->validateAndGetUserId($conn);
            $this->connectionData[$this->getConnectionId($conn)]['userId'] = $userId;
            $this->userConnections[$userId] = $conn;

            $this->redisService->executeWithRetry(function ($client) use ($userId, $conn) {
                $connId = $this->getConnectionId($conn);
                $client->hset('ws_connections', $userId, $connId);
                $client->hset('ws_connection_map', $connId, $userId);
                $client->setex("ws_connection:{$connId}", 300, $userId);
            });

            $this->sendInitialData($userId, $conn);
            Console::info("User {$userId} connected");
        } catch (\Throwable $e) {
            Console::error("Connection error: " . $e->getMessage());
            $conn->close();
        }
    }

    public function onMessage(ConnectionInterface $from, $msg): void
    {
        $connId = $this->getConnectionId($from);
        $this->connectionData[$connId]['lastActivity'] = time();

        try {
            $data = json_decode($msg, true) ?? [];
            $userId = $this->connectionData[$connId]['userId'] ?? null;

            if (!$userId) {
                $from->close();
                return;
            }

            $this->handleMessage($data, $from, $userId);
        } catch (\Throwable $e) {
            Console::error("Message error: " . $e->getMessage());
            $from->send(json_encode(['type' => 'error', 'message' => 'Invalid request']));
        }
    }

    public function onClose(ConnectionInterface $conn): void
    {
        $connId = $this->getConnectionId($conn);
        $userId = $this->connectionData[$connId]['userId'] ?? null;

        $this->clients->detach($conn);
        unset($this->connectionData[$connId]);

        if ($userId) {
            unset($this->userConnections[$userId]);
            $this->redisService->executeWithRetry(function ($client) use ($userId, $connId) {
                $client->hdel('ws_connections', $userId);
                $client->hdel('ws_connection_map', $connId);
                $client->del("ws_connection:{$connId}");
            });
        }

        Console::info("User {$userId} disconnected");
    }

    public function onError(ConnectionInterface $conn, \Exception $e): void
    {
        Console::error("Error: " . $e->getMessage());
        $conn->close();
    }

    private function validateAndGetUserId(ConnectionInterface $conn): int
    {
        $query = $conn->httpRequest->getUri()->getQuery();
        parse_str($query, $params);
        
        if (empty($params['userId']) || !is_numeric($params['userId'])) {
            throw new \InvalidArgumentException("Invalid user ID");
        }

        return (int) $params['userId'];
    }

    private function getConnectionId(ConnectionInterface $conn): string
    {
        return spl_object_hash($conn);
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

            case 'send_notification':
                $this->sendDirectNotification(
                    $data['user_id'],
                    $data['message'] ?? '',
                    $data['event'] ?? 'notification'
                );
                break;

            case 'mark_read':
                if (isset($data['notification_id'])) {
                    // Handle mark as read
                    $this->notificationModel->markAsRead($data['notification_id'], $userId);
                }
                break;

            default:
                $conn->send(json_encode([
                    'type' => 'error',
                    'message' => 'Unknown action'
                ]));
        }
    }

    public function sendDirectNotification(int $userId, string $message, string $event): bool
    {
        try {
            if (isset($this->userConnections[$userId])) {
                $conn = $this->userConnections[$userId];
                $payload = json_encode([
                    'type' => 'notification',
                    'event' => $event,
                    'message' => $message,
                    'timestamp' => time()
                ]);
                
                $conn->send($payload);
                return true;
            } else {
                // Queue notification if user not connected
                $this->redisService->executeWithRetry(function ($client) use ($userId, $message, $event) {
                    $client->rpush('notification_queue', json_encode([
                        'user_id' => $userId,
                        'message' => $message,
                        'event' => $event,
                        'timestamp' => time()
                    ]));
                });
                return false;
            }
        } catch (\Exception $e) {
            Console::error("Send notification error: " . $e->getMessage());
            return false;
        }
    }

    public function broadcastNotification(string $message, string $event): void
    {
        $payload = json_encode([
            'type' => 'broadcast',
            'event' => $event,
            'message' => $message,
            'timestamp' => time()
        ]);

        foreach ($this->userConnections as $userId => $conn) {
            try {
                $conn->send($payload);
            } catch (\Exception $e) {
                Console::error("Broadcast failed for user {$userId}: " . $e->getMessage());
            }
        }
    }

    private function setupTimers(): void
    {
        // Process queued notifications every 10 seconds
        $this->loop->addPeriodicTimer(10, function () {
            $this->processQueuedNotifications();
        });

        // Health check every 60 seconds
        $this->loop->addPeriodicTimer(60, function () {
            $this->checkConnectionHealth();
        });

        // Heartbeat every 30 seconds
        $this->loop->addPeriodicTimer(30, function () {
            $this->sendHeartbeat();
        });
    }

    private function processQueuedNotifications(): void
    {
        try {
            $this->redisService->executeWithRetry(function ($client) {
                $notifications = $client->lrange('notification_queue', 0, 99);
                if (empty($notifications)) return;

                foreach ($notifications as $notification) {
                    $data = json_decode($notification, true);
                    if (!$data || empty($data['message'])) {
                        $client->lrem('notification_queue', 1, $notification);
                        continue;
                    }

                    $userId = $data['user_id'] ?? null;
                    if ($userId && $this->sendDirectNotification($userId, $data['message'], $data['event'])) {
                        $client->lrem('notification_queue', 1, $notification);
                    }
                }
            });
        } catch (\Exception $e) {
            Console::error("Queue processing error: " . $e->getMessage());
        }
    }

    private function checkConnectionHealth(): void
    {
        foreach ($this->userConnections as $userId => $conn) {
            try {
                // Simple ping to check if connection is alive
                $conn->send(json_encode(['type' => 'ping']));
            } catch (\Exception $e) {
                Console::warn("Removing dead connection for user {$userId}");
                unset($this->userConnections[$userId]);
                $this->clients->detach($conn);
            }
        }
    }

    private function sendHeartbeat(): void
    {
        $payload = json_encode(['type' => 'heartbeat', 'timestamp' => time()]);
        
        foreach ($this->userConnections as $userId => $conn) {
            try {
                $conn->send($payload);
            } catch (\Exception $e) {
                Console::warn("Heartbeat failed for user {$userId}");
            }
        }
    }
}
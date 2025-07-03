<?php
// File: /workers/WebSocketServer.php
namespace App\Workers;

use Swoole\WebSocket\Server;
use Swoole\Timer;
use App\Services\RedisService;
use App\Config\DatabaseAccessors;
use App\Models\NotificationModel;
use App\Exceptions\Console;
use Predis\Client;

class WebSocketServer
{
    private Server $server;
    private RedisService $redisService;
    private NotificationModel $notificationModel;
    private array $config;
    private array $heartbeatTimers = [];
    
    public function __construct(array $config = [])
    {
        $this->config = array_merge([
            'host' => '0.0.0.0',
            'port' => 9502,
            'worker_num' => swoole_cpu_num() * 2,
            'task_worker_num' => swoole_cpu_num() * 4,
            'enable_coroutine' => true,
            'max_connection' => 10000,
            'dispatch_mode' => 2, // IP dispatch for better consistency
            'heartbeat_idle_time' => 120, // seconds
            'heartbeat_check_interval' => 60, // seconds
            // 'ssl_cert_file' => '/etc/ssl/certs/ssl-cert-snakeoil.pem',
            // 'ssl_key_file' => '/etc/ssl/private/ssl-cert-snakeoil.key',
        ], $config);

        $this->redisService = new RedisService();
        $this->notificationModel = new NotificationModel();
    }

    public function start(): void
    {
        $this->server = new Server(
            $this->config['host'],
            $this->config['port'],
            // SWOOLE_PROCESS,
            // SWOOLE_SOCK_TCP | SWOOLE_SSL
        );

        $this->server->set($this->config);
        $this->registerEventHandlers();
        $this->server->start();
    }

    private function registerEventHandlers(): void
    {
        $this->server->on('start', [$this, 'onStart']);
        $this->server->on('workerStart', [$this, 'onWorkerStart']);
        $this->server->on('open', [$this, 'onOpen']);
        $this->server->on('message', [$this, 'onMessage']);
        $this->server->on('close', [$this, 'onClose']);
        $this->server->on('task', [$this, 'onTask']);
        $this->server->on('finish', [$this, 'onFinish']);
    }

    public function onStart(Server $server): void
    {
        Console::info("WebSocket server started on wss://{$this->config['host']}:{$this->config['port']}");
        
        // Register this server instance
        $this->redisService->executeWithRetry(function($client) {
            $client->hset('ws_servers', gethostname() . ':' . $this->config['port'], time());
        });
        
        // Setup cleanup timer
        Timer::tick(60000, [$this, 'cleanupStaleServers']);
    }

    public function onWorkerStart(Server $server, int $workerId): void
    {
        if ($workerId < $this->config['worker_num']) {
            // Start health checks in worker processes
            Timer::tick($this->config['heartbeat_check_interval'] * 1000, function() {
                $this->checkConnectionHealth();
            });
            
            // Start processing queued notifications
            Timer::tick(5000, function() use ($server) {
                $server->task(['type' => 'process_queued_notifications']);
            });
        }
    }

    public function onOpen(Server $server, $request): void
    {
        try {
            $userId = $this->validateAndGetUserId($request);
            $fd = $request->fd;

            // Store connection in Redis
            $this->redisService->executeWithRetry(function($client) use ($userId, $fd) {
                $client->hset('ws_connections', $userId, $fd);
                $client->hset('ws_connection_map', $fd, $userId);
                $client->setex("ws_connection:{$fd}", $this->config['heartbeat_idle_time'], $userId);
            });

            // Start heartbeat timer for this connection
            $this->heartbeatTimers[$fd] = Timer::tick(30000, function() use ($server, $fd) {
                if ($server->exists($fd)) {
                    $server->push($fd, json_encode(['type' => 'ping']));
                } else {
                    Timer::clear($this->heartbeatTimers[$fd]);
                    unset($this->heartbeatTimers[$fd]);
                }
            });

            // Send initial data
            $this->sendInitialData($userId, $fd);

        } catch (\Exception $e) {
            Console::error("Connection error: " . $e->getMessage());
            $server->close($request->fd);
        }
    }

    public function onMessage(Server $server, $frame): void
    {
        try {
            $data = json_decode($frame->data, true);
            if (!$data) {
                throw new \InvalidArgumentException("Invalid JSON format");
            }

            $userId = $this->getUserIdFromFrame($frame->fd);
            if (!$userId) {
                $server->close($frame->fd);
                return;
            }

            // Update connection activity
            $this->redisService->executeWithRetry(function($client) use ($frame) {
                $client->expire("ws_connection:{$frame->fd}", $this->config['heartbeat_idle_time']);
            });

            $this->handleMessage($data, $frame->fd, $userId);

        } catch (\Exception $e) {
            Console::error("Message handling error: " . $e->getMessage());
            $server->push($frame->fd, json_encode([
                'type' => 'error',
                'message' => 'Internal server error'
            ]));
        }
    }

    public function onClose(Server $server, int $fd): void
    {
        try {
            // Clear heartbeat timer
            if (isset($this->heartbeatTimers[$fd])) {
                Timer::clear($this->heartbeatTimers[$fd]);
                unset($this->heartbeatTimers[$fd]);
            }

            // Remove from Redis
            $this->redisService->executeWithRetry(function($client) use ($fd) {
                $userId = $client->hget('ws_connection_map', $fd);
                if ($userId) {
                    $client->hdel('ws_connections', $userId);
                }
                $client->hdel('ws_connection_map', $fd);
                $client->del("ws_connection:{$fd}");
            });

        } catch (\Exception $e) {
            Console::error("Connection close error: " . $e->getMessage());
        }
    }

    public function onTask(Server $server, int $taskId, int $srcWorkerId, $data): void
    {
        try {
            switch ($data['type'] ?? '') {
                case 'process_queued_notifications':
                    $this->processQueuedNotifications();
                    break;
                    
                case 'send_notification':
                    $this->sendDirectNotification(
                        $data['user_id'],
                        $data['message'],
                        $data['event'] ?? 'notification'
                    );
                    break;
                    
                case 'broadcast':
                    $this->broadcastNotification($data['message'], $data['event'] ?? 'broadcast');
                    break;
            }
        } catch (\Exception $e) {
            Console::error("Task error: " . $e->getMessage());
        }
    }

    public function onFinish(Server $server, int $taskId, string $data): void
    {
        // Optional: Handle task completion if needed
    }

    private function validateAndGetUserId($request): int
    {
        parse_str($request->server['query_string'] ?? '', $query);
        if (!isset($query['userId']) || !is_numeric($query['userId'])) {
            throw new \InvalidArgumentException("Invalid user ID");
        }
        
        $userId = (int)$query['userId'];
        
        // Optional: Add additional validation (e.g., token verification)
        
        return $userId;
    }

    private function getUserIdFromFrame(int $fd): ?int
    {
        return $this->redisService->executeWithRetry(function($client) use ($fd) {
            return $client->hget('ws_connection_map', $fd);
        });
    }

    private function sendInitialData(int $userId, int $fd): void
    {
        try {
            // Send connection acknowledgement
            $this->server->push($fd, json_encode([
                'type' => 'connection',
                'status' => 'connected',
                'message' => 'WebSocket connection established'
            ]));

            // Send initial notification count
            $counts = $this->notificationModel->getNotificationCounts((string)$userId);
            $this->server->push($fd, json_encode([
                'type' => 'notification_count',
                'data' => $counts
            ]));

        } catch (\Exception $e) {
            Console::error("Initial data error: " . $e->getMessage());
        }
    }

    private function handleMessage(array $data, int $fd, int $userId): void
    {
        switch ($data['action'] ?? '') {
            case 'ping':
                $this->server->push($fd, json_encode(['type' => 'pong']));
                break;
                
            case 'get_notifications':
                $counts = $this->notificationModel->getNotificationCounts((string)$userId);
                $this->server->push($fd, json_encode([
                    'type' => 'notification_count',
                    'data' => $counts
                ]));
                break;
                
            case 'mark_read':
                if (isset($data['notification_id'])) {
                    $this->server->task([
                        'type' => 'mark_notification_read',
                        'user_id' => $userId,
                        'notification_id' => $data['notification_id']
                    ]);
                }
                break;
                
            default:
                $this->server->push($fd, json_encode([
                    'type' => 'error',
                    'message' => 'Unknown action'
                ]));
        }
    }

    private function processQueuedNotifications(): void
    {
        $this->redisService->executeWithRetry(function($client) {
            // Get batch of queued notifications
            $notifications = $client->lrange('notification_queue', 0, 99);
            if (empty($notifications)) return;

            foreach ($notifications as $notification) {
                $data = json_decode($notification, true);
                if (!$data) continue;

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

    private function sendDirectNotification(int $userId, string $message, string $event): bool
    {
        return $this->redisService->executeWithRetry(function($client) use ($userId, $message, $event) {
            $fd = $client->hget('ws_connections', $userId);
            if (!$fd) {
                // User not connected, queue for later
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

            // Send via WebSocket server API
            $payload = json_encode([
                'type' => 'notification',
                'event' => $event,
                'message' => $message,
                'timestamp' => time()
            ]);

            $this->server->push($fd, $payload);
            return true;
        });
    }

    private function broadcastNotification(string $message, string $event): void
    {
        $payload = json_encode([
            'type' => 'broadcast',
            'event' => $event,
            'message' => $message,
            'timestamp' => time()
        ]);

        $this->redisService->executeWithRetry(function($client) use ($payload) {
            $userIds = $client->hkeys('ws_connections');
            foreach ($userIds as $userId) {
                $fd = $client->hget('ws_connections', $userId);
                if ($fd && $client->exists("ws_connection:{$fd}")) {
                    $this->server->push($fd, $payload);
                }
            }
        });
    }

    private function checkConnectionHealth(): void
    {
        $this->redisService->executeWithRetry(function($client) {
            $now = time();
            $staleTime = $now - $this->config['heartbeat_idle_time'];
            
            // Find stale connections
            $fds = $client->hgetall('ws_connection_map');
            foreach ($fds as $fd => $userId) {
                $lastActive = $client->ttl("ws_connection:{$fd}");
                if ($lastActive < $staleTime) {
                    // Close stale connection
                    if ($this->server->exists($fd)) {
                        $this->server->close($fd);
                    }
                    $client->hdel('ws_connections', $userId);
                    $client->hdel('ws_connection_map', $fd);
                    $client->del("ws_connection:{$fd}");
                }
            }
        });
    }

    private function cleanupStaleServers(): void
    {
        $this->redisService->executeWithRetry(function($client) {
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
}
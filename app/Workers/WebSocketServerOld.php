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

class WebSocketServerOld
{
    private Server $server;
    private RedisService $redisService;
    private NotificationModel $notificationModel;
    private array $config;
    private array $config2;
    private array $heartbeatTimers = [];
    private array $connectionHealth = []; // Track connection health
    private array $pingTimeouts = []; // Track ping timeouts

    public function __construct(array $config = [])
    {
        $this->config2 = [
            'host' => '0.0.0.0',
            'port' => 9502
        ];
        $this->config = array_merge([
            'worker_num' => swoole_cpu_num() * 2,
            'task_worker_num' => swoole_cpu_num() * 4,
            'enable_coroutine' => true,
            'max_connection' => 1024,
            'dispatch_mode' => 2, // IP dispatch for better consistency
            'heartbeat_idle_time' => 300, // seconds
            'heartbeat_check_interval' => 60, // seconds
            // 'ssl_cert_file' => '/etc/ssl/certs/ssl-cert-snakeoil.pem',
            // 'ssl_key_file' => '/etc/ssl/private/ssl-cert-snakeoil.key',
            'ping_interval' => 30, // Separate ping interval
            'ping_timeout' => 10, // Timeout for ping responses
            'max_missed_pings' => 3, // Max missed pings before disconnect
            // Add keep-alive settings
            // 'tcp_keepalive' => 1,
            // 'tcp_keepidle' => 600,
            // 'tcp_keepinterval' => 60,
            // 'tcp_keepcount' => 3,
        ], $config);

        $this->redisService = new RedisService();
        $this->notificationModel = new NotificationModel();
        $this->initializeRedisStructures();
        $this->verifyKeyTypes();
    }

    public function start(): void
    {
        // Create server with host/port in constructor
        $host = $this->config['host'] ?? '0.0.0.0';
        $port = $this->config['port'] ?? 9502;
        // Verify Redis key types before starting
        $this->verifyKeyTypes();
        $this->server = new Server($host, $port);//, SWOOLE_PROCESS, SWOOLE_SOCK_TCP | SWOOLE_SSL);

        // Remove host/port from config before passing to set()
        $serverConfig = $this->config;
        unset(
            $serverConfig['host'],
            $serverConfig['port'],
            $serverConfig['ping_interval'],
            $serverConfig['ping_timeout'],
            $serverConfig['max_missed_pings']
        );

        $this->server->set($serverConfig);
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
        Console::info("WebSocket server started on wss://{$this->config2['host']}:{$this->config2['port']}");

        // Register this server instance
        $this->redisService->executeWithRetry(function ($client) {
            $serverKey = gethostname() . ':' . $this->config2['port'];
            $client->hset('ws_servers', $serverKey, time());
            $client->expire('ws_servers', 3600); // Expire server list after 1 hour

        });

        // Setup cleanup timer
        Timer::tick(30000, [$this, 'cleanupStaleServers']);
    }

    public function onWorkerStart(Server $server, int $workerId): void
    {
        $this->verifyKeyTypes();
        if ($workerId < $this->config['worker_num']) {
            // Start health checks in worker processes
            Timer::tick($this->config['heartbeat_check_interval'] * 1000, function () {
                $this->checkConnectionHealth();
            });

            // Process queued notifications
            Timer::tick(3000, function () use ($server) { // More frequent processing
                $server->task(['type' => 'process_queued_notifications']);
            });

            // Clean up stale connection data
            Timer::tick(120000, function () { // Every 2 minutes
                $this->cleanupStaleConnections();
            });
        }
    }

    public function onOpen(Server $server, $request): void
    {
        try {
            $userId = $this->validateAndGetUserId($request);
            $fd = $request->fd;

            // Initialize connection health tracking
            $this->connectionHealth[$fd] = [
                'user_id' => $userId,
                'last_pong' => time(),
                'missed_pings' => 0,
                'connected_at' => time()
            ];

            // Store connection in Redis with proper error handling
            $this->redisService->executeWithRetry(function ($client) use ($userId, $fd) {
                // Use string keys consistently
                $userKey = (string) $userId;
                $fdKey = (string) $fd;

                // Check if user already has a connection
                $existingFd = $client->hget('ws_connections', $userKey);
                if ($existingFd && $existingFd !== $fdKey) {
                    // Close old connection
                    $client->hdel('ws_connection_map', $existingFd);
                    $client->del("ws_connection:{$existingFd}");
                }

                $client->hset('ws_connections', $userKey, $fdKey);
                $client->hset('ws_connection_map', $fdKey, $userKey);
                $client->setex("ws_connection:{$fdKey}", $this->config['heartbeat_idle_time'], $userKey);

                // Store connection metadata
                $client->hset("ws_connection_meta:{$fdKey}", 'user_id', $userKey);
                $client->hset("ws_connection_meta:{$fdKey}", 'connected_at', time());
                $client->expire("ws_connection_meta:{$fdKey}", $this->config['heartbeat_idle_time']);
            });

            // Start improved heartbeat system
            $this->startHeartbeat($fd);

            // Send initial data
            $this->sendInitialData($userId, $fd);

            Console::info("User {$userId} connected with fd {$fd}");

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
            $this->updateConnectionActivity($frame->fd);

            // Handle pong responses
            if (isset($data['type']) && $data['type'] === 'pong') {
                $this->handlePong($frame->fd);
                return;
            }

            // Validate message content
            if (!isset($data['action']) || (isset($data['message']) && trim($data['message']) === '')) {
                $server->push($frame->fd, json_encode([
                    'type' => 'error',
                    'message' => 'Invalid message format or empty content'
                ]));
                return;
            }

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
            $userId = $this->connectionHealth[$fd]['user_id'] ?? null;

            // Clear timers
            $this->clearHeartbeat($fd);

            // Clean up connection tracking
            unset($this->connectionHealth[$fd]);
            unset($this->pingTimeouts[$fd]);

            // Remove from Redis
            $this->redisService->executeWithRetry(function ($client) use ($fd, $userId) {
                $fdKey = (string) $fd;

                if ($userId) {
                    $userKey = (string) $userId;
                    $client->hdel('ws_connections', $userKey);
                }

                $client->hdel('ws_connection_map', $fdKey);
                $client->del("ws_connection:{$fdKey}");
                $client->del("ws_connection_meta:{$fdKey}");
            });

            Console::info("User {$userId} disconnected (fd: {$fd})");

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
                    $message = trim($data['message'] ?? '');
                    if ($message !== '') {
                        $this->sendDirectNotification(
                            $data['user_id'],
                            $message,
                            $data['event'] ?? 'notification'
                        );
                    }
                    break;

                case 'broadcast':
                    $message = trim($data['message'] ?? '');
                    if ($message !== '') {
                        $this->broadcastNotification($message, $data['event'] ?? 'broadcast');
                    }
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

        $userId = (int) $query['userId'];

        // Optional: Add additional validation (e.g., token verification)

        return $userId;
    }

    private function getUserIdFromFrame(int $fd): ?int
    {
        return $this->redisService->executeWithRetry2(function ($client) use ($fd) {
            $fdKey = (string) $fd;
            $userId = $client->hget('ws_connection_map', $fdKey);
            return ($userId !== null && is_numeric($userId)) ? (int) $userId : null;
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
            $counts = $this->notificationModel->getNotificationCounts((string) $userId);
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
        Console::log("Received message from user {$userId}: " . json_encode($data));
        switch ($data['action'] ?? '') {
            case 'ping':
                $this->server->push($fd, json_encode(['type' => 'pong']));
                break;

            case 'get_notifications':
                $counts = $this->notificationModel->getNotificationCounts((string) $userId);
                $this->server->push($fd, json_encode([
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


    private function startHeartbeat(int $fd): void
    {
        // Clear any existing heartbeat
        $this->clearHeartbeat($fd);

        // Start ping timer
        $this->heartbeatTimers[$fd] = Timer::tick($this->config['ping_interval'] * 1000, function () use ($fd) {
            if (!$this->server->exists($fd)) {
                $this->clearHeartbeat($fd);
                return;
            }

            // Check if we've missed too many pings
            if (($this->connectionHealth[$fd]['missed_pings'] ?? 0) >= $this->config['max_missed_pings']) {
                Console::warn("Closing connection {$fd} due to missed pings");
                $this->server->close($fd);
                return;
            }

            // Send ping
            $success = $this->server->push($fd, json_encode([
                'type' => 'ping',
                'timestamp' => time()
            ]));

            if ($success) {
                // Set timeout for pong response
                $this->pingTimeouts[$fd] = Timer::after($this->config['ping_timeout'] * 1000, function () use ($fd) {
                    if (isset($this->connectionHealth[$fd])) {
                        $this->connectionHealth[$fd]['missed_pings']++;
                        Console::warn("Ping timeout for connection {$fd}, missed pings: " . $this->connectionHealth[$fd]['missed_pings']);
                    }
                    unset($this->pingTimeouts[$fd]);
                });
            } else {
                Console::warn("Failed to send ping to connection {$fd}");
                $this->clearHeartbeat($fd);
            }
        });
    }

    private function handlePong(int $fd): void
    {
        if (isset($this->connectionHealth[$fd])) {
            $this->connectionHealth[$fd]['last_pong'] = time();
            $this->connectionHealth[$fd]['missed_pings'] = 0;
        }

        // Clear ping timeout
        if (isset($this->pingTimeouts[$fd])) {
            Timer::clear($this->pingTimeouts[$fd]);
            unset($this->pingTimeouts[$fd]);
        }
    }

    private function clearHeartbeat(int $fd): void
    {
        if (isset($this->heartbeatTimers[$fd])) {
            Timer::clear($this->heartbeatTimers[$fd]);
            unset($this->heartbeatTimers[$fd]);
        }

        if (isset($this->pingTimeouts[$fd])) {
            Timer::clear($this->pingTimeouts[$fd]);
            unset($this->pingTimeouts[$fd]);
        }
    }

    private function updateConnectionActivity(int $fd): void
    {
        $this->redisService->executeWithRetry(function ($client) use ($fd) {
            $fdKey = (string) $fd;
            $client->expire("ws_connection:{$fdKey}", $this->config['heartbeat_idle_time']);
            $client->expire("ws_connection_meta:{$fdKey}", $this->config['heartbeat_idle_time']);
        });
    }

    private function processQueuedNotifications(): void
    {
        $this->redisService->executeWithRetry(function ($client) {
            // Get batch of queued notifications
            $notifications = $client->lrange('notification_queue', 0, 99);
            if (empty($notifications) || !is_array($notifications)) {
                return;
            }
            Console::log("Processing " . count($notifications) . " queued notifications: " . json_encode($notifications));
            foreach ($notifications as $notification) {
                $data = json_decode($notification, true);
                if (!$data || empty(trim($data['message'] ?? ''))) {
                    $client->lrem('notification_queue', 1, $notification); // remove empty
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


    private function sendDirectNotification(int $userId, string $message, string $event): bool
    {
        if (empty(trim($message))) {
            return false;
        }

        return $this->redisService->executeWithRetry2(function ($client) use ($userId, $message, $event) {
            $userKey = (string) $userId;
            $fd = $client->hget('ws_connections', $userKey);

            // Validate fd
            if ($fd === null || !is_numeric($fd)) {
                // Queue for later delivery
                $this->queueNotification($userId, $message, $event);
                return false;
            }

            $fdKey = (string) $fd;

            // Check if connection is still valid
            if (!$client->exists("ws_connection:{$fdKey}")) {
                // Clean up stale connection
                $client->hdel('ws_connections', $userKey);
                $client->hdel('ws_connection_map', $fdKey);
                $client->del("ws_connection_meta:{$fdKey}");

                // Queue for later delivery
                $this->queueNotification($userId, $message, $event);
                return false;
            }

            // Send notification
            $payload = json_encode([
                'type' => 'notification',
                'event' => $event,
                'message' => $message,
                'timestamp' => time()
            ]);

            $success = $this->server->push((int) $fd, $payload);

            if (!$success) {
                Console::warn("Failed to send notification to fd {$fd}");
                // Queue for retry
                $this->queueNotification($userId, $message, $event);
                return false;
            }

            return true;
        });
    }

    private function queueNotification(int $userId, string $message, string $event): void
    {
        $this->redisService->executeWithRetry(function ($client) use ($userId, $message, $event) {
            $notification = json_encode([
                'user_id' => $userId,
                'message' => $message,
                'event' => $event,
                'timestamp' => time(),
                'retry_count' => 0
            ]);

            $client->rpush('notification_queue', $notification);
            // Set TTL for the queue to prevent infinite growth
            $client->expire('notification_queue', 3600);
        });
    }

    private function cleanupStaleConnections(): void
    {
        $this->redisService->executeWithRetry(function ($client) {
            // Get all connection mappings
            $connections = $this->redisService->safeHGetAll('ws_connection_map');

            foreach ($connections as $fd => $userId) {
                $fdKey = (string) $fd;

                // Check if connection key exists
                if (!$client->exists("ws_connection:{$fdKey}")) {
                    Console::info("Cleaning up stale connection: fd={$fd}, user={$userId}");

                    // Clean up all related keys
                    $client->hdel('ws_connections', (string) $userId);
                    $client->hdel('ws_connection_map', $fdKey);
                    $client->del("ws_connection_meta:{$fdKey}");

                    // Close connection if still exists in Swoole
                    if ($this->server->exists((int) $fd)) {
                        $this->server->close((int) $fd);
                    }
                }
            }
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

        $this->redisService->executeWithRetry(function ($client) use ($payload) {
            $userIds = $client->hkeys('ws_connections');
            if (!is_array($userIds)) {
                $userIds = [];
            }
            foreach ($userIds as $userId) {
                $fd = $client->hget('ws_connections', $userId);
                if ($fd && $client->exists("ws_connection:{$fd}")) {
                    $this->server->push((int) $fd, $payload); // Cast $fd to int for Swoole push
                }
            }
        });
    }

    private function checkConnectionHealth(): void
    {
        $this->redisService->executeWithRetry(function ($client) {
            $now = time();
            $staleTime = $now - $this->config['heartbeat_idle_time'];

            // Safely get all connection mappings
            // Check type before hgetall
            $type = $client->type('ws_connection_map');
            // Console::log2('checkConnectionHealth - type of ws_connection_map: ', $type);
            if ($type !== 'hash') {
                $ttype = json_encode($type);
                // Console::warn("Redis key 'ws_connection_map' is not a hash (type: {$ttype}). Re-initializing.");
                $client->del('ws_connection_map');
                $client->hset('ws_connection_map', 'init', time());
                $client->hdel('ws_connection_map', 'init');
                $fds = [];
            } else {
                $fds = $client->hgetall('ws_connection_map');
                if (!is_array($fds)) {
                    $fds = [];
                }
            }

            foreach ($fds as $fd => $userId) {
                try {
                    if (!$client->exists("ws_connection:{$fd}")) {
                        Console::info("Cleaning up stale connection for user {$userId} (fd: {$fd}). Redis key ws_connection:{$fd} does not exist.");
                        if ($this->server->exists((int) $fd)) {
                            $this->server->close((int) $fd);
                        }
                        $client->hdel('ws_connections', $userId);
                        $client->hdel('ws_connection_map', $fd);
                    }
                } catch (\Exception $e) {
                    Console::error("Error cleaning up connection {$fd}: " . $e->getMessage());
                }
            }
        });
    }


    private function initializeRedisStructures(): void
    {
        $this->redisService->executeWithRetry(function ($client) {
            // Ensure these keys exist as hashes
            if (!$client->exists('ws_connections')) {
                $client->hset('ws_connections', 'init', time()); // Add dummy entry
                $client->hdel('ws_connections', 'init'); // Remove dummy entry
            }
            if (!$client->exists('ws_connection_map')) {
                $client->hset('ws_connection_map', 'init', time()); // Add dummy entry
                $client->hdel('ws_connection_map', 'init'); // Remove dummy entry
            }
        });
    }

    private function verifyKeyTypes(): void
    {
        $this->redisService->executeWithRetry(function ($client) {
            $type = $client->type('ws_connections');
            if ($type !== 'hash') {
                $str_type = json_encode($type);
                Console::warn("Redis key 'ws_connections' has type '{$str_type}', expected 'hash'. Deleting and re-initializing.");
                $client->del('ws_connections');
                $client->hset('ws_connections', 'init', time());
                $client->hdel('ws_connections', 'init');
            }

            $type = $client->type('ws_connection_map');
            if ($type !== 'hash') {
                $str_type = json_encode($type);
                Console::warn("Redis key 'ws_connection_map' has type '{$str_type}', expected 'hash'. Deleting and re-initializing.");
                $client->del('ws_connection_map');
                $client->hset('ws_connection_map', 'init', time());
                $client->hdel('ws_connection_map', 'init');
            }
        });
    }

    private function cleanupStaleServers(): void
    {
        $this->redisService->executeWithRetry(function ($client) {
            $servers = $this->redisService->safeHGetAll('ws_servers');
            if (!is_array($servers)) {
                $servers = [];
            }
            $now = time();
            $timeout = 300; // 5 minutes

            foreach ($servers as $address => $lastSeen) {
                if ($now - $lastSeen > $timeout) {
                    Console::info("Cleaning up stale server: {$address}");
                    $client->hdel('ws_servers', $address);

                    // Cleanup connections from stale server (if any specific pattern was used)
                    // The original code tried to use `server:{$address}:*` for scanning,
                    // but connections are stored in `ws_connections` and `ws_connection_map`
                    // globally, not per server. This part might be unnecessary or needs rethinking
                    // if connections are not directly linked to a specific server in Redis.
                    // For now, removing the `scan` and `del` logic related to `server:{$address}:*`
                    // as it seems misplaced given the current connection storage mechanism.
                    // If you intend to store connections per server, that Redis key structure
                    // and associated logic need to be implemented elsewhere.
                }
            }
        });
    }
}
<?php
// File: /workers/WebSocketServer.php
namespace App\Workers;

use Swoole\WebSocket\Server;
use Swoole\Timer;
use App\Services\RedisService2;
use App\Config\DatabaseAccessors;
use App\Models\NotificationModel;
use App\Exceptions\Console;
use Predis\Client;

class WebSocketServer
{
    private Server $server;
    private RedisService2 $redisService;
    private NotificationModel $notificationModel;
    private array $config;
    private array $config2;
    private array $heartbeatTimers = [];

    public function __construct(array $config = [])
    {
        $this->serverId = gethostname() . ':' . ($config['port'] ?? 9502);
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
            'heartbeat_idle_time' => 120, // seconds
            'heartbeat_check_interval' => 60, // seconds
            // 'ssl_cert_file' => '/etc/ssl/certs/ssl-cert-snakeoil.pem',
            // 'ssl_key_file' => '/etc/ssl/private/ssl-cert-snakeoil.key',
            'ssl_cert_file' => '/etc/letsencrypt/live/winsstarts.com/fullchain.pem',
            'ssl_key_file' => '/etc/letsencrypt/live/winsstarts.com/privkey.pem',
            'ssl_protocols' => SWOOLE_SSL_TLSv1_2 | SWOOLE_SSL_TLSv1_3, // Enforce modern protocols
            'ssl_ciphers' => 'ECDHE-ECDSA-AES128-GCM-SHA256:ECDHE-RSA-AES128-GCM-SHA256:ECDHE-ECDSA-AES256-GCM-SHA384:ECDHE-RSA-AES256-GCM-SHA384', // Strong ciphers
            'ssl_prefer_server_ciphers' => true,
            'open_http2_protocol' => true,
            'buffer_output_size' => 32 * 1024 * 1024, // 32MB
            'socket_buffer_size' => 128 * 1024 * 1024, // 128MB
            'reload_async' => true,
            'max_wait_time' => 60,
        ], $config);

        $this->redisService = new RedisService2();
        $this->notificationModel = new NotificationModel();
        $this->initializeRedisStructures();
        // $this->verifyKeyTypes();
    }

    public function start(): void
    {
        // Create server with host/port in constructor
        $host = $this->config['host'] ?? '0.0.0.0';
        $port = $this->config['port'] ?? 9502;
        // Verify Redis key types before starting
        // $this->verifyKeyTypes();
        $this->server = new Server($host, $port, SWOOLE_PROCESS, SWOOLE_SOCK_TCP | SWOOLE_SSL);

        // Remove host/port from config before passing to set()
        $serverConfig = $this->config;
        unset($serverConfig['host'], $serverConfig['port']);

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

        // Register this server instance with extended TTL
        $this->redisService->registerServer($this->serverId);

        // Setup cleanup timers
        Timer::tick(60000, [$this, 'cleanupStaleServers']);
        Timer::tick(30000, function () {
            $this->redisService->registerServer($this->serverId);
        });
    }

    public function onWorkerStart(Server $server, int $workerId): void
    {
        if ($workerId < $this->config['worker_num']) {
            // Health checks with less frequency
            Timer::tick($this->config['heartbeat_check_interval'] * 1000, function () {
                $this->checkConnectionHealth();
            });

            // Process queued notifications
            Timer::tick(10000, function () use ($server) { // 10 seconds instead of 5
                $server->task(['type' => 'process_queued_notifications']);
            });
        }
    }

    public function onOpen(Server $server, $request): void
    {
        try {
            $userId = $this->validateAndGetUserId($request);
            $fd = $request->fd;

            // Track connection with extended TTL (2x heartbeat idle time)
            $this->redisService->trackConnection(
                $userId,
                $fd,
                $this->config['heartbeat_idle_time'] * 2
            );

            // Less aggressive heartbeat (every 45 seconds)
            $this->heartbeatTimers[$fd] = Timer::tick(45000, function () use ($server, $fd) {
                if ($server->exists($fd)) {
                    $server->push($fd, json_encode(['type' => 'ping']));
                } else {
                    Timer::clear($this->heartbeatTimers[$fd]);
                    unset($this->heartbeatTimers[$fd]);
                }
            });

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

            // Get user ID and validate connection
            $userId = $this->redisService->getConnectionUserId($frame->fd);
            if (!$userId || !$this->server->exists($frame->fd)) {
                // Console::warn("Message from invalid connection (fd: {$frame->fd})");
                $server->close($frame->fd);
                return;
            }

            // Rest of your message handling logic...
            $this->handleMessage($data, $frame->fd, $userId);

        } catch (\Exception $e) {
            Console::error("Message handling error: " . $e->getMessage());
            if (isset($frame->fd) && $server->exists($frame->fd)) {
                $server->push($frame->fd, json_encode([
                    'type' => 'error',
                    'message' => 'Internal server error'
                ]));
            }
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

            // Clean up Redis entries
            $this->redisService->executeWithRetry(function ($client) use ($fd) {
                $pipe = $client->pipeline();
                $userId = $client->get(RedisService2::CONNECTION_PREFIX . $fd);

                if ($userId) {
                    $pipe->hdel(RedisService2::USER_CONNECTION_MAP, $userId);
                }
                $pipe->hdel(RedisService2::FD_USER_MAP, $fd);
                $pipe->del(RedisService2::CONNECTION_PREFIX . $fd);
                $pipe->execute();
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

    public function onShutdown(): void
    {
        // Clean up all server registrations
        $this->redisService->executeWithRetry(function ($client) {
            $client->hdel(RedisService2::SERVER_REGISTRY, $this->serverId);
        });

        // Clear all timers
        foreach ($this->heartbeatTimers as $timerId) {
            Timer::clear($timerId);
        }
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
        // Console::log("Received message from user {$userId}: " . json_encode($data));
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

    private function processQueuedNotifications(): void
    {
        $this->redisService->executeWithRetry(function ($client) {
            // Get batch of queued notifications
            $notifications = $client->lrange('notification_queue', 0, 99);
            if (empty($notifications) || !is_array($notifications)) {
                return;
            }
            // Console::log("Processing " . count($notifications) . " queued notifications: " . json_encode($notifications));
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

    public function sendDirectNotification(int $userId, string $message, string $event): bool
    {
        return $this->redisService->executeWithRetry2(function ($client) use ($userId, $message, $event) {
            // Get the file descriptor - ensure it's a single value
            $fd = $client->hget(RedisService2::USER_CONNECTION_MAP, (string) $userId);

            // Validate the connection thoroughly
            if (!$this->isValidConnection($fd)) {
                if (!empty($message)) {
                    // Queue notification if user disconnected
                    $client->rpush(RedisService2::QUEUE_PREFIX . 'notifications', json_encode([
                        'user_id' => $userId,
                        'message' => $message,
                        'event' => $event,
                        'timestamp' => time()
                    ]));
                }
                return false;
            }

            // At this point we're guaranteed $fd is a valid integer
            $fd = (int) $fd;

            try {
                $payload = json_encode([
                    'type' => 'notification',
                    'event' => $event,
                    'message' => $message,
                    'timestamp' => time()
                ]);

                return $this->server->push($fd, $payload);
            } catch (\Exception $e) {
                Console::error("Push failed for fd {$fd}: " . $e->getMessage());
                $this->cleanupStaleConnection($userId, $fd);
                return false;
            }
        });
    }

    private function isValidConnection($fd): bool
    {
        // Comprehensive type and value validation
        if (
            empty($fd) ||
            $fd === 0 ||
            $fd === '0' ||
            is_array($fd) ||
            is_object($fd)
        ) {
            return false;
        }

        // Convert to integer if it's a numeric string
        $fd = is_numeric($fd) ? (int) $fd : $fd;

        try {
            return $this->redisService->executeWithRetry2(function ($client) use ($fd) {
                // Verify the connection exists in both Redis and Swoole

                // 1. Check Redis connection tracking
                $redisKey = RedisService2::CONNECTION_PREFIX . $fd;
                $redisExists = (bool) $client->exists($redisKey);

                // 2. Check Swoole connection status
                $swooleExists = false;
                if (is_int($fd)) {
                    $swooleExists = $this->server->exists($fd);
                }

                // Connection is only valid if both systems agree
                return $redisExists && $swooleExists;
            });
        } catch (\Exception $e) {
            Console::error("Connection validation failed for fd {$fd}: " . $e->getMessage());
            return false;
        }
    }

    private function cleanupStaleConnection($userId, $fd): void
    {
        // Ensure we have valid inputs
        if (empty($userId) || empty($fd)) {
            return;
        }

        $this->redisService->executeWithRetry(function ($client) use ($userId, $fd) {
            $pipe = $client->pipeline();

            // Convert to strings to ensure consistent key types
            $pipe->hdel(RedisService2::USER_CONNECTION_MAP, (string) $userId);
            $pipe->hdel(RedisService2::FD_USER_MAP, (string) $fd);
            $pipe->del(RedisService2::CONNECTION_PREFIX . $fd);

            $pipe->execute();
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
            // Use safeHGetAll as recommended - this handles type checking and empty cases
            $fds = $this->redisService->safeHGetAll(RedisService2::FD_USER_MAP);

            foreach ($fds as $fd => $userId) {
                try {
                    // Skip invalid file descriptors immediately
                    if (empty($fd) || $fd === 0 || $fd === '0') {
                        $this->cleanupStaleConnection($userId, $fd);
                        continue;
                    }

                    $connectionKey = RedisService2::CONNECTION_PREFIX . $fd;

                    // Double verification:
                    // 1. Check Redis connection tracking
                    // 2. Verify Swoole knows about the connection
                    $redisConnectionExists = $this->redisService->executeWithRetry2(
                        fn($c) => (bool) $c->exists($connectionKey)
                    );

                    $swooleConnectionExists = $this->server->exists((int) $fd);

                    if (!$redisConnectionExists || !$swooleConnectionExists) {
                        Console::info("Cleaning up stale connection for user {$userId} (fd: {$fd})");
                        $this->cleanupStaleConnection($userId, $fd);

                        // If Redis thinks it exists but Swoole doesn't, log the discrepancy
                        if ($redisConnectionExists && !$swooleConnectionExists) {
                            Console::warn("Connection mismatch - Redis had fd {$fd} but Swoole didn't");
                        }
                    }
                } catch (\Exception $e) {
                    Console::error("Health check failed for fd {$fd}: " . $e->getMessage());
                    // If we can't verify, play it safe and clean up
                    $this->cleanupStaleConnection($userId, $fd);
                }
            }
        });
    }


    private function initializeRedisStructures(): void
    {
        $this->redisService->executeWithRetry(function ($client) {
            // Initialize USER_CONNECTION_MAP
            if ($client->type(RedisService2::USER_CONNECTION_MAP) !== 'hash') {
                $client->del(RedisService2::USER_CONNECTION_MAP);
                $client->hset(RedisService2::USER_CONNECTION_MAP, '__initialized__', time());
                $client->hdel(RedisService2::USER_CONNECTION_MAP, '__initialized__');
            }

            // Initialize FD_USER_MAP
            if ($client->type(RedisService2::FD_USER_MAP) !== 'hash') {
                $client->del(RedisService2::FD_USER_MAP);
                $client->hset(RedisService2::FD_USER_MAP, '__initialized__', time());
                $client->hdel(RedisService2::FD_USER_MAP, '__initialized__');
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
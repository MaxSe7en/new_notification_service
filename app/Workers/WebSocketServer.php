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

        // Fixed configuration with better timeouts
        $this->config = array_merge([
            'worker_num' => swoole_cpu_num(),
            'task_worker_num' => swoole_cpu_num() * 2,
            'enable_coroutine' => true,
            'max_connection' => 1024,
            'dispatch_mode' => 2,

            // FIXED: Better heartbeat configuration
            'heartbeat_idle_time' => 180, // 3 minutes instead of 2
            'heartbeat_check_interval' => 60, // Keep at 60 seconds

            // SSL Configuration - Make optional
            'ssl_cert_file' => $this->getSSLCertPath(),
            'ssl_key_file' => $this->getSSLKeyPath(),
            // 'ssl_protocols' => SWOOLE_SSL_TLSv1_2 | SWOOLE_SSL_TLSv1_3,
            'ssl_ciphers' => 'ECDHE-ECDSA-AES128-GCM-SHA256:ECDHE-RSA-AES128-GCM-SHA256',
            'ssl_prefer_server_ciphers' => true,
            'open_http2_protocol' => false, // Disable HTTP/2 for WebSocket

            // Buffer settings
            'buffer_output_size' => 8 * 1024 * 1024, // Reduced to 8MB
            'socket_buffer_size' => 32 * 1024 * 1024, // Reduced to 32MB
            'package_max_length' => 8 * 1024 * 1024,

            // Connection settings
            'reload_async' => true,
            'max_wait_time' => 60,
            'tcp_fastopen' => true,
            'open_tcp_nodelay' => true,
            'open_cpu_affinity' => true,

            // FIXED: Add these important settings
            'max_request' => 0, // Don't restart workers
            'enable_reuse_port' => true,
            'backlog' => 128,
        ], $config);

        $this->redisService = new RedisService2();
        $this->notificationModel = new NotificationModel();
        $this->initializeRedisStructures();
    }

    private function getSSLCertPath(): ?string
    {
        $certPath = '/etc/letsencrypt/live/winsstarts.com/fullchain.pem';
        return (file_exists($certPath) && is_readable($certPath)) ? $certPath : null;
    }

    private function getSSLKeyPath(): ?string
    {
        $keyPath = '/etc/letsencrypt/live/winsstarts.com/privkey.pem';
        return (file_exists($keyPath) && is_readable($keyPath)) ? $keyPath : null;
    }

    public function start(): void
    {
        $host = $this->config['host'] ?? '0.0.0.0';
        $port = $this->config['port'] ?? 9502;

        // Check if SSL is available
        $useSSL = $this->config['ssl_cert_file'] && $this->config['ssl_key_file'];

        if ($useSSL) {
            Console::info("Starting WebSocket server with SSL");
            $this->server = new Server($host, $port, SWOOLE_PROCESS, SWOOLE_SOCK_TCP | SWOOLE_SSL);
        } else {
            Console::warn("Starting WebSocket server without SSL (certificates not found)");
            $this->server = new Server($host, $port, SWOOLE_PROCESS, SWOOLE_SOCK_TCP);
            // Remove SSL configs if not using SSL
            unset(
                $this->config['ssl_cert_file'],
                $this->config['ssl_key_file'],
                $this->config['ssl_protocols'],
                $this->config['ssl_ciphers'],
                $this->config['ssl_prefer_server_ciphers']
            );
        }

        // Remove host/port from config
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
        // $protocol = $this->config['ssl_cert_file'] ? 'wss' : 'ws';
        Console::info("WebSocket server started on ://{$this->config2['host']}:{$this->config2['port']}");

        $this->redisService->registerServer($this->serverId);

        // Cleanup and registration timers
        Timer::tick(60000, [$this, 'cleanupStaleServers']);
        Timer::tick(30000, function () {
            $this->redisService->registerServer($this->serverId);
        });
    }

    public function onWorkerStart(Server $server, int $workerId): void
    {
        if ($workerId < $this->config['worker_num']) {
            // FIXED: Less aggressive connection health checks
            Timer::tick(90000, function () { // Every 90 seconds instead of 60
                $this->checkConnectionHealth();
            });

            // Process queued notifications
            Timer::tick(15000, function () use ($server) { // Every 15 seconds
                $server->task(['type' => 'process_queued_notifications']);
            });
        }
    }

    public function onOpen(Server $server, $request): void
    {
        try {
            $userId = $this->validateAndGetUserId($request);
            $fd = $request->fd;

            Console::info("New connection: User {$userId} (fd: {$fd})");

            // FIXED: Longer TTL for connection tracking
            $this->redisService->trackConnection($userId, $fd, 300); // 5 minutes

            // FIXED: Synchronized heartbeat - every 60 seconds to match client
            $this->heartbeatTimers[$fd] = Timer::tick(60000, function () use ($server, $fd) {
                if ($server->exists($fd)) {
                    $server->push($fd, json_encode([
                        'type' => 'ping',
                        'timestamp' => time()
                    ]));
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

            // FIXED: More lenient connection validation
            $userId = $this->redisService->getConnectionUserId($frame->fd);
            if (!$userId) {
                Console::warn("Message from unknown connection (fd: {$frame->fd})");
                return; // Don't close immediately
            }

            // Update connection timestamp
            $this->redisService->trackConnection($userId, $frame->fd, 300);

            $this->handleMessage($data, $frame->fd, $userId);

        } catch (\Exception $e) {
            Console::error("Message handling error: " . $e->getMessage());
            // Don't close connection on message errors
        }
    }

    public function onClose(Server $server, int $fd): void
    {
        try {
            Console::info("Connection closed: fd {$fd}");

            // Clear heartbeat timer
            if (isset($this->heartbeatTimers[$fd])) {
                Timer::clear($this->heartbeatTimers[$fd]);
                unset($this->heartbeatTimers[$fd]);
            }

            // Clean up Redis entries
            $this->redisService->executeWithRetry(function ($client) use ($fd) {
                $userId = $client->get(RedisService2::CONNECTION_PREFIX . $fd);
                if ($userId) {
                    $client->hdel(RedisService2::USER_CONNECTION_MAP, $userId);
                }
                $client->hdel(RedisService2::FD_USER_MAP, $fd);
                $client->del(RedisService2::CONNECTION_PREFIX . $fd);
            });

        } catch (\Exception $e) {
            Console::error("Connection close error: " . $e->getMessage());
        }
    }

    public function onTask(Server $server, int $taskId, int $srcWorkerId, $data): void
    {
        // try {
        //     switch ($data['type'] ?? '') {
        //         case 'process_queued_notifications':
        //             $this->processQueuedNotifications();
        //             break;

        //         case 'send_notification':
        //             $message = trim($data['message'] ?? '');
        //             if ($message !== '') {
        //                 $this->sendDirectNotification(
        //                     $data['user_id'],
        //                     $message,
        //                     $data['event'] ?? 'notification'
        //                 );
        //             }
        //             break;

        //         case 'broadcast':
        //             $message = trim($data['message'] ?? '');
        //             if ($message !== '') {
        //                 $this->broadcastNotification($message, $data['event'] ?? 'broadcast');
        //             }
        //             break;
        //     }
        // } catch (\Exception $e) {
        //     Console::error("Task error: " . $e->getMessage());
        // }
    }

    public function onFinish(Server $server, int $taskId, string $data): void
    {
        // Optional: Handle task completion if needed
    }

    // FIXED: Improved connection validation
    private function isValidConnection($fd): bool
    {
        if (empty($fd) || !is_numeric($fd)) {
            return false;
        }

        $fd = (int) $fd;

        try {
            // Check Swoole first (faster)
            if (!$this->server->exists($fd)) {
                return false;
            }

            // Then check Redis
            return $this->redisService->executeWithRetry2(function ($client) use ($fd) {
                return (bool) $client->exists(RedisService2::CONNECTION_PREFIX . $fd);
            });

        } catch (\Exception $e) {
            Console::error("Connection validation failed for fd {$fd}: " . $e->getMessage());
            return false;
        }
    }

    // FIXED: More gentle connection health checking
    private function checkConnectionHealth(): void
    {
        try {
            $this->redisService->executeWithRetry(function ($client) {
                $connections = $this->redisService->safeHGetAll(RedisService2::FD_USER_MAP);

                foreach ($connections as $fd => $userId) {
                    if (empty($fd) || !is_numeric($fd)) {
                        $this->cleanupStaleConnection($userId, $fd);
                        continue;
                    }

                    $fd = (int) $fd;

                    // Only check Swoole existence (Redis might be slower)
                    if (!$this->server->exists($fd)) {
                        Console::info("Cleaning up disconnected fd {$fd} for user {$userId}");
                        $this->cleanupStaleConnection($userId, $fd);
                    }
                }
            });
        } catch (\Exception $e) {
            Console::error("Health check failed: " . $e->getMessage());
        }
    }

    // Rest of your methods remain the same...
    private function handleMessage(array $data, int $fd, int $userId): void
    {
        Console::log("Received message from user {$userId}: " . json_encode($data));
        switch ($data['action'] ?? '') {
            case 'ping':
                $this->server->push($fd, json_encode([
                    'type' => 'pong',
                    'timestamp' => time()
                ]));
                break;

            case 'pong':
                // Client responded to our ping
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
                Console::warn("Unknown action: " . ($data['action'] ?? 'none'));
        }
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

    private function isValidConnection2($fd): bool
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

    private function isValidConnectionss($fd): bool
    {
        // Simplified validation
        if (!is_numeric($fd) || $fd <= 0) {
            return false;
        }

        $fd = (int) $fd;

        try {
            return $this->server->exists($fd);
        } catch (\Exception $e) {
            Console::error("Connection validation failed: " . $e->getMessage());
            return false;
        }
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

    private function checkConnectionHealths(): void
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

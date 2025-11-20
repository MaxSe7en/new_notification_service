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
    private array $userTimers = []; // Added: To store user-specific notification check timers

    // Added: Interval for checking user-specific notifications (e.g., every 5 seconds)
    private const NOTIFICATION_CHECK_INTERVAL = 5000; // milliseconds
    private string $serverId;
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
            Timer::tick($this->config['heartbeat_check_interval'] * 1000, function () {
                $this->checkConnectionHealth();
            });

            // Process queued notifications from Redis (general queue)
            Timer::tick(10000, function () use ($server) {
                $server->task(['type' => 'process_queued_notifications']);
            });

            // Process pending notifications from Database (new)
            Timer::tick(15000, function () use ($server) { // e.g., every 15 seconds
                $server->task(['type' => 'process_pending_db_notifications']);
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
            $this->redisService->trackConnection($userId, $fd, $this->config['heartbeat_idle_time'] * 2); // 5 minutes

            // FIXED: Synchronized heartbeat - every 60 seconds to match client
            $this->heartbeatTimers[$fd] = Timer::tick(45000, function () use ($server, $fd) {
                if ($server->exists($fd)) {
                    $server->push($fd, json_encode([
                        'type' => 'ping',
                        'timestamp' => time(),
                        'connection_id' => $fd
                    ]));
                } else {
                    Timer::clear($this->heartbeatTimers[$fd]);
                    unset($this->heartbeatTimers[$fd]);
                }
            });

            $this->sendInitialData($userId, $fd);
            // Start user-specific notification timer
            $this->startUserNotificationTimer($userId, $fd);
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

            // Get user ID from Redis
            $userId = 3;//$this->redisService->getConnectionUserIdByFd($server, $frame->fd);
            if (!$userId) {
                Console::warn("Message from unknown connection (fd: {$frame->fd})");
                return; // Don't close immediately
            }

            // Update connection timestamp
            $this->redisService->trackConnection($userId, $frame->fd, $this->config['heartbeat_idle_time'] * 2);

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

            // Retrieve userId before cleaning up Redis entries to clear user-specific timer
            $userId = $this->redisService->executeWithRetry2(function ($client) use ($fd) {
                return $client->get(RedisService2::CONNECTION_PREFIX . $fd);
            });

            // Clear user-specific notification timer
            if ($userId && isset($this->userTimers[$userId])) {
                Timer::clear($this->userTimers[$userId]);
                unset($this->userTimers[$userId]);
            }

            // Clean up Redis entries
            $this->redisService->executeWithRetry2(function ($client) use ($fd, $userId) {
                $pipe = $client->pipeline();
                if ($userId) {
                    $pipe->hdel(RedisService2::USER_CONNECTION_MAP, $userId);
                }
                // $pipe->hdel(RedisService2::FD_USER_MAP, $fd);
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
                    // This processes the general Redis queue (`notification_queue`)
                    $this->processQueuedNotifications();
                    break;

                case 'process_pending_db_notifications':
                    // This processes notifications from the database (new)
                    $this->processPendingNotifications();
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

                case 'mark_notification_read':
                    // Handle marking notification as read in a task worker
                    if (isset($data['user_id'], $data['notification_id'])) {
                        $this->notificationModel->markAsRead((int) $data['notification_id'], (int) $data['user_id']);
                        // Optionally, send updated count back to user
                        $fd = $this->redisService->executeWithRetry2(function ($client) use ($data) {
                            return $client->hget(RedisService2::USER_CONNECTION_MAP, (string) $data['user_id']);
                        });
                        if ($fd && $this->server->exists((int) $fd)) {
                            $this->sendNotificationCount((int) $data['user_id'], (int) $fd, true);
                        }
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
            $this->redisService->executeWithRetry2(function ($client) {
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
                // $this->refreshTTL($userId, $fd);
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
        $this->redisService->executeWithRetry2(function ($client) {
            $client->hdel(RedisService2::SERVER_REGISTRY, $this->serverId);
        });

        // Clear all timers
        foreach ($this->heartbeatTimers as $timerId) {
            Timer::clear($timerId);
        }
    }

    private function refreshTTL(int $userId, int $fd): void
    {
        try {
            // Validate inputs first
            if ($fd <= 0 || $userId <= 0) {
                throw new \InvalidArgumentException("Invalid connection parameters");
            }

            // Update Redis TTL for the user's connection
            $this->redisService->trackConnection($userId, $fd, $this->config['heartbeat_idle_time'] * 2, 'ping update'); // 5 minutes

            Console::info("Refreshed TTL for User {$userId} (fd: {$fd})");

        } catch (\Exception $e) {
            Console::error("Error refreshing TTL: " . $e->getMessage());
        }
    }

    private function sendInitialData(int $userId, int $fd): void
    {
        try {
            // Send connection acknowledgement
            $this->server->push($fd, json_encode([
                'type' => 'connection',
                'status' => 'connected',
                'message' => 'WebSocket connection established',
                'connection_id' => $fd
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
        $this->redisService->executeWithRetry2(function ($client) {
            // Get batch of queued notifications from the general Redis queue
            // This queue is populated by sendDirectNotification when a user is offline.
            $notifications = $client->lrange(RedisService2::QUEUE_PREFIX . 'notifications', 0, 99);
            if (empty($notifications) || !is_array($notifications)) {
                return;
            }

            foreach ($notifications as $notification) {
                $data = json_decode($notification, true);
                if (!$data || empty(trim($data['message'] ?? ''))) {
                    $client->lrem(RedisService2::QUEUE_PREFIX . 'notifications', 1, $notification); // remove empty/invalid
                    continue;
                }

                $userId = $data['user_id'] ?? null;
                $message = $data['message'] ?? '';
                $event = $data['event'] ?? 'notification';
                $connId = $data['connection_id'] ?? null;

                if ($userId) {
                    $this->sendDirectNotification($userId, $message, $event, $connId);
                }

                // Remove processed notification from the general queue
                $client->lrem(RedisService2::QUEUE_PREFIX . 'notifications', 1, $notification);
            }
        });
    }

    /**
     * Sends a notification directly to a user's WebSocket connection.
     * If the user is not connected, the notification is queued in Redis.
     * This method is designed to be called by internal services/tasks.
     *
     * @param int $userId The ID of the user to send the notification to.
     * @param string $message The notification message content.
     * @param string $event The event type of the notification (e.g., 'new_message', 'alert').
     * @return bool True if the notification was sent immediately, false if queued or failed.
     */
    public function sendDirectNotification(int $userId, string $message, string $event, $connId): bool
    {
        return $this->redisService->executeWithRetry2(function ($client) use ($userId, $message, $event) {
            // Get the file descriptor (fd) for the user's active connection from Redis
            $fd = $client->hget(RedisService2::USER_CONNECTION_MAP, (string) $userId);
            Console::info("Attempting to send notification to User {$userId} (fd: {$fd})");
            // Validate if the connection is currently active and known to Swoole
            if (!$this->isValidConnection($fd)) {
                // User is not connected to this server or connection is stale, queue the notification
                if (!empty($message)) {
                    // Queue notification to the general Redis queue for later processing
                    // This queue is checked by processQueuedNotifications()
                    $client->rpush(RedisService2::QUEUE_PREFIX . 'notifications', json_encode([
                        'user_id' => $userId,
                        'message' => $message,
                        'event' => $event,
                        'timestamp' => time()
                    ]));
                    Console::info("User {$userId} not connected, notification queued.");
                }
                return false;
            }

            // At this point, $fd is a valid integer representing an active connection
            $fd = (int) $fd;

            try {
                $payload = json_encode([
                    'type' => 'notification',
                    'event' => $event,
                    'message' => $message,
                    'timestamp' => time()
                ]);

                // Push the notification to the client's WebSocket connection
                $pushed = $this->server->push($fd, $payload);
                if ($pushed) {
                    Console::info("Sent direct notification to User {$userId} (fd: {$fd}).");
                } else {
                    Console::warn("Failed to push notification to fd {$fd} for User {$userId}.");
                    $this->cleanupStaleConnection($userId, $fd); // Clean up if push fails
                }
                return $pushed;
            } catch (\Exception $e) {
                Console::error("Error pushing notification to fd {$fd} for User {$userId}: " . $e->getMessage());
                $this->cleanupStaleConnection($userId, $fd); // Clean up on push error
                return false;
            }
        });
    }

    /**
     * Sends notifications to multiple users by calling sendDirectNotification for each.
     * This is a utility method for bulk operations.
     *
     * @param array $userIds An array of user IDs to send notifications to.
     * @param string $message The notification message content.
     * @param string $event The event type of the notification.
     */
    private function sendBulkNotification(array $userIds, string $message, string $event): void
    {
        foreach ($userIds as $userId) {
            $this->sendDirectNotification($userId, $message, $event);
        }
    }


    /**
     * Starts a periodic timer for a specific user to check for pending notifications.
     * This helps ensure real-time updates for connected users.
     *
     * @param int $userId The ID of the user.
     * @param int $fd The file descriptor of the user's connection.
     */
    private function startUserNotificationTimer(int $userId, int $fd): void
    {
        // Clear existing timer for this user if any, to prevent duplicates
        if (isset($this->userTimers[$userId])) {
            Timer::clear($this->userTimers[$userId]);
        }

        // Start a new timer that ticks every NOTIFICATION_CHECK_INTERVAL
        $this->userTimers[$userId] = Timer::tick(self::NOTIFICATION_CHECK_INTERVAL, function () use ($userId, $fd) {
            // If the connection no longer exists, clear the timer and remove it
            if (!$this->server->exists($fd)) {
                Timer::clear($this->userTimers[$userId]);
                unset($this->userTimers[$userId]);
                Console::info("Cleared notification timer for disconnected user {$userId} (fd: {$fd}).");
                return;
            }

            // Perform checks for this user's notifications
            $this->checkUserNotifications($userId, $fd);
        });
        Console::info("Started notification timer for user {$userId} (fd: {$fd}).");
    }

    /**
     * Checks for queued notifications for a specific user in Redis and sends them.
     * Also checks for and sends notification count changes.
     *
     * @param int $userId The ID of the user.
     * @param int $fd The file descriptor of the user's connection.
     */
    private function checkUserNotifications(int $userId, int $fd): void
    {
        // Check for queued notifications in a user-specific Redis queue
        // This queue is distinct from the general 'notification_queue'
        $queueKey = "notification_queue:{$userId}"; // Example: "notification_queue:123"
        $notification = $this->redisService->executeWithRetry2(function ($client) use ($queueKey) {
            return $client->lpop($queueKey); // Pop one notification from the left
        });

        $formattedPayload = json_encode([
            'type' => 'notification',
            'event' => 'user_notification',
            'data' => $notification, // Changed 'message' to 'data' for consistency
            'user_id' => $userId,
            'timestamp' => time()
        ]);

        if ($notification) {
            // Send the notification if found
            if ($this->server->exists($fd)) {
                // print_r($notification);
                $this->server->push($fd, $formattedPayload);
                Console::info("Sent queued user-specific notification to User {$userId} (fd: {$fd}).");
            } else {
                // If user disconnected between check and push, re-queue (or log)
                $this->redisService->executeWithRetry2(function ($client) use ($queueKey, $notification) {
                    $client->rpush($queueKey, json_encode($notification)); // Push back to the right
                });
                Console::warn("User {$userId} disconnected, re-queued notification.");
            }
        }

        // Always send updated notification counts to ensure client is in sync
        $this->sendNotificationCount($userId, $fd, true);
    }

    /**
     * Sends the current notification counts to a specific user.
     * Can optionally check for changes before sending to reduce unnecessary pushes.
     *
     * @param int $userId The ID of the user.
     * @param int $fd The file descriptor of the user's connection.
     * @param bool $checkChanges If true, only sends if counts have changed since last check.
     */
    private function sendNotificationCount(int $userId, int $fd, bool $checkChanges = false): void
    {
        try {
            $newCounts = $this->notificationModel->getNotificationCounts((string) $userId);

            if ($checkChanges) {
                $lastCountKey = "last_counts:{$userId}"; // Redis key to store last sent count
                $lastCountsJson = $this->redisService->executeWithRetry2(function ($client) use ($lastCountKey) {
                    return $client->get($lastCountKey);
                });

                // Compare JSON strings directly for simplicity
                if ($lastCountsJson && $lastCountsJson === json_encode($newCounts)) {
                    return; // No changes, do not send
                }

                // Store the new counts in Redis
                $this->redisService->executeWithRetry2(function ($client) use ($lastCountKey, $newCounts) {
                    $client->set($lastCountKey, json_encode($newCounts));
                });
            }

            $message = json_encode([
                'type' => 'notification_count',
                'event' => 'notification_count',
                'data' => $newCounts, // Changed 'message' to 'data' for counts
                'user_id' => $userId,
                'timestamp' => time()
            ]);

            if ($this->server->exists($fd)) {
                $this->server->push($fd, $message);
                // Console::info("Sent notification count to User {$userId} (fd: {$fd}).");
            } else {
                Console::warn("Attempted to send notification count to disconnected fd {$fd} for User {$userId}.");
            }
        } catch (\Exception $e) {
            Console::error("Error sending notification count for User {$userId}: " . $e->getMessage());
        }
    }


    /**
     * Processes notifications that are marked as 'pending' in the database.
     * This runs periodically as a task to ensure all database-queued notifications are sent.
     */
    private function processPendingNotifications(): void
    {
        try {
            $pending = $this->notificationModel->getPendingNotifications();
            if (empty($pending)) {
                return;
            }
            Console::info("Processing " . count($pending) . " pending notifications from database.");

            foreach ($pending as $notification) {
                if (empty($notification['user_id'])) {
                    Console::warn("Skipping notification with missing user_id: " . json_encode($notification));
                    continue;
                }
                if (empty($notification['message'])) {
                    Console::warn("Skipping notification with missing message for user {$notification['user_id']}");
                    continue;
                }

                $userId = (int) $notification['user_id'];
                $message = $notification['message'];
                $event = $notification['n_event'] ?? 'notification'; // Assuming 'n_event' is the event field

                // Attempt to send the notification directly
                $sent = $this->sendDirectNotification(
                    $userId,
                    $message,
                    $event
                );

                // Mark as sent in the database regardless of immediate push success
                // If sendDirectNotification queued it, it's considered "handled" by the WebSocket server.
                DatabaseAccessors::update(
                    "UPDATE notifications SET status = 'sent' WHERE id = ?",
                    [$notification['id']]
                );
                if ($sent) {
                    Console::info("Processed and sent DB notification ID {$notification['id']} to user {$userId}.");
                } else {
                    Console::info("Processed DB notification ID {$notification['id']} for user {$userId}, queued for later.");
                }
            }
        } catch (\Exception $e) {
            Console::error("Error processing pending notifications from database: " . $e->getMessage());
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

        $this->redisService->executeWithRetry2(function ($client) use ($userId, $fd) {
            $pipe = $client->pipeline();

            // Convert to strings to ensure consistent key types
            $pipe->hdel(RedisService2::USER_CONNECTION_MAP, (string) $userId);
            // $pipe->hdel(RedisService2::FD_USER_MAP, (string) $fd);
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

        $this->redisService->executeWithRetry2(function ($client) use ($payload) {
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
        $this->redisService->executeWithRetry2(function ($client) {
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
        $this->redisService->executeWithRetry2(function ($client) {
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

    private function cleanupStaleServers(): void
    {
        $this->redisService->executeWithRetry2(function ($client) {
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

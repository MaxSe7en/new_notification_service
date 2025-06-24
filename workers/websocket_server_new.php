<?php
require_once __DIR__ . '/../vendor/autoload.php';

use Swoole\WebSocket\Server;
use Swoole\Timer;
use Swoole\Table;
use Swoole\Coroutine;

use App\Exceptions\Console;
use App\Config\DatabaseAccessors;
use App\Models\NotificationModel;
use \Predis\Client;

class NotificationServer
{
    private Server $server;
    private Client $redis;
    private Table $connectionTable;
    private NotificationModel $notificationModel;
    private array $userTimers = [];

    private const NOTIFICATION_CHECK_INTERVAL = 1000; // 1 second
    private const HEARTBEAT_INTERVAL = 30000; // 30 seconds

    public function __construct()
    {
        $this->redis = new Client();
        $this->notificationModel = new NotificationModel();
        $this->initConnectionTable();
        $this->initServer();
    }

    private function initConnectionTable(): void
    {
        // Swoole Table for high-performance in-memory storage
        $this->connectionTable = new Table(10000);
        $this->connectionTable->column('user_id', Table::TYPE_INT);
        $this->connectionTable->column('fd', Table::TYPE_INT);
        $this->connectionTable->column('last_heartbeat', Table::TYPE_INT);
        $this->connectionTable->create();
    }

    private function initServer(): void
    {
        $this->server = new Server("0.0.0.0", 9502, SWOOLE_PROCESS, SWOOLE_SOCK_TCP | SWOOLE_SSL);

        $this->server->set([
            'ssl_cert_file' => '/etc/letsencrypt/live/winsstarts.com/fullchain.pem',
            'ssl_key_file' => '/etc/letsencrypt/live/winsstarts.com/privkey.pem',
            'ssl_protocols' => SWOOLE_SSL_TLSv1_2 | SWOOLE_SSL_TLSv1_3, // Enforce modern protocols
            'ssl_ciphers' => 'ECDHE-ECDSA-AES128-GCM-SHA256:ECDHE-RSA-AES128-GCM-SHA256:ECDHE-ECDSA-AES256-GCM-SHA384:ECDHE-RSA-AES256-GCM-SHA384', // Strong ciphers
            'ssl_prefer_server_ciphers' => true,
            'open_http2_protocol' => true,
            'heartbeat_check_interval' => 60,
            'heartbeat_idle_time' => 120,
            'max_connection' => 1024,
            'task_worker_num' => 4,
            'enable_coroutine' => true,
        ]);

        $this->registerEventHandlers();
    }

    private function registerEventHandlers(): void
    {
        $this->server->on("start", [$this, 'onStart']);
        $this->server->on("open", [$this, 'onOpen']);
        $this->server->on("message", [$this, 'onMessage']);
        $this->server->on("close", [$this, 'onClose']);
        $this->server->on("task", [$this, 'onTask']);
        $this->server->on("finish", [$this, 'onFinish']);
    }

    public function onStart(Server $server): void
    {
        echo "WebSocket Server started on ws://0.0.0.0:9502\n";

        // Global notification checker - runs every 5 seconds
        Timer::tick(5000, function () {
            $this->server->task([
                'action' => 'process_pending_notifications'
            ]);
        });

        // Connection cleanup - runs every 30 seconds
        Timer::tick(30000, function () {
            $this->cleanupStaleConnections();
        });
    }

    public function onOpen(Server $server, $request): void
    {
        try {
            $queryString = $request->server['query_string'] ?? '';
            parse_str($queryString, $query);

            if (!isset($query['userId']) || !is_numeric($query['userId'])) {
                $server->close($request->fd);
                return;
            }

            $userId = (int) $query['userId'];

            // Close any existing connection for this user
            $existingFd = $this->redis->hget("ws_connections", $userId);
            if ($existingFd && $this->connectionTable->exists($existingFd)) {
                $server->close($existingFd);
            }

            // Store connection in Swoole Table for ultra-fast access
            $this->connectionTable->set($request->fd, [
                'user_id' => $userId,
                'fd' => $request->fd,
                'last_heartbeat' => time()
            ]);

            // Also store in Redis for persistence across server restarts
            $this->redis->hset("ws_connections", $userId, $request->fd);

            echo "User $userId connected with FD {$request->fd}\n";

            // Send initial notification count
            $this->sendNotificationCount($userId, $request->fd);

            // Start personalized notification checking for this user
            $this->startUserNotificationTimer($userId, $request->fd);

            // Send welcome message
            $server->push($request->fd, json_encode([
                'type' => 'connection',
                'event' => 'connection',
                'status' => 'connected',
                'message' => 'Successfully connected to notification service'
            ]));
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
                $server->push($frame->fd, json_encode([
                    'type' => 'error',
                    'message' => 'Invalid JSON format'
                ]));
                return;
            }

            $connection = $this->connectionTable->get($frame->fd);
            if (!$connection) {
                $server->close($frame->fd);
                return;
            }

            // Update heartbeat
            $this->connectionTable->set($frame->fd, array_merge($connection, [
                'last_heartbeat' => time()
            ]));

            $this->handleMessage($data, $frame->fd, $connection['user_id']);

        } catch (\Exception $e) {
            Console::error("Message handling error: " . $e->getMessage());
            $server->push($frame->fd, json_encode([
                'type' => 'error',
                'message' => 'Internal server error'
            ]));
        }
    }

    private function handleMessage(array $data, int $fd, int $userId): void
    {
        // Console::log2('data-------------> ', $data);
        switch ($data['action'] ?? '') {
            case 'ping':
                $this->server->push($fd, json_encode(['type' => 'pong']));
                break;

            case 'mark_read':
                if (isset($data['notification_id'])) {
                    $this->server->task([
                        'action' => 'mark_notification_read',
                        'user_id' => $userId,
                        'notification_id' => $data['notification_id']
                    ]);
                }
                break;

            case 'get_notifications':
                $this->sendNotificationCount($userId, $fd);
                break;

            case 'send_notification':
                // Direct notification sending through WebSocket
                if (isset($data['target_user_id'], $data['message'])) {
                    $this->sendDirectNotification(
                        $data['target_user_id'],
                        $data['message'],
                        $data['event'] ?? 'notification'
                    );
                }
                break;

            default:
                $this->server->push($fd, json_encode([
                    'type' => 'error',
                    'message' => 'Unknown action: ' . ($data['action'] ?? 'none')
                ]));
        }
    }

    public function onClose(Server $server, int $fd): void
    {
        $connection = $this->connectionTable->get($fd);

        if ($connection) {
            $userId = $connection['user_id'];

            // Remove from connection table
            $this->connectionTable->del($fd);

            // Remove from Redis
            $this->redis->hdel("ws_connections", $userId);

            // Stop user-specific timer
            if (isset($this->userTimers[$userId])) {
                Timer::clear($this->userTimers[$userId]);
                unset($this->userTimers[$userId]);
            }

            echo "User $userId disconnected (FD: $fd)\n";
        }
    }

    public function onTask(Server $server, int $taskId, int $reactorId, $data): void
    {
        try {
            switch ($data['action']) {
                case 'process_pending_notifications':
                    $this->processPendingNotifications();
                    break;

                case 'mark_notification_read':
                    $this->markNotificationAsRead($data['user_id'], $data['notification_id']);
                    break;

                case 'send_bulk_notification':
                    $this->sendBulkNotification($data['user_ids'], $data['message'], $data['event']);
                    break;
            }
        } catch (\Exception $e) {
            Console::error("Task error: " . $e);
        }
    }

    public function onFinish(Server $server, int $taskId, string $data): void
    {
        // Task completion handling if needed
    }

    private function startUserNotificationTimer(int $userId, int $fd): void
    {
        // Clear existing timer if any
        if (isset($this->userTimers[$userId])) {
            Timer::clear($this->userTimers[$userId]);
        }

        // Start new timer for this user
        $this->userTimers[$userId] = Timer::tick(self::NOTIFICATION_CHECK_INTERVAL, function () use ($userId, $fd) {
            if (!$this->server->exist($fd)) {
                Timer::clear($this->userTimers[$userId]);
                unset($this->userTimers[$userId]);
                return;
            }

            $this->checkUserNotifications($userId, $fd);
        });
    }

    private function checkUserNotifications(int $userId, int $fd): void
    {
        // Check for queued notifications in Redis
        $queueKey = "notification_queue:$userId";
        $notification = $this->redis->lpop($queueKey);

        if ($notification) {
            $this->server->push($fd, $notification);
            // Console::info("Sent queued notification to User $userId");
        }

        // Check for notification count changes
        $this->sendNotificationCount($userId, $fd, true);
    }

    private function sendNotificationCount(int $userId, int $fd, bool $checkChanges = false): void
    {
        $newCounts = $this->notificationModel->getNotificationCounts((string) $userId);
        // Console::log2('Send pending notifications sendNotificationCount', $newCounts);

        if ($checkChanges) {
            $lastCountKey = "last_counts:$userId";
            $lastCounts = $this->redis->get($lastCountKey);

            if ($lastCounts && $lastCounts === json_encode($newCounts)) {
                return; // No changes
            }

            $this->redis->set($lastCountKey, json_encode($newCounts));
        }

        $message = json_encode([
            'type' => 'notification_count',
            'event' => 'notification_count',
            'message' => $newCounts,
            'user_id' => $userId,
            'timestamp' => time()
        ]);

        $this->server->push($fd, $message);
    }

    private function processPendingNotifications(): void
    {
        $pending = $this->notificationModel->getPendingNotifications();
        if(empty($pending)) return;
        // Console::log2('All pending notifications processPendingNotifications', $pending);
        foreach ($pending as $notification) {
            $userId = (int) $notification['user_id'] ?? 0;
            $this->sendDirectNotification(
                $userId,
                $notification['message'],
                $notification['n_event'] ?? 'notification'
            );

            // Mark as sent
            DatabaseAccessors::update(
                "UPDATE notifications SET status = 'sent' WHERE id = ?",
                [$notification['id'] ?? 0]
            );
        }
    }

    private function sendDirectNotification(int $userId, string $message, string $event): bool
    {
        // Find user's connection
        foreach ($this->connectionTable as $fd => $connection) {
            if ($connection['user_id'] === $userId) {
                $payload = json_encode([
                    'type' => 'notification',
                    'event' => $event,
                    'message' => $message,
                    'user_id' => $userId,
                    'timestamp' => time()
                ]);
                // Console::log2('Send pending notifications sendDirectNotification', $this->server->exist($fd));
                // Console::log2('Send pending notifications payload', $payload);
                // Console::log2('Send pending notifications connection', $connection);

                if ($this->server->exist($fd)) {
                    $this->server->push($fd, $payload);
                    return true;
                }
            }
        }

        // User not connected, queue for later
        $this->redis->rpush("notification_queue:$userId", json_encode([
            'type' => 'notification',
            'event' => $event,
            'message' => $message,
            'user_id' => $userId,
            'timestamp' => time()
        ]));

        return false;
    }

    private function sendBulkNotification(array $userIds, string $message, string $event): void
    {
        foreach ($userIds as $userId) {
            $this->sendDirectNotification($userId, $message, $event);
        }
    }

    private function markNotificationAsRead(int $userId, int $notificationId): void
    {
        DatabaseAccessors::update(
            "UPDATE notifications SET read_status = 'read' WHERE id = ? AND user_id = ?",
            [$notificationId, $userId]
        );
    }

    private function cleanupStaleConnections(): void
    {
        $staleThreshold = time() - 180; // 3 minutes

        foreach ($this->connectionTable as $fd => $connection) {
            if ($connection['last_heartbeat'] < $staleThreshold) {
                if ($this->server->exist($fd)) {
                    $this->server->close($fd);
                }
                $this->connectionTable->del($fd);
                Console::info("Cleaned up stale connection for FD: $fd");
            }
        }
    }

    // Public API for external notification sending
    public function sendNotificationToUser(int $userId, string $message, string $event = 'notification'): bool
    {
        return $this->sendDirectNotification($userId, $message, $event);
    }

    public function broadcastNotification(string $message, string $event = 'broadcast'): void
    {
        $payload = json_encode([
            'type' => 'broadcast',
            'event' => $event,
            'message' => $message,
            'timestamp' => time()
        ]);

        foreach ($this->connectionTable as $fd => $connection) {
            if ($this->server->exist($fd)) {
                $this->server->push($fd, $payload);
            }
        }
    }

    public function start(): void
    {
        $this->server->start();
    }
}

// Usage
$notificationServer = new NotificationServer();
$notificationServer->start();
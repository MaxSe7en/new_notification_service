<?php

namespace App\Services;

use Exception;
use App\Exceptions\Console;
use App\Config\DatabaseAccessors;
use App\Models\NotificationModel;
use \Predis\Client;

class NotificationService
{
    // private Client $redis;
    private RedisService $redisService;
    private NotificationModel $notificationModel;

    private const QUEUE_PREFIX = 'notification_queue:';
    private const CONNECTION_KEY = 'ws_connections';
    private const NOTIFICATION_KEY = 'ws_notifications';

    public function __construct()
    {
        // $this->redis = new Client();
        $this->redisService = new RedisService();
        $this->notificationModel = new NotificationModel();
    }

    /**
     * Send notification through various channels
     */
    public function sendNotification(int $userId, string $type, string $event, string $message): bool
    {
        switch ($type) {
            case 'system':
            case 'websocket':
                return $this->sendWebSocketNotification($userId, $message, $event);

            case 'email':
                return $this->sendEmail($userId, $message);

            case 'sms':
                return $this->sendSMS($userId, $message);

            case 'push':
                return $this->sendPushNotification($userId, $message);

            default:
                Console::error("Unknown notification type: $type");
                return false;
        }
    }

    /**
     * Send notification via WebSocket (immediate or queued)
     */
    private function sendWebSocketNotification(int $userId, string $message, string $event): bool
    {
        return $this->redisService->executeWithRetry(
            function ($client) use ($userId, $message, $event) {
                try {
                    // Check if user is connected via Redis
                    $fd = $client->hget(self::CONNECTION_KEY, $userId);

                    if ($fd !== null) {
                        // User is connected - send directly via WebSocket server API
                        return $this->sendDirectWebSocketMessage($userId, $message, $event);
                    } else {
                        // User not connected - queue for later delivery
                        return $this->queueNotification($client, $userId, $message, $event);
                    }
                } catch (Exception $e) {
                    Console::error("WebSocket notification error: " . $e->getMessage());
                    return false;
                }
            },
            3 // max retries
        );
    }

    /**
     * Send direct message to WebSocket server
     */
    private function sendDirectWebSocketMessage(int $userId, string $message, string $event): bool
    {
        $payload = [
            'action' => 'send_notification',
            'user_id' => $userId,
            'message' => $message,
            'event' => $event,
            'timestamp' => time()
        ];

        return $this->redisService->executeWithRetry(
            function ($client) use ($payload) {
                $result = $client->publish(self::NOTIFICATION_KEY, json_encode($payload));
                return $result > 0;
            },
            2 // max retries
        );
    }

    /**
     * Queue notification for offline users
     */
    private function queueNotification(Client $client, int $userId, string $message, string $event): bool
    {
        $notification = [
            'type' => 'notification',
            'event' => $event,
            'message' => $message,
            'user_id' => $userId,
            'timestamp' => time(),
            'queued_at' => date('Y-m-d H:i:s')
        ];

        $queueKey = self::QUEUE_PREFIX . $userId;

        // Use pipeline for atomic operations
        $result = $client->pipeline(function ($pipe) use ($queueKey, $notification) {
            $pipe->rpush($queueKey, json_encode($notification));
            $pipe->expire($queueKey, 604800); // 7 days expiration
        });

        if ($result[0] > 0) { // Check if RPUSH was successful
            Console::info("Queued notification for offline user: $userId");
            return true;
        }

        Console::error("Failed to queue notification for user: $userId");
        return false;
    }

    /**
     * Send bulk notifications efficiently
     */
    public function sendBulkNotification(array $userIds, string $message, string $event, string $type = 'websocket'): array
    {
        $results = [
            'success' => [],
            'failed' => [],
            'queued' => []
        ];

        // Batch process for efficiency
        $chunks = array_chunk($userIds, 100);

        foreach ($chunks as $chunk) {
            foreach ($chunk as $userId) {
                try {
                    $success = $this->sendNotification($userId, $type, $event, $message);

                    if ($success) {
                        $results['success'][] = $userId;
                    } else {
                        $results['failed'][] = $userId;
                    }
                } catch (Exception $e) {
                    $results['failed'][] = $userId;
                    Console::error("Bulk notification failed for user $userId: " . $e->getMessage());
                }
            }
        }

        return $results;
    }

    /**
     * Send email notification with Redis optimizations
     */
    private function sendEmail(int $userId, string $message): bool
    {
        return $this->redisService->executeWithRetry(function ($client) use ($userId, $message) {
            try {
                // Get user email from database
                $user = DatabaseAccessors::select("SELECT email FROM users_test WHERE uid = ?", [$userId]);

                if (!$user || empty($user['email'])) {
                    Console::error("No email found for user: $userId");
                    return false;
                }

                // Queue email using pipeline for atomic operation
                $result = $client->pipeline(function ($pipe) use ($userId, $message, $user) {
                    $emailData = [
                        'to' => $user['email'],
                        'subject' => 'Notification',
                        'message' => $message,
                        'user_id' => $userId,
                        'created_at' => date('Y-m-d H:i:s')
                    ];
                    $pipe->rpush('email_queue', json_encode($emailData));
                    $pipe->expire('email_queue', 86400 * 3); // 3 days expiration
                });

                if ($result[0] > 0) {
                    Console::info("Email queued for user: $userId");
                    return true;
                }
                return false;
            } catch (Exception $e) {
                Console::error("Email sending failed: " . $e->getMessage());
                return false;
            }
        });
    }

    /**
     * Send SMS notification with Redis optimizations
     */
    private function sendSMS(int $userId, string $message): bool
    {
        return $this->redisService->executeWithRetry(function ($client) use ($userId, $message) {
            try {
                // Get user phone from database
                $user = DatabaseAccessors::select("SELECT contact FROM users_test WHERE uid = ?", [$userId]);

                if (!$user || empty($user['contact'])) {
                    Console::error("No phone found for user: $userId");
                    return false;
                }

                // Queue SMS using pipeline
                $result = $client->pipeline(function ($pipe) use ($userId, $message, $user) {
                    $smsData = [
                        'to' => $user['contact'],
                        'message' => $message,
                        'user_id' => $userId,
                        'created_at' => date('Y-m-d H:i:s')
                    ];
                    $pipe->rpush('sms_queue', json_encode($smsData));
                    $pipe->expire('sms_queue', 86400 * 2); // 2 days expiration
                });

                return $result[0] > 0;
            } catch (Exception $e) {
                Console::error("SMS sending failed: " . $e->getMessage());
                return false;
            }
        });
    }

    /**
     * Send push notification with Redis optimizations
     */
    private function sendPushNotification(int $userId, string $message): bool
    {
        return $this->redisService->executeWithRetry(function ($client) use ($userId, $message) {
            try {
                // Get user FCM token from database
                $user = DatabaseAccessors::select("SELECT fcm_token FROM users_test WHERE uid = ?", [$userId]);

                if (!$user || empty($user['fcm_token'])) {
                    Console::error("No FCM token found for user: $userId");
                    return false;
                }

                // Queue push notification
                $result = $client->pipeline(function ($pipe) use ($userId, $message, $user) {
                    $pushData = [
                        'token' => $user['fcm_token'],
                        'title' => 'Notification',
                        'message' => $message,
                        'user_id' => $userId,
                        'created_at' => date('Y-m-d H:i:s')
                    ];
                    $pipe->rpush('push_queue', json_encode($pushData));
                    $pipe->expire('push_queue', 86400 * 1); // 1 day expiration
                });

                return $result[0] > 0;
            } catch (Exception $e) {
                Console::error("Push notification failed: " . $e->getMessage());
                return false;
            }
        });
    }
    /**
     * Get notification statistics with SCAN instead of KEYS
     */
    public function getNotificationStats(): array
    {
        return $this->redisService->executeWithRetry(function ($client) {
            // Use pipeline to get all stats in one roundtrip
            $result = $client->pipeline(function ($pipe) {
                $pipe->hlen(self::CONNECTION_KEY);
                $pipe->llen('email_queue');
                $pipe->llen('sms_queue');
                $pipe->llen('push_queue');
            });

            return [
                'total_connections' => $result[0],
                'queued_notifications' => $this->getQueuedNotificationCount($client),
                'email_queue_size' => $result[1],
                'sms_queue_size' => $result[2],
                'push_queue_size' => $result[3]
            ];
        });
    }

    /**
     * Get total queued notifications using SCAN
     */
    private function getQueuedNotificationCount(Client $client): int
    {
        $iterator = null;
        $total = 0;

        do {
            $result = $client->scan($iterator, ['match' => self::QUEUE_PREFIX . '*', 'count' => 100]);
            $iterator = $result[0];
            $keys = $result[1];

            if (!empty($keys)) {
                $pipe = $client->pipeline();
                foreach ($keys as $key) {
                    $pipe->llen($key);
                }
                $counts = $pipe->execute();
                $total += array_sum($counts);
            }
        } while ($iterator > 0);

        return $total;
    }

    /**
     * Optimized cleanup using SCAN and pipelining
     */
    public function cleanupOldNotifications(int $daysOld = 7): int
    {
        return $this->redisService->executeWithRetry(function ($client) use ($daysOld) {
            $cutoff = time() - ($daysOld * 24 * 60 * 60);
            $cleaned = 0;
            $iterator = null;

            do {
                $result = $client->scan($iterator, ['match' => self::QUEUE_PREFIX . '*', 'count' => 50]);
                $iterator = $result[0];
                $keys = $result[1];

                foreach ($keys as $key) {
                    $notifications = $client->lrange($key, 0, -1);
                    $toRemove = [];

                    foreach ($notifications as $notification) {
                        $data = json_decode($notification, true);
                        if ($data && isset($data['timestamp']) && $data['timestamp'] < $cutoff) {
                            $toRemove[] = $notification;
                        }
                    }

                    if (!empty($toRemove)) {
                        $pipe = $client->pipeline();
                        foreach ($toRemove as $notification) {
                            $pipe->lrem($key, 1, $notification);
                        }
                        $pipe->execute();
                        $cleaned += count($toRemove);
                    }
                }
            } while ($iterator > 0);

            Console::info("Cleaned up $cleaned old notifications");
            return $cleaned;
        });
    }

    /**
     * Enhanced WebSocket event subscription
     */
    public function subscribeToWebSocketEvents(): void
    {
        $this->redisService->executeWithRetry(function ($client) {
            $pubsub = $client->pubSubLoop();
            $pubsub->subscribe(self::NOTIFICATION_KEY);

            try {
                foreach ($pubsub as $message) {
                    if ($message->kind === 'message') {
                        $data = json_decode($message->payload, true);

                        if ($data && isset($data['action'])) {
                            switch ($data['action']) {
                                case 'user_connected':
                                    $this->handleUserConnection($data['user_id']);
                                    break;

                                case 'user_disconnected':
                                    $this->handleUserDisconnection($data['user_id']);
                                    break;
                            }
                        }
                    }
                }
            } catch (Exception $e) {
                Console::error("PubSub error: " . $e->getMessage());
                $pubsub->close();
                throw $e;
            }
        });
    }

    private function handleUserConnection(int $userId): void
    {
        $this->redisService->executeWithRetry(function ($client) use ($userId) {
            Console::info("User $userId connected - processing queued notifications");

            $queueKey = self::QUEUE_PREFIX . $userId;
            $notifications = $client->lrange($queueKey, 0, -1);

            // Process notifications in chunks to prevent memory issues
            foreach (array_chunk($notifications, 50) as $chunk) {
                foreach ($chunk as $notification) {
                    $this->sendDirectWebSocketMessage($userId, $notification, 'queued');
                }
            }

            // Clear the queue
            $client->del($queueKey);
        });
    }


    private function handleUserDisconnection(int $userId): void
    {
        Console::info("User $userId disconnected");
    }
}

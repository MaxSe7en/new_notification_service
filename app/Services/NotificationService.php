<?php

namespace App\Services;

use Exception;
use App\Exceptions\Console;
use App\Config\DatabaseAccessors;
use App\Models\NotificationModel;
use \Predis\Client;

class NotificationService
{
    private Client $redis;
    private NotificationModel $notificationModel;
    
    private const QUEUE_PREFIX = 'notification_queue:';
    private const CONNECTION_KEY = 'ws_connections';
    
    public function __construct()
    {
        $this->redis = new Client();
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
        try {
            // Console::error("WebSocket notification sending: " . $message);
            // Check if user is connected via Redis
            $fd = $this->redis->hget(self::CONNECTION_KEY, $userId);
            
            if ($fd !== null) {
                // User is connected - send directly via WebSocket server API
                return $this->sendDirectWebSocketMessage($userId, $message, $event);
            } else {
                // User not connected - queue for later delivery
                return $this->queueNotification($userId, $message, $event);
            }
        } catch (Exception $e) {
            Console::error("WebSocket notification error: " . $e->getMessage());
            return false;
        }
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
        
        // Use Redis pub/sub for internal server communication
        return $this->redis->publish('ws_notifications', json_encode($payload)) > 0;
    }
    
    /**
     * Queue notification for offline users
     */
    private function queueNotification(int $userId, string $message, string $event): bool
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
        $this->redis->rpush($queueKey, json_encode($notification));
        
        // Set expiration for queue (7 days)
        $this->redis->expire($queueKey, 604800);
        
        Console::info("Queued notification for offline user: $userId");
        return true;
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
     * Send email notification
     */
    private function sendEmail(int $userId, string $message): bool
    {
        try {
            // Get user email from database
            $user = DatabaseAccessors::select("SELECT email FROM users_test WHERE uid = ?", [$userId]);
            
            if (!$user || empty($user['email'])) {
                Console::error("No email found for user: $userId");
                return false;
            }
            
            // Queue email for background processing
            $emailData = [
                'to' => $user['email'],
                'subject' => 'Notification',
                'message' => $message,
                'user_id' => $userId,
                'created_at' => date('Y-m-d H:i:s')
            ];
            
            $this->redis->rpush('email_queue', json_encode($emailData));
            
            Console::info("Email queued for user: $userId");
            return true;
            
        } catch (Exception $e) {
            Console::error("Email sending failed: " . $e->getMessage());
            return false;
        }
    }
    
    /**
     * Send SMS notification
     */
    private function sendSMS(int $userId, string $message): bool
    {
        try {
            // Get user phone from database
            $user = DatabaseAccessors::select("SELECT contact FROM users_test WHERE uid = ?", [$userId]);
            
            if (!$user || empty($user['contact'])) {
                Console::error("No phone found for user: $userId");
                return false;
            }
            
            // Queue SMS for background processing
            $smsData = [
                'to' => $user['contact'],
                'message' => $message,
                'user_id' => $userId,
                'created_at' => date('Y-m-d H:i:s')
            ];
            
            $this->redis->rpush('sms_queue', json_encode($smsData));
            
            Console::info("SMS queued for user: $userId");
            return true;
            
        } catch (Exception $e) {
            Console::error("SMS sending failed: " . $e->getMessage());
            return false;
        }
    }
    
    /**
     * Send push notification
     * @status yet to implement
     */
    private function sendPushNotification(int $userId, string $message): bool
    {
        try {
            // Get user FCM token from database
            $user = DatabaseAccessors::select("SELECT fcm_token FROM users_test WHERE uid = ?", [$userId]);
            
            if (!$user || empty($user['fcm_token'])) {
                Console::error("No FCM token found for user: $userId");
                return false;
            }
            
            // Queue push notification for background processing
            $pushData = [
                'token' => $user['fcm_token'],
                'title' => 'Notification',
                'message' => $message,
                'user_id' => $userId,
                'created_at' => date('Y-m-d H:i:s')
            ];
            
            $this->redis->rpush('push_queue', json_encode($pushData));
            
            Console::info("Push notification queued for user: $userId");
            return true;
            
        } catch (Exception $e) {
            Console::error("Push notification failed: " . $e->getMessage());
            return false;
        }
    }
    
    /**
     * Get notification statistics
     */
    public function getNotificationStats(): array
    {
        return [
            'total_connections' => $this->redis->hlen(self::CONNECTION_KEY),
            'queued_notifications' => $this->getQueuedNotificationCount(),
            'email_queue_size' => $this->redis->llen('email_queue'),
            'sms_queue_size' => $this->redis->llen('sms_queue'),
            'push_queue_size' => $this->redis->llen('push_queue')
        ];
    }
    
    /**
     * Get total queued notifications across all users
     */
    private function getQueuedNotificationCount(): int
    {
        $keys = $this->redis->keys(self::QUEUE_PREFIX . '*');
        $total = 0;
        
        foreach ($keys as $key) {
            $total += $this->redis->llen($key);
        }
        
        return $total;
    }
    
    /**
     * Clear old queued notifications
     */
    public function cleanupOldNotifications(int $daysOld = 7): int
    {
        $cutoff = time() - ($daysOld * 24 * 60 * 60);
        $cleaned = 0;
        
        $keys = $this->redis->keys(self::QUEUE_PREFIX . '*');
        
        foreach ($keys as $key) {
            $notifications = $this->redis->lrange($key, 0, -1);
            
            foreach ($notifications as $index => $notification) {
                $data = json_decode($notification, true);
                
                if ($data && isset($data['timestamp']) && $data['timestamp'] < $cutoff) {
                    $this->redis->lrem($key, 1, $notification);
                    $cleaned++;
                }
            }
        }
        
        Console::info("Cleaned up $cleaned old notifications");
        return $cleaned;
    }
    
    /**
     * Subscribe to WebSocket events for real-time processing
     */
    public function subscribeToWebSocketEvents(): void
    {
        $this->redis->subscribe(['ws_notifications'], function ($redis, $channel, $message) {
            $data = json_decode($message, true);
            
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
        });
    }
    
    private function handleUserConnection(int $userId): void
    {
        Console::info("User $userId connected - processing queued notifications");
        
        // Process any queued notifications immediately
        $queueKey = self::QUEUE_PREFIX . $userId;
        $notifications = $this->redis->lrange($queueKey, 0, -1);
        
        foreach ($notifications as $notification) {
            $this->sendDirectWebSocketMessage($userId, $notification, 'queued');
        }
        
        // Clear the queue
        $this->redis->del($queueKey);
    }
    
    private function handleUserDisconnection(int $userId): void
    {
        Console::info("User $userId disconnected");
    }
}
<?php

namespace App\Controllers;

use App\Models\NotificationModel;
use App\Services\NotificationService;
use App\Exceptions\Console;

class NotificationController
{
    private NotificationModel $notificationModel;
    private NotificationService $notificationService;

    public function __construct()
    {
        $this->notificationModel = new NotificationModel();
        $this->notificationService = new NotificationService();
    }

    /**
     * Send individual notification
     */
    public function sendNotification(int $userId, string $type, string $message, string $event = 'notification'): array
    {
        try {
            // Store in database first
            $stored = $this->notificationModel->create($userId, $type, $message);

            if (!$stored) {
                return [
                    'success' => false,
                    'message' => 'Failed to store notification in database'
                ];
            }

            // Send via service
            $sent = $this->notificationService->sendNotification($userId, $type, $event, $message);

            return [
                'success' => $sent,
                'message' => $sent ? 'Notification sent successfully' : 'Notification queued for delivery'
            ];

        } catch (\Exception $e) {
            Console::error("Notification sending failed: " . $e->getMessage());

            return [
                'success' => false,
                'message' => 'Internal server error'
            ];
        }
    }

    /**
     * Send bulk notifications efficiently
     */
    public function sendBulkNotification(array $userIds, string $type, string $message, string $event = 'notification'): array
    {
        try {
            // Validate input
            if (empty($userIds) || !is_array($userIds)) {
                return [
                    'success' => false,
                    'message' => 'Invalid user IDs provided'
                ];
            }

            // Limit bulk size to prevent abuse
            if (count($userIds) > 1000) {
                return [
                    'success' => false,
                    'message' => 'Bulk notification limited to 1000 users per request'
                ];
            }

            // Store all notifications in database first
            $this->storeBulkNotifications($userIds, $type, $message);

            // Send via service
            $results = $this->notificationService->sendBulkNotification($userIds, $message, $event, $type);

            return [
                'success' => true,
                'message' => 'Bulk notifications processed',
                'details' => [
                    'total' => count($userIds),
                    'sent' => count($results['success']),
                    'failed' => count($results['failed']),
                    'queued' => count($results['queued'] ?? [])
                ]
            ];

        } catch (\Exception $e) {
            Console::error("Bulk notification failed: " . $e->getMessage());

            return [
                'success' => false,
                'message' => 'Bulk notification processing failed'
            ];
        }
    }

    /**
     * Process and send all pending notices
     */
    public function processAndSendNotices(): array
    {
        try {
            $notices = $this->notificationModel->getAllPendingNotices();
            $processed = 0;
            $failed = 0;

            foreach ($notices as $notice) {
                try {
                    if ($notice['ms_type'] === 'general') {
                        // Send to all users efficiently
                        $users = $this->notificationModel->getAllUsers();
                        $userIds = array_column($users, 'uid');

                        $result = $this->notificationService->sendBulkNotification(
                            $userIds,
                            $notice['content'],
                            'general_notice',
                            'websocket'
                        );

                        $processed += count($result['success']);
                        $failed += count($result['failed']);

                    } else {
                        // Handle personal notices
                        if (!empty($notice['user_id'])) {
                            $sent = $this->notificationService->sendNotification(
                                $notice['user_id'],
                                'personal',
                                'personal_notice',
                                $notice['content']
                            );

                            $sent ? $processed++ : $failed++;
                        }
                    }

                    // Mark notice as processed
                    $this->notificationModel->markNoticeAsSent($notice['msg_id']);

                } catch (\Exception $e) {
                    Console::error("Failed to process notice {$notice['msg_id']}: " . $e->getMessage());
                    $failed++;
                }
            }

            return [
                'success' => true,
                'message' => 'Notices processed successfully',
                'details' => [
                    'processed' => $processed,
                    'failed' => $failed,
                    'total' => count($notices)
                ]
            ];

        } catch (\Exception $e) {
            Console::error("Notice processing failed: " . $e->getMessage());

            return [
                'success' => false,
                'message' => 'Failed to process notices'
            ];
        }
    }

    /**
     * Get notification counts for a user
     */
    public function getNotificationCounts(int $userId): array
    {
        try {
            return $this->notificationModel->getNotificationCounts($userId);
        } catch (\Exception $e) {
            Console::error("Failed to get notification counts for user {$userId}: " . $e->getMessage());
            return [
                'system_notifications' => 0,
                'general_notices' => 0,
                'personal_notifications' => 0
            ];
        }
    }

    /**
     * Mark notification as read
     */
    public function markAsRead(int $notificationId, int $userId): array
    {
        try {
            $updated = $this->notificationModel->markAsRead($notificationId, $userId);

            if ($updated) {
                // Trigger real-time count update
                $this->notificationService->sendNotificationCountUpdate($userId);

                return [
                    'success' => true,
                    'message' => 'Notification marked as read'
                ];
            }

            return [
                'success' => false,
                'message' => 'Failed to mark notification as read'
            ];

        } catch (\Exception $e) {
            Console::error("Failed to mark notification as read: " . $e->getMessage());

            return [
                'success' => false,
                'message' => 'Internal server error'
            ];
        }
    }

    /**
     * Store bulk notifications efficiently
     */
    private function storeBulkNotifications(array $userIds, string $type, string $message): void
    {
        try {
            $this->notificationModel->createBulk($userIds, $type, $message);
        } catch (\Exception $e) {
            Console::error("Failed to store bulk notifications: " . $e->getMessage());
            throw $e;
        }
    }

    /**
     * Get user's notifications with pagination
     */
    public function getUserNotifications(int $userId, int $page = 1, int $limit = 20): array
    {
        try {
            $offset = ($page - 1) * $limit;
            $notifications = $this->notificationModel->getUserNotifications($userId, $limit, $offset);
            $total = $this->notificationModel->getUserNotificationCount($userId);

            return [
                'success' => true,
                'data' => [
                    'notifications' => $notifications,
                    'pagination' => [
                        'current_page' => $page,
                        'total_pages' => ceil($total / $limit),
                        'total_count' => $total,
                        'per_page' => $limit
                    ]
                ]
            ];

        } catch (\Exception $e) {
            Console::error("Failed to get user notifications: " . $e->getMessage());

            return [
                'success' => false,
                'message' => 'Failed to retrieve notifications'
            ];
        }
    }

    /**
     * Delete old notifications (cleanup)
     */
    public function cleanupOldNotifications(int $daysOld = 30): array
    {
        try {
            $deleted = $this->notificationModel->deleteOldNotifications($daysOld);

            return [
                'success' => true,
                'message' => "Cleaned up {$deleted} old notifications"
            ];

        } catch (\Exception $e) {
            Console::error("Cleanup failed: " . $e->getMessage());

            return [
                'success' => false,
                'message' => 'Cleanup operation failed'
            ];
        }
    }

    /**
     * Send notification to specific user groups
     */
    // public function sendGroupNotification(array $groups, string $message, string $type = 'group'): array
    // {
    //     try {
    //         $userIds = $this->notificationModel->getUsersByGroups($groups);

    //         if (empty($userIds)) {
    //             return [
    //                 'success' => false,
    //                 'message' => 'No users found in specified groups'
    //             ];
    //         }

    //         return $this->sendBulkNotification($userIds, $type, $message, 'group_notification');

    //     } catch (\Exception $e) {
    //         Console::error("Group notification failed: " . $e->getMessage());

    //         return [
    //             'success' => false,
    //             'message' => 'Failed to send group notification'
    //         ];
    //     }
    // }
}
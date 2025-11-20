<?php

namespace App\Models;

use PDO;
use PDOException;
use App\Config\Database;
use App\Exceptions\Console;
use App\Config\DatabaseAccessors;

class NotificationModel
{
    private $db;

    public function __construct()
    {
        // Use the static connect method from DatabaseAccessors
        // This will return the shared, managed PDO instance
        $this->db = DatabaseAccessors::connect();
    }

    public function create($userId, $type, $message)
    {
        $sql = "INSERT INTO notifications (user_id, ms_type, message, status) VALUES (?, ?, ?, 'pending')";
        return DatabaseAccessors::insert($sql, [$userId, $type, $message]);
    }

    public function getPendingNotifications()
    {
        $sql = "SELECT * FROM notifications WHERE status = 'pending'";

        return DatabaseAccessors::selectAll($sql);
    }

    public function getPendingNotices()
    {
        $sql = "SELECT * FROM notices WHERE ms_type = 'personal'";
        return DatabaseAccessors::selectAll($sql);
    }

    public function getUserNotices($userId)
    {
        $sql = "
            SELECT n.msg_id, n.title, n.content, n.send_by, n.ms_type, n.created_at, n.ms_status 
            FROM notices n
            LEFT JOIN notice_users nu ON n.msg_id = nu.notice_id AND nu.user_id = :userId
            WHERE n.ms_type = 'general' OR nu.user_id IS NOT NULL
            ORDER BY n.created_at DESC
        ";
        return DatabaseAccessors::selectAll($sql, [$userId]);
    }

    public function getAllPendingNotices()
    {
        $sql = "
            SELECT n.msg_id, n.title, n.content, n.send_by, n.ms_type, n.created_at, n.ms_status, nu.user_id
            FROM notices n
            LEFT JOIN notice_users nu ON n.msg_id = nu.notice_id
            WHERE n.ms_status = 'Active'
            ORDER BY n.created_at DESC
        ";
        return DatabaseAccessors::selectAll($sql);
    }

    public function getAllUsers()
    {
        $sql = "SELECT uid FROM users_test";
        return DatabaseAccessors::selectAll($sql);
    }

    public function markNoticeAsSent($msgId)
    {
        $sql = "UPDATE notices SET ms_status = 'sent' WHERE msg_id = :msgId";
        DatabaseAccessors::update($sql, [$msgId]);
    }

    public function getGeneralNotices()
    {
        $sql = "SELECT * FROM notices WHERE ms_type = 'general'";
        return DatabaseAccessors::selectAll($sql);
    }

    public function getNotificationCounts(string $userId)
    {
        try {
            $totalSql = "SELECT COUNT(*) as total FROM notifications WHERE user_id = :userId AND read_status = 'unread'";

            $totalCount = DatabaseAccessors::select($totalSql, [$userId]);

            $generalSql = "SELECT * FROM notices WHERE ms_type = 'general' AND ms_status = 'active'";
            $generalNote = DatabaseAccessors::selectAll($generalSql);
            $generalCount = count($generalNote);
            $personalSql = "
                SELECT COUNT(*) as personal
                FROM notices
                LEFT JOIN notice_users ON notices.msg_id = notice_users.msg_id
                WHERE notices.ms_type != 'general'
                AND notices.ms_status = 'active'
                AND notice_users.read_status = 'unread'
                AND notice_users.user_id = :userId
            ";
            $personalCount = DatabaseAccessors::select($personalSql, [$userId]);
            // Console::log2('countssss ', $generalNote);
            return [
                'system_notifications' => $totalCount['total'] ?? 0,
                'general_notices' => $generalCount ?? 0,
                'personal_notifications' => $personalCount['personal'] ?? 0,
                'announcements' => $generalNote
            ];
        } catch (PDOException $e) {
            Console::log2('Notification count error: ', $e);
            return [
                'system_notifications' => 0,
                'general_notices' => 0,
                'personal_notifications' => 0
            ];
        }
    }

    public function createBulk(array $userIds, string $type, string $message): bool
    {
        try {
            $values = [];
            $params = [];
            $placeholder = [];

            foreach ($userIds as $index => $userId) {
                $userParam = ":user_id_{$index}";
                $typeParam = ":type_{$index}";
                $messageParam = ":message_{$index}";

                $placeholder[] = "({$userParam}, {$typeParam}, {$messageParam}, 'pending', NOW())";

                $params[$userParam] = $userId;
                $params[$typeParam] = $type;
                $params[$messageParam] = $message;
            }

            $sql = "INSERT INTO notifications (user_id, ms_type, message, status, created_at) VALUES "
                . implode(', ', $placeholder);

            return DatabaseAccessors::insert($sql, $params);

        } catch (\Exception $e) {
            throw new \Exception("Bulk insert failed: " . $e->getMessage());
        }
    }

    /**
     * Get user notifications with pagination
     */
    public function getUserNotificationsOl(int $userId, int $limit = 20, int $offset = 0): array
    {
        $sql = "SELECT * FROM notifications
                WHERE user_id = :user_id
                ORDER BY created_at DESC
                LIMIT :limit OFFSET :offset";

        return DatabaseAccessors::selectAll($sql, [
            ':user_id' => $userId,
            ':limit' => $limit,
            ':offset' => $offset
        ]);
    }

    /**
     * Get user notification count
     */
    public function getUserNotificationCount(int $userId): int
    {
        $result = DatabaseAccessors::select(
            "SELECT COUNT(*) as count FROM notifications WHERE user_id = :user_id",
            [':user_id' => $userId]
        );

        return $result['count'] ?? 0;
    }

    /**
     * Mark notification as read
     */
    public function markAsRead(int $notificationId, int $userId): bool
    {
        return DatabaseAccessors::update(
            "UPDATE notifications SET read_status = 'read', read_at = NOW() 
             WHERE id = :id AND user_id = :user_id",
            [':id' => $notificationId, ':user_id' => $userId]
        );
    }

    /**
     * Delete old notifications
     */
    public function deleteOldNotifications(int $daysOld): int
    {
        $sql = "DELETE FROM notifications 
                WHERE created_at < DATE_SUB(NOW(), INTERVAL :days DAY)";

        $result = DatabaseAccessors::delete($sql, [':days' => $daysOld]);

        return $result ? $this->db->rowCount() : 0;
    }

    /**
     * Get users by groups
     */
    public function getUsersByGroups(array $groups): array
    {
        $placeholders = str_repeat('?,', count($groups) - 1) . '?';

        $sql = "SELECT DISTINCT user_id FROM user_groups 
                WHERE group_name IN ({$placeholders})";

        $result = DatabaseAccessors::selectAll($sql, $groups);

        return array_column($result, 'user_id');
    }
}

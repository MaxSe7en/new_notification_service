<?php

use Swoole\Timer;
use Swoole\Process;
use Predis\Client;

use App\Models\NotificationModel;
use App\Config\DatabaseAccessors;
use App\Services\NotificationService;
use App\Controllers\NotificationController;

require 'vendor/autoload.php';


$redisCache = new Client();

echo "Starting Notification Worker...\n";

Timer::tick(5000, function () use ($redisCache) {
    $db = new DatabaseAccessors();
    echo "Started ticking Worker...\n";
    $notificationModel = new NotificationModel();
    $pending = $notificationModel->getPendingNotifications();

    foreach ($pending as $notification) {
        echo "Sending notification to ..." . json_encode($notification) . "\n";
        $userId = $notification['user_id'];
        // Get user's WebSocket FD

        $success = NotificationService::sendNotification(
            $notification['user_id'],
            $notification['ms_type'],
            $notification['n_event'],
            $notification['message']
        );

        $status = $success ? 'sent' : 'failed';
        $updateResult = $db->update("UPDATE notifications SET status = ? WHERE id = ?", [$status, $notification['id']]);
        if($updateResult){
            echo "Failed to update status for notification ID {$notification['user_id']}";
        }
        echo "Processed Notification ID: {$notification['id']} - Status: {$status}\n";
        echo "Sending: {$notification['message']}\n";
    }

    $connectedUsers = $redisCache->hgetall("connected_users");

    foreach ($connectedUsers as $userId => $fd) {
        // Get the notification counts
        $newCounts = $notificationModel->getNotificationCounts($userId);

        // Generate a unique key for storing last counts
        $lastCountKey = "last_notification_counts:{$userId}";

        // Retrieve last stored counts
        $lastCountsJson = $redisCache->get($lastCountKey);
        $lastCounts = $lastCountsJson ? json_decode($lastCountsJson, true) : [
            'system_notifications' => 0,
            'general_notices' => 0,
            'personal_notifications' => 0
        ];

        // Check if any count has changed
        $countChanged =
            $newCounts['system_notifications'] != $lastCounts['system_notifications'] ||
            $newCounts['general_notices'] != $lastCounts['general_notices'] ||
            $newCounts['personal_notifications'] != $lastCounts['personal_notifications'];
        $message = json_encode([
            'type' => 'notification_count',
            'message' => $newCounts,
            'event' => 'notification_count',
            'user_id' => $userId
        ]);

        if ($countChanged) {
            NotificationService::queueNotification(
                $userId,
                $message
            );
            // Update the last counts in Redis
            $redisCache->set($lastCountKey, json_encode($newCounts));

            echo "Sent update to User $userId: " . json_encode($newCounts) . " notifications\n";
        }
    }

    echo "Finished ticking Worker...\n";
});

Process::wait();
<?php
// File: /app/Services/NotificationProcessor.php
namespace App\Services;

use App\Config\DatabaseAccessors;
use App\Exceptions\Console;
use App\Services\RedisService;
use Predis\Client;

class NotificationProcessor
{
    private RedisService $redisService;
    private int $batchSize;
    private int $rateLimit;
    private int $rateLimitInterval;

    public function __construct(
        RedisService $redisService,
        int $batchSize = 100,
        int $rateLimit = 1000,
        int $rateLimitInterval = 60
    ) {
        $this->redisService = $redisService;
        $this->batchSize = $batchSize;
        $this->rateLimit = $rateLimit;
        $this->rateLimitInterval = $rateLimitInterval;
    }

    public function processBulkNotifications(array $userIds, string $message, string $event): array
    {
        return $this->redisService->executeWithRetry(function($client) use ($userIds, $message, $event) {
            $results = [
                'success' => [],
                'failed' => [],
                'rate_limited' => []
            ];

            // Check rate limit
            if (!$this->checkRateLimit($client, 'bulk_notify', $this->rateLimit, $this->rateLimitInterval)) {
                return ['rate_limited' => $userIds];
            }

            // Process in batches
            foreach (array_chunk($userIds, $this->batchSize) as $batch) {
                $batchResults = $this->processBatch($client, $batch, $message, $event);
                $results['success'] = array_merge($results['success'], $batchResults['success']);
                $results['failed'] = array_merge($results['failed'], $batchResults['failed']);
            }

            return $results;
        });
    }

    private function processBatch(Client $client, array $userIds, string $message, string $event): array
    {
        $pipe = $client->pipeline();
        $results = ['success' => [], 'failed' => []];
        $timestamp = time();

        foreach ($userIds as $userId) {
            $notification = [
                'user_id' => $userId,
                'message' => $message,
                'event' => $event,
                'attempts' => 0,
                'created_at' => $timestamp,
                'last_attempt' => 0
            ];

            $pipe->hset(
                'pending_notifications',
                "{$userId}:{$timestamp}",
                json_encode($notification)
            );
        }

        $pipe->expire('pending_notifications', 86400); // 1 day TTL
        $pipe->execute();

        // Queue for background processing
        $client->rpush('notification_batches', json_encode([
            'user_ids' => $userIds,
            'message' => $message,
            'event' => $event,
            'created_at' => $timestamp
        ]));

        return ['success' => $userIds]; // Optimistic - actual results will come from background worker
    }

    private function checkRateLimit(Client $client, string $key, int $limit, int $interval): bool
    {
        $current = $client->incr("rate_limit:{$key}");
        if ($current === 1) {
            $client->expire("rate_limit:{$key}", $interval);
        }
        return $current <= $limit;
    }
}

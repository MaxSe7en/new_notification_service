<?php
// File: /workers/RatchetServer.php
namespace App\Workers;

use Ratchet\MessageComponentInterface;
use Ratchet\ConnectionInterface;
use App\Services\RedisService;
use App\Models\NotificationModel;
use App\Exceptions\Console;

class RatchetServer implements MessageComponentInterface
{
    private \SplObjectStorage $clients;
    private RedisService $redisService;
    private NotificationModel $notificationModel;

    public function __construct()
    {
        $this->clients = new \SplObjectStorage;
        $this->redisService = new RedisService();
        $this->notificationModel = new NotificationModel();
    }

    public function onOpen(ConnectionInterface $conn)
    {
        // Parse userId from query string
        parse_str($conn->httpRequest->getUri()->getQuery(), $query);
        $userId = (int) ($query['userId'] ?? 0);
        if (!$userId) {
            $conn->close();
            return;
        }

        $conn->userId = $userId;
        $this->clients->attach($conn);

        $this->redisService->executeWithRetry(function ($client) use ($userId, $conn) {
            $client->hset('ws_connections', $userId, spl_object_hash($conn));
        });

        // Send initial data
        $counts = $this->notificationModel->getNotificationCounts((string) $userId);
        $conn->send(json_encode([
            'type' => 'notification_count',
            'data' => $counts
        ]));
    }

    public function onMessage(ConnectionInterface $from, $msg)
    {
        $data = json_decode($msg, true);
        if (!$data) return;

        switch ($data['action'] ?? '') {
            case 'ping':
                $from->send(json_encode(['type' => 'pong']));
                break;
            case 'get_notifications':
                $counts = $this->notificationModel->getNotificationCounts((string) $from->userId);
                $from->send(json_encode([
                    'type' => 'notification_count',
                    'data' => $counts
                ]));
                break;
        }
    }

    public function onClose(ConnectionInterface $conn)
    {
        $this->clients->detach($conn);
        if (isset($conn->userId)) {
            $this->redisService->executeWithRetry(function ($client) use ($conn) {
                $client->hdel('ws_connections', $conn->userId);
            });
        }
    }

    public function onError(ConnectionInterface $conn, \Exception $e)
    {
        Console::error("Ratchet error: " . $e->getMessage());
        $conn->close();
    }

    public function broadcast(string $message): void
    {
        foreach ($this->clients as $client) {
            $client->send($message);
        }
    }
}

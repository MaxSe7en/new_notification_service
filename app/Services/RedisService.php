<?php
// File: /app/Services/RedisService.php
namespace App\Services;

use Predis\Client;
use App\Exceptions\Console;

class RedisService
{
    private Client $client;
    private array $config;
    private bool $isCluster;
    private const QUEUE_PREFIX = 'notification_queue:';

    public function __construct()
    {
        $this->config = [
            'cluster' => getenv('REDIS_CLUSTER') ?: false,
            'scheme' => getenv('REDIS_SCHEME') ?: 'tcp',
            'host' => getenv('REDIS_HOST') ?: '127.0.0.1',//'192.168.1.51',
            'port' => getenv('REDIS_PORT') ?: 6379,
            'password' => getenv('REDIS_PASSWORD') ?: null,
            'read_write_timeout' => 0, // seconds
            'failover' => 'distribute', // or 'error'
        ];

        $this->isCluster = filter_var($this->config['cluster'], FILTER_VALIDATE_BOOLEAN);
        $this->initializeClient();
    }

    private function initializeClient(): void
    {
        try {
            if ($this->isCluster) {
                $this->client = new Client([
                    'cluster' => 'redis',
                    'parameters' => [
                        'password' => $this->config['password'],
                    ],
                    'nodes' => [
                        'tcp://' . $this->config['host'] . ':' . $this->config['port'],
                        // Add more nodes if available
                    ],
                ]);
            } else {
                $this->client = new Client([
                    'scheme' => $this->config['scheme'],
                    'host' => $this->config['host'],
                    'port' => $this->config['port'],
                    'password' => $this->config['password'],
                    'read_write_timeout' => $this->config['read_write_timeout'],
                ]);
            }

            // Test connection
            $this->client->ping();
        } catch (\Exception $e) {
            Console::error("Redis connection failed: " . $e->getMessage());
            throw $e;
        }
    }

    public function getClient(): Client
    {
        try {
            $this->client->ping();
        } catch (\Exception $e) {
            Console::warn("Redis connection lost, reconnecting... " . $e->getMessage());
            $this->initializeClient();
        }

        return $this->client;
    }

    public function safeHGetAll($key): array {
        return $this->executeWithRetry(function ($client) use ($key) {
            $type = $client->type($key);
            if ($type !== 'hash') {
                $client->del($key);
                $client->hset($key, 'init', time());
                $client->hdel($key, 'init');
                return [];
            }
            $data = $client->hgetall($key);
            return is_array($data) ? $data : [];
        });
    }

    // In RedisService.php

    public function executeWithRetry(callable $operation, int $maxRetries = 3)
    {
        $retries = 0;
        $lastException = null;

        while ($retries < $maxRetries) {
            try {
                $result = $operation($this->getClient());

                // Handle empty responses
                if ($result === null || $result === false) {
                    return []; // Return empty array for hash commands
                }

                return $result;

            } catch (\Exception $e) {
                $lastException = $e;
                $retries++;
                Console::warn("Redis operation failed (attempt {$retries}): " . $e->getMessage());
                if ($retries < $maxRetries) {
                    usleep(100000 * $retries); // Exponential backoff
                    $this->initializeClient(); // Reconnect
                }
            }
        }

        throw new \RuntimeException(
            "Redis operation failed after {$maxRetries} attempts. " .
            "Last error: " . $lastException->getMessage()
        );
    }

    // In RedisService.php

    public function getStats(): array
    {
        $client = $this->getClient();

        return [
            'memory' => $client->info('memory'),
            'clients' => $client->info('clients'),
            'stats' => $client->info('stats'),
            'slowlog' => $client->slowlog('get', 10),
        ];
    }

    public function getQueueStats(): array
    {
        return $this->executeWithRetry(function ($client) {
            $iterator = null;
            $stats = [];
            $pattern = self::QUEUE_PREFIX . '*';

            do {
                $result = $client->scan($iterator, ['match' => $pattern, 'count' => 100]);
                $iterator = $result[0];
                $keys = $result[1];

                if (!empty($keys)) {
                    $pipe = $client->pipeline();
                    foreach ($keys as $key) {
                        $pipe->llen($key);
                        $pipe->ttl($key);
                    }
                    $results = $pipe->execute();

                    for ($i = 0; $i < count($keys); $i++) {
                        $stats[$keys[$i]] = [
                            'length' => $results[$i * 2],
                            'ttl' => $results[$i * 2 + 1]
                        ];
                    }
                }
            } while ($iterator > 0);

            return $stats;
        });
    }
}

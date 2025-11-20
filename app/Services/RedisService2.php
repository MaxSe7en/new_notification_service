<?php
namespace App\Services;

use Predis\Client;
use App\Exceptions\Console;
use Swoole\WebSocket\Server;

class RedisService2
{
    private Client $client;
    private array $config;
    private bool $isCluster;

    // Define all Redis key patterns as constants
    public const KEY_PREFIX = 'ws:';
    public const CONNECTION_PREFIX = self::KEY_PREFIX . 'connection:';
    public const QUEUE_PREFIX = self::KEY_PREFIX . 'notification_queue:';
    public const SERVER_REGISTRY = self::KEY_PREFIX . 'servers';
    public const USER_CONNECTION_MAP = self::KEY_PREFIX . 'user_connections';
    public const FD_USER_MAP = self::KEY_PREFIX . 'fd_user_map';

    public function __construct()
    {
        $this->config = [
            'cluster' => filter_var(getenv('REDIS_CLUSTER') ?: false, FILTER_VALIDATE_BOOLEAN),
            'scheme' => getenv('REDIS_SCHEME') ?: 'tcp',
            'host' => getenv('REDIS_HOST') ?: '127.0.0.1',
            'port' => (int) (getenv('REDIS_PORT') ?: 6379),
            'password' => getenv('REDIS_PASSWORD') ?: null,
            'read_write_timeout' => 0,
            'failover' => 'distribute',
        ];

        $this->initializeClient();
    }

    private function initializeClient(): void
    {
        try {
            if ($this->config['cluster']) {
                $this->client = new Client([
                    'cluster' => 'redis',
                    'parameters' => [
                        'password' => $this->config['password'],
                    ],
                    'nodes' => [
                        $this->config['scheme'] . '://' . $this->config['host'] . ':' . $this->config['port'],
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

            // Test connection with actual command that verifies connectivity
            $this->client->time();
        } catch (\Exception $e) {
            Console::error("Redis connection failed: " . $e->getMessage());
            throw $e;
        }
    }

    public function getClient(): Client
    {
        try {
            // Use a simple command that doesn't modify data to test connection
            $this->client->time();
        } catch (\Exception $e) {
            Console::warn("Redis connection lost, reconnecting... " . $e->getMessage());
            $this->initializeClient();
        }

        return $this->client;
    }

    public function safeHGetAll(string $key): array
    {
        return $this->executeWithRetry(function ($client) use ($key) {
            // First check if key exists
            if (!$client->exists($key)) {
                return [];
            }

            // Check the type
            $type = $client->type($key);
            if ($type !== 'hash') {
                $client->del($key);
                return [];
            }

            // Safely get hash data with error handling
            try {
                $data = $client->hgetall($key);
                if (!is_array($data)) {
                    return [];
                }
                return $data;
            } catch (\Exception $e) {
                Console::warn("HGETALL failed for key {$key}: " . $e->getMessage());
                return [];
            }
        });
    }

    public function safeHashGet(string $hashKey, string $field): ?string
    {
        return $this->executeWithRetry(function ($client) use ($hashKey, $field) {
            if ($client->type($hashKey) !== 'hash') {
                return null;
            }
            $value = $client->hget($hashKey, $field);
            return is_string($value) ? $value : null;
        });
    }

    public function executeWithRetry(callable $operation, int $maxRetries = 3)
    {
        $retries = 0;
        $lastException = null;

        while ($retries < $maxRetries) {
            try {
                $result = $operation($this->getClient());

                // Handle empty responses consistently
                if ($result === null || $result === false) {
                    return [];
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

    public function executeWithRetry2(callable $operation, int $maxRetries = 3)
    {
        $retries = 0;
        $lastException = null;

        while ($retries < $maxRetries) {
            try {
                return $operation($this->getClient()); // Just return the raw result
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

    // Add specific methods for our key patterns
    public function registerServer(string $serverId): void
    {
        $this->executeWithRetry2(function ($client) use ($serverId) {
            $client->hset(self::SERVER_REGISTRY, $serverId, time());
        });
    }

    public function trackConnection(int $userId, int $fd, int $ttl, $msg = ''): void
    {
        Console::info("Tracking connection: userId=$userId, fd=$fd");
        // Validate inputs first
        if ($fd <= 0 || $userId <= 0) {
            throw new \InvalidArgumentException("Invalid connection parameters");
        }

        $this->executeWithRetry2(function ($client) use ($userId, $fd, $ttl) {
            $pipe = $client->pipeline();

            // Use consistent typing for Redis keys
            $pipe->hset(self::USER_CONNECTION_MAP, (string) $userId, (string) $fd);
            $pipe->hset(self::FD_USER_MAP, (string) $fd, (string) $userId);
            $pipe->setex(
                self::CONNECTION_PREFIX . $fd,
                $ttl,
                (string) $userId
            );

            $pipe->execute();
        });
    }

    public function getConnectionUserId(int $fd): ?int
    {
        return $this->executeWithRetry2(function ($client) use ($fd) {
            try {
                $key = self::CONNECTION_PREFIX . $fd;
                $result = $client->get($key);
                Console::info("Getting user ID for fd {$fd} from Redis key {$key}");
                // Handle null case first
                if ($result === null) {
                    return 0;
                }

                // Convert to string if object has __toString()
                if (is_object($result) && method_exists($result, '__toString')) {
                    $result = (string) $result;
                }

                // Handle string case
                if (is_string($result)) {
                    // Empty string should be treated as null
                    if (trim($result) === '') {
                        return 0;
                    }

                    // Return numeric strings as int
                    if (is_numeric($result)) {
                        return (int) $result;
                    }

                    // Non-numeric string - log warning
                    Console::warn("Non-numeric user ID in Redis for fd {$fd}: {$result}");
                    return 0;
                }

                // Handle integer case directly
                if (is_int($result)) {
                    return $result;
                }

                // Handle arrays (shouldn't happen with GET, but just in case)
                if (is_array($result)) {
                    Console::warn("Unexpected array response from Redis for key {$key} result " . json_encode($result));
                    return 0;
                }

                // Handle boolean case (unlikely but possible)
                if (is_bool($result)) {
                    return $result ? 1 : 0; // Convert to 1/0 if needed
                }

                // Final fallback for any other type
                Console::warn("Unhandled response type for fd {$fd}: " . gettype($result));
                return 0;

            } catch (\Exception $e) {
                Console::error("Failed to get user ID for fd {$fd}: " . $e->getMessage());
                return 0;
            }
        });
    }

    public function getConnectionFdByUserId(int $userId): ?int
    {
        return $this->executeWithRetry2(function ($client) use ($userId) {
            $fd = $client->hget(self::USER_CONNECTION_MAP, (string) $userId);
            return is_numeric($fd) ? (int) $fd : null;
        });
    }

    public function getConnectionUserIdByFd(Server $server, int $fd): ?int
    {
        return $this->executeWithRetry2(function ($client) use ($server, $fd) {
            $swooleKnows = $server->exists($fd);
            $userId = $client->hget(self::USER_CONNECTION_MAP, (string) $fd);
            Console::info("FD {$fd} exists in swoole: " . ($swooleKnows ? 'yes' : 'no') . ", userId in redis: " . var_export($userId, true));
            return is_numeric($userId) ? (int) $userId : null;
        });
    }

    public function resolveUserIdFromFd(Server $server, int $fd): ?int
    {
        return $this->executeWithRetry2(function ($client) use ($server, $fd) {
            // Make sure Swoole knows the connection exists
            $swooleKnows = $server->exists($fd);
            $redisUserId = $client->hget(self::USER_CONNECTION_MAP, (string) $fd);

            Console::info("FD {$fd} exists in swoole: " . ($swooleKnows ? 'yes' : 'no') . ", userId in redis: " . var_export($redisUserId, true));

            if (!$server->exists($fd)) {
                Console::warn("FD {$fd} found in Redis but not in Swoole — cleaning up...");
                return null;
            }

            // Get userId from Redis
            $userId = $client->hget(self::FD_USER_MAP, (string) $fd);

            // Must be numeric and not empty
            return is_numeric($userId) ? (int) $userId : null;
        });
    }

}
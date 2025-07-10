<?php
// File: /app/Config/DatabaseAccessors.php
namespace App\Config;

use PDO;
use PDOException;
use App\Exceptions\Console;
use App\Config\DatabasePool;
class DatabaseAccessors
{
    private static ?DatabasePool $pool = null;
    private static array $readConfig = [
        'host' => '192.168.1.51',//localhost // Set your read replica host
        // 'host' => 'localhost',//'192.168.1.51',//localhost // Set your read replica host
        'database' => 'lottery_test',
        'username' => 'enzerhub',
        'password' => 'enzerhub'
    ];

    private static array $writeConfig = [
        // 'host' => 'localhost',//'192.168.1.51', // Primary write host
        'host' => '192.168.1.51', // Primary write host
        'database' => 'lottery_test',
        'username' => 'enzerhub',
        'password' => 'enzerhub'
    ];

    private static function initializePool(): void
    {
        if (self::$pool === null) {
            $readPoolSize = getenv('DB_READ_POOL_SIZE') ?: 15;
            $writePoolSize = getenv('DB_WRITE_POOL_SIZE') ?: 5;
            self::$pool = new DatabasePool(
                self::$readConfig,
                self::$writeConfig,
                (int) $readPoolSize,
                (int) $writePoolSize
            );
        }
    }

    public static function connect(bool $forWrite = false): PDO
    {
        self::initializePool();

        try {
            $connection = $forWrite
                ? self::$pool->getWriteConnection()
                : self::$pool->getReadConnection();

            // Verify connection is still alive
            $connection->query('SELECT 1')->fetch();

            return $connection;
        } catch (PDOException $e) {
            Console::error("😱😱😱 Database connection failed: " . $e->getMessage());

            // Fallback to write connection if read fails
            if (!$forWrite) {
                try {
                    Console::warn("Falling back to write connection for read operation");
                    return self::connect(true);
                } catch (PDOException $e) {
                    throw $e; // Re-throw if both fail
                }
            }

            throw $e;
        }
    }

    public static function release(PDO $connection, bool $isWriteConnection = false): void
    {
        if (self::$pool !== null) {
            try {
                // Check if connection is still valid before returning to pool
                $connection->query('SELECT 1')->fetch();

                if ($isWriteConnection) {
                    self::$pool->releaseWriteConnection($connection);
                } else {
                    self::$pool->releaseReadConnection($connection);
                }
            } catch (PDOException $e) {
                // Connection is bad, don't return it to the pool
                Console::error("😱😱😱 Releasing bad connection: " . $e->getMessage());
                $connection = null;
            }
        }
    }

    public static function select(string $query, array $params = []): ?array
    {
        $connection = null;
        try {
            $connection = self::connect(false); // Use read connection
            $stmt = $connection->prepare($query);
            $stmt->execute($params);
            $result = $stmt->fetch(PDO::FETCH_ASSOC) ?: null;
            return $result;
        } catch (PDOException $e) {
            Console::log2("😱😱😱 Select Error: ", $e->getMessage());
            return null;
        } finally {
            if ($connection !== null) {
                self::release($connection, false);
            }
        }
    }

    public static function selectAll(string $query, array $params = []): array
    {
        $connection = null;
        try {
            $connection = self::connect(false); // Use read connection
            $stmt = $connection->prepare($query);
            $stmt->execute($params);
            return $stmt->fetchAll(PDO::FETCH_ASSOC);
        } catch (PDOException $e) {
            Console::log2("😱😱😱 SelectAll Error: ", $e->getMessage());
            return [];
        } finally {
            if ($connection !== null) {
                self::release($connection, false);
            }
        }
    }

    public static function insert(string $query, array $params = []): bool
    {
        $connection = null;
        try {
            $connection = self::connect(true); // Use write connection
            $stmt = $connection->prepare($query);
            return $stmt->execute($params);
        } catch (PDOException $e) {
            Console::log2("😱😱😱 Insert Error: ", $e->getMessage());
            return false;
        } finally {
            if ($connection !== null) {
                self::release($connection, true);
            }
        }
    }

    public static function update(string $query, array $params = []): bool
    {
        $connection = null;
        try {
            $connection = self::connect(true); // Use write connection
            $stmt = $connection->prepare($query);
            return $stmt->execute($params);
        } catch (PDOException $e) {
            Console::error("😱😱😱 Update Error: " . $e->getMessage());
            return false;
        } finally {
            if ($connection !== null) {
                self::release($connection, true);
            }
        }
    }

    public static function delete(string $query, array $params = []): bool
    {
        $connection = null;
        try {
            $connection = self::connect(true); // Use write connection
            $stmt = $connection->prepare($query);
            return $stmt->execute($params);
        } catch (PDOException $e) {
            Console::log2("😱😱😱 Delete Error: ", $e->getMessage());
            return false;
        } finally {
            if ($connection !== null) {
                self::release($connection, true);
            }
        }
    }

    public static function getPoolStats(): array
    {
        self::initializePool();
        return self::$pool->getStats();
    }

    public static function executeTransaction(callable $transactionCallback)
    {
        $connection = null;
        try {
            $connection = self::connect(true); // Write connection for transactions
            $connection->beginTransaction();

            $result = $transactionCallback($connection);

            $connection->commit();
            return $result;
        } catch (PDOException $e) {
            if ($connection !== null && $connection->inTransaction()) {
                $connection->rollBack();
            }
            Console::error("😱😱😱 Transaction failed: " . $e->getMessage());
            throw $e;
        } finally {
            if ($connection !== null) {
                self::release($connection, true);
            }
        }
    }
}

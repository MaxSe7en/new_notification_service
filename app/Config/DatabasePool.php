<?php
namespace App\Config;

use App\Exceptions\Console;
use PDO;
use PDOException;
use SplQueue;

class DatabasePool
{
    private SplQueue $readPool;
    private SplQueue $writePool;
    private array $readConfig;
    private array $writeConfig;
    private int $readSize;
    private int $writeSize;
    private int $currentReadConnections = 0;
    private int $currentWriteConnections = 0;

    public function __construct(array $readConfig, array $writeConfig, int $readSize = 10, int $writeSize = 5)
    {
        $this->readPool = new SplQueue();
        $this->writePool = new SplQueue();
        $this->readConfig = $readConfig;
        $this->writeConfig = $writeConfig;
        $this->readSize = $readSize;
        $this->writeSize = $writeSize;
    }

    public function getReadConnection(): PDO
    {
        if (!$this->readPool->isEmpty()) {
            return $this->readPool->dequeue();
        }

        if ($this->currentReadConnections < $this->readSize) {
            $this->currentReadConnections++;
            return $this->createNewConnection($this->readConfig);
        }

        throw new PDOException("Read database connection pool exhausted");
    }

    public function getWriteConnection(): PDO
    {
        if (!$this->writePool->isEmpty()) {
            return $this->writePool->dequeue();
        }

        if ($this->currentWriteConnections < $this->writeSize) {
            $this->currentWriteConnections++;
            return $this->createNewConnection($this->writeConfig);
        }

        throw new PDOException("Write database connection pool exhausted");
    }

    public function releaseReadConnection(PDO $connection): void
    {
        $this->readPool->enqueue($connection);
    }

    public function releaseWriteConnection(PDO $connection): void
    {
        $this->writePool->enqueue($connection);
    }

    private function createNewConnection(array $config): PDO
    {
        try {
            $dsn = "mysql:host={$config['host']};dbname={$config['database']};charset=utf8mb4";
                // Console::log2('============================> ', $config);
            $defaultOptions = [
                PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
                PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
                PDO::ATTR_EMULATE_PREPARES => false,
                PDO::ATTR_PERSISTENT => false,
                PDO::MYSQL_ATTR_INIT_COMMAND => 'SET NAMES utf8mb4',
                PDO::ATTR_TIMEOUT => 5, // 5 second timeout
            ];

            return new PDO(
                $dsn,
                $config['username'],
                $config['password'],
                $defaultOptions
            );
        } catch (PDOException $e) {
            throw new PDOException("Connection failed: " . $e->getMessage());
        }
    }

    public function getStats(): array
    {
        return [
            'read' => [
                'available' => $this->readPool->count(),
                'in_use' => $this->currentReadConnections - $this->readPool->count(),
                'max_size' => $this->readSize
            ],
            'write' => [
                'available' => $this->writePool->count(),
                'in_use' => $this->currentWriteConnections - $this->writePool->count(),
                'max_size' => $this->writeSize
            ]
        ];
    }
}

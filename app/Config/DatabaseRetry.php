<?php
namespace App\Config;

use PDO;
use PDOException;
use App\Exceptions\Console;

class DatabaseRetry
{
    public static function executeWithRetry(callable $operation, int $maxRetries = 3, bool $isWrite = false)
    {
        $retry = 0;
        $lastException = null;

        while ($retry < $maxRetries) {
            try {
                return $operation();
            } catch (PDOException $e) {
                $lastException = $e;
                $retry++;

                // Log only if it's not the last retry
                if ($retry < $maxRetries) {
                    Console::warn("Database operation failed (attempt $retry/$maxRetries): " . $e->getMessage());
                }

                // Exponential backoff: 100ms, 200ms, 400ms, etc.
                usleep(100000 * (2 ** ($retry - 1)));
            }
        }

        Console::error("Database operation failed after $maxRetries attempts: " . $lastException->getMessage());
        throw $lastException;
    }
}
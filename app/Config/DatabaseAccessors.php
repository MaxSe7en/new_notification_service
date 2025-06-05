<?php
namespace App\Config;

use App\Config\Database;
use PDO;
use PDOException;
use App\Exceptions\Console;

class DatabaseAccessors
{
    private static ?PDO $db = null;
    private static function reconnect(): void
    {
        try {
            $database = new Database();
            // Connect without persistent attribute
            self::$db = $database->connect();

            // Configure connection for long-running processes
            self::$db->setAttribute(PDO::ATTR_TIMEOUT, 30); // Connection timeout
            self::$db->setAttribute(PDO::MYSQL_ATTR_USE_BUFFERED_QUERY, true); // Buffered queries
            // ATTR_ERRMODE and ATTR_DEFAULT_FETCH_MODE are now handled by the Database class defaults
        } catch (PDOException $e) {
            Console::error("😱😱😱 Database connection failed: " . $e->getMessage());
            throw $e;
        }
    }

    public static function connect(): PDO
    {
        if (self::$db === null) {
            self::reconnect();
        }

        try {
            // Verify connection is still alive
            self::$db->query('SELECT 1')->fetch();
        } catch (PDOException $e) {
            // If connection is lost, explicitly null it and then reconnect
            self::$db = null;
            self::reconnect();
        }

        return self::$db;
    }

    public static function select(string $query, array $params = []): ?array
    {
        try {
            $stmt = self::connect()->prepare($query);
            $stmt->execute($params);
            return $stmt->fetch(PDO::FETCH_ASSOC) ?: null;
        } catch (PDOException $e) {
            Console::log2("😱😱😱 Select Error: ", $e->getMessage());
            return null;
        }

    }


    public static function selectAll(string $query, array $params = []): array
    {
        try {
            $stmt = self::connect()->prepare($query);
            $stmt->execute($params);
            return $stmt->fetchAll(PDO::FETCH_ASSOC);
        } catch (PDOException $e) {
            Console::log2("😱😱😱 SelectAll Error: ", $e->getMessage());
            return [];
        }
    }

    public static function insert(string $query, array $params = []): bool
    {
        try {
            $stmt = self::connect()->prepare($query);
            return $stmt->execute($params);
        } catch (PDOException $e) {
            Console::log2("😱😱😱 Insert Error: ", $e->getMessage());
            return false;
        }
    }

    public static function update(string $query, array $params = []): bool
    {
        try {
            $stmt = self::connect()->prepare($query);
            return $stmt->execute($params);
        } catch (PDOException $e) {
            echo '😱😱😱 Update Error' . $e->getMessage();
            Console::error("😱😱😱 Update Error: " . $e->getMessage());
            return false;
        }
    }

    public static function delete(string $query, array $params = []): bool
    {
        try {
            $stmt = self::connect()->prepare($query);
            return $stmt->execute($params);
        } catch (PDOException $e) {
            Console::log2("😱😱😱 Delete Error: ", $e->getMessage());
            return false;
        }
    }
}
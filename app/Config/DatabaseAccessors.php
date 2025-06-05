<?php
namespace App\Config;

use App\Config\Database;
use PDO;
use PDOException;
use App\Exceptions\Console;

class DatabaseAccessors{
    private static ?PDO $db = null;

    public static function connect(): PDO {
        if (self::$db === null) { // ✅ Only initialize if not already connected
            $database = new Database();
            self::$db = $database->connect();
        }
        return self::$db;
    }

    public static function select(string $query, array $params = []): ?array {
        try {
            $stmt = self::connect()->prepare($query);
            $stmt->execute($params);
            return $stmt->fetch(PDO::FETCH_ASSOC) ?: null;
        } catch (PDOException $e) {
            Console::log2("Select Error: ", $e->getMessage());
            return null;
        }            

    }


    public static function selectAll(string $query, array $params = []): array {
        try {
            $stmt = self::connect()->prepare($query);
            $stmt->execute($params);
            return $stmt->fetchAll(PDO::FETCH_ASSOC);
        } catch (PDOException $e) {
            Console::log2("SelectAll Error: ", $e->getMessage());
            return [];
        }
    }

    public static function insert(string $query, array $params = []): bool {
        try {
            $stmt = self::connect()->prepare($query);
            return $stmt->execute($params);
        } catch (PDOException $e) {
            Console::log2("Insert Error: ", $e->getMessage());
            return false;
        }
    }

    public static function update(string $query, array $params = []): bool {
        try {
            $stmt = self::connect()->prepare($query);
            return $stmt->execute($params);
        } catch (PDOException $e) {
            echo 'Update Error'. $e->getMessage();
            return false;
        }
    }

    public static function delete(string $query, array $params = []): bool {
        try {
            $stmt = self::connect()->prepare($query);
            return $stmt->execute($params);
        } catch (PDOException $e) {
            Console::log2("Delete Error: ", $e->getMessage());
            return false;
        }
    }
}
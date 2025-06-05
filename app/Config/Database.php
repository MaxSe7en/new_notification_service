<?php
namespace App\Config;

use PDO;
use PDOException;

class Database
{
    /**
     * Connect to the database.
     *
     * @param array $options Optional PDO attributes.
     * @return PDO
     * @throws PDOException
     */
    public function connect(array $options = []): PDO
    {
        $dsn = "mysql:host=localhost;dbname=lottery_test";
        $username = "enzerhub";
        $password = "enzerhub";

        // Define default options, excluding ATTR_PERSISTENT
        $defaultOptions = [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
            PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
            PDO::MYSQL_ATTR_INIT_COMMAND => 'SET NAMES utf8mb4', // Recommended for proper UTF-8 handling
        ];

        // Merge default options with user-provided ones
        $pdoOptions = $options + $defaultOptions;

        return new PDO($dsn, $username, $password, $pdoOptions);
    }
}
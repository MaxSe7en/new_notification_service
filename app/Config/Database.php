<?php
namespace App\Config;

use PDO;
use PDOException;

class Database
{
    /**
     * Connect to the database (now just a wrapper for the pool)
     * @param bool $forWrite Whether to get a write connection
     */
    public function connect(bool $forWrite = false, array $options = []): PDO
    {
        return DatabaseAccessors::connect($forWrite);
    }
}

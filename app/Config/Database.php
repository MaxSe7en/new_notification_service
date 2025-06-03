<?php

namespace App\Config;

use PDO;

class Database
{
    public static function connect()
    {
        return new PDO("mysql:host=localhost;dbname=lottery_test", "enzerhub", "enzerhub");
        // return new PDO("mysql:host=192.168.1.51;dbname=lottery_test", "enzerhub", "enzerhub");
    }
}

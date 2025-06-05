<?php

use Swoole\Timer;
use Swoole\Process;
use Predis\Client;

use App\Models\NotificationModel;
use App\Config\DatabaseAccessors;
use App\Services\NotificationService;
use App\Controllers\NotificationController;

require 'vendor/autoload.php';


$redisCache = new Client();

echo "Starting Notification Worker...\n";

print_r((new NotificationService())->getNotificationStats());
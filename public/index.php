<?php
# Entry point (includes autoload & routes)

require __DIR__ . '/../vendor/autoload.php';

use App\Controllers\NotificationController;

$controller = new NotificationController();
$controller->send(1, 'email', 'Hello, this is a test notification.');

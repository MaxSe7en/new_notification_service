<?php
require_once 'bootstrap.php';

use App\Services\RedisService2;
use App\Exceptions\Console;

Console::info("Testing Redis connection tracking fixes...");

$redisService = new RedisService2();

// Test 1: Clean up any test keys
Console::info("Test 1: Cleaning up test keys");
$redisService->cleanupTestKeys();

// Test 2: Track a connection
Console::info("Test 2: Tracking connection");
$userId = 123;
$fd = 456;
$ttl = 300;

try {
    $redisService->trackConnection($userId, $fd, $ttl);
    Console::info("✓ Connection tracking successful");
} catch (Exception $e) {
    Console::error("✗ Connection tracking failed: " . $e->getMessage());
}

// Test 3: Retrieve user ID by fd using the simpler method
Console::info("Test 3: Retrieving user ID by fd");
try {
    $retrievedUserId = $redisService->getConnectionUserId($fd);
    Console::info("Retrieved user ID: " . var_export($retrievedUserId, true));
    if ($retrievedUserId === $userId) {
        Console::info("✓ User ID retrieval successful");
    } else {
        Console::warn("✗ User ID mismatch: expected {$userId}, got {$retrievedUserId}");
    }
} catch (Exception $e) {
    Console::error("✗ User ID retrieval failed: " . $e->getMessage());
}

// Test 4: Retrieve fd by user ID
Console::info("Test 4: Retrieving fd by user ID");
try {
    $retrievedFd = $redisService->getConnectionFdByUserId($userId);
    Console::info("Retrieved fd: " . var_export($retrievedFd, true));
    if ($retrievedFd === $fd) {
        Console::info("✓ FD retrieval successful");
    } else {
        Console::warn("✗ FD mismatch: expected {$fd}, got {$retrievedFd}");
    }
} catch (Exception $e) {
    Console::error("✗ FD retrieval failed: " . $e->getMessage());
}

Console::info("Test completed!");

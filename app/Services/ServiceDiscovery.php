<?php
// File: /app/Services/ServiceDiscovery.php
namespace App\Services;

use App\Exceptions\Console;

class ServiceDiscovery
{
    private RedisService $redisService;

    public function __construct(RedisService $redisService)
    {
        $this->redisService = $redisService;
    }

    public function getAvailableServers(): array
    {
        return $this->redisService->executeWithRetry(function($client) {
            $servers = $client->hgetall('ws_servers');
            $active = [];
            $now = time();
            
            foreach ($servers as $address => $lastSeen) {
                if ($now - $lastSeen < 60) { // 60 second heartbeat
                    $active[] = $address;
                } else {
                    $client->hdel('ws_servers', $address);
                }
            }
            
            return $active;
        });
    }

    public function registerServer(string $address): void
    {
        $this->redisService->executeWithRetry(function($client) use ($address) {
            $client->hset('ws_servers', $address, time());
        });
    }
}

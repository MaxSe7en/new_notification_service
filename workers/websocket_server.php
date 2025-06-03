<?php

use Swoole\WebSocket\Server;
use Swoole\Table;
use Swoole\Server as TcpServer;

use App\Config\DatabaseAccessors;

$server = new Server("0.0.0.0", 9502);
// TEMPORARY CONFIG WILL. IMPLEMENTATION WILL BE REDIS
// Create a Swoole Table to store user ID -> FD mappings
$userTable = new Table(1024);
$userTable->column('fd', Table::TYPE_INT);
$userTable->create();

$server->on("open", function ($server, $request) use ($userTable) {
    $queryString = $request->server['query_string'] ?? '';
    parse_str($queryString, $query);

    echo "Full Request URI: {$request->server['request_uri']}\n";
    echo "Query String: $queryString\n";

    if (isset($query['userId'])) {
        $userId = (int) $query['userId'];
        $userTable->set($userId, ['fd' => $request->fd]);
        echo "User $userId connected with FD {$request->fd}\n";
    }
});

$server->on("message", function ($server, $frame) use ($userTable) {
    echo "Received message from client: {$frame->data}\n";
    $data = json_decode($frame->data, true);
    // print_r($data);
    if ($data && isset($data['action']) && $data['action'] === 'send_notification') {
        $userId = (int) $data['user_id'];
        $message = "Socket new man"; //$data['message'];
        sendNotificationToUser($server, $userId, $message, $userTable, "normal");
    }
});

$server->on("receive", function ($server, $fd, $reactor_id, $data) use ($userTable) {
    $notification = json_decode($data, true);
    echo "TCP Listener received data: " . $data . "\n";
    echo "=============================================\n";
    if ($notification && isset($notification['action']) && $notification['action'] === 'send_notification') {
        $userId = (int) $notification['user_id'];
        $message = $notification['message'];
        $event = $notification['event'];
        sendNotificationToUser($server, $userId, $message, $userTable, $event);
    }
});

$server->on("close", function ($server, $fd) use ($userTable) {
    foreach ($userTable as $userId => $row) {
        if ($row['fd'] === $fd) {
            $userTable->del($userId);
            echo "User $userId disconnected\n";
            break;
        }
    }
});

function sendNotificationToUser22($server, $userId, $message, $userTable)
{
    if ($userTable->exists($userId)) {
        $fd = $userTable->get($userId, 'fd');
        $server->push($fd, json_encode(["user_id" => $userId, "message" => $message]));
        echo "Sent notification to User $userId\n";
    } else {
        echo "User $userId not connected\n";
    }
}
function sendNotificationToUser($server, $userId, $message, $userTable, $event)
{
    if ($userTable->exists($userId)) {
        $fd = $userTable->get($userId, 'fd');
        $result = $server->push($fd, json_encode(["user_id" => $userId, "message" => $message, "event" => $event]));
        echo "Sent notification to User $userId\n";
        return $result;
    } else {
        echo "User $userId not connected\n";
        return false;
    }
}

$port = $server->addlistener("127.0.0.1", 9503, SWOOLE_SOCK_TCP);
echo "TCP Listener started on 127.0.0.1:9503\n";
$port->set([
    // 'open_length_check' => true,
    // 'package_length_type' => 'N', // Unsigned long (32-bit, big-endian)
    // 'package_length_offset' => 0,
    // 'package_body_offset' => 4,
    // // 'package_max_length' => 1024 * 1024, // 1MB
    'open_eof_check' => true,
    'package_eof' => "\n",
]);

$port->on('connect', function ($serv, $fd, $from_id) {
    echo 'connected callback' . PHP_EOL;
});

$port->on("receive", function ($port, $fd, $reactorId, $data) use ($server, $userTable) {
    try {
        $notification = json_decode(trim($data), true);

        if (!$notification) {
            echo "ERROR: Failed to parse JSON: " . json_last_error_msg() . "\n";
            return;
        }

        if (isset($notification['action']) && $notification['action'] === 'send_notification') {
            $userId = (int) $notification['user_id'];
            $message = $notification['message'];
            $event = $notification['event'];

            echo "TCP Listener: Forwarding notification for user $userId\n";

            if ($userTable->exists($userId)) {
                $clientFd = $userTable->get($userId, 'fd');
                $server->push($clientFd, json_encode([
                    "type" => "notification",
                    "user_id" => $userId,
                    "message" => $message,
                    "event" => $event
                ]));
                echo "Sent notification to User $userId (FD: $clientFd)\n";
            } else {
                echo "User $userId not connected to WebSocket\n";
            }
        }
    } catch (\Exception $e) {
        echo "ERROR in TCP listener: " . $e->getMessage() . "\n";
    }
});

$GLOBALS['userTable'] = $userTable;

$server->start();

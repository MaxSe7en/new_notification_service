<?php
// Composer is the recommended way to install Monolog
// Run in terminal: composer require monolog/monolog

use Monolog\Logger;
use Monolog\Handler\StreamHandler;
use Monolog\Handler\FirePHPHandler;

class LoggerService {
    private static $instance = null;
    private $logger;

    private function __construct() {
        // Create the logger
        $this->logger = new Logger('app_logger');
        
        // Log directory setup
        $logDirectory = __DIR__ . '/../logs/';
        
        // Create logs directory if it doesn't exist
        if (!file_exists($logDirectory)) {
            mkdir($logDirectory, 0777, true);
        }

        // Add handlers
        $this->logger->pushHandler(
            new StreamHandler(
                $logDirectory . 'app_' . date('Y-m-d') . '.log', 
                Logger::DEBUG
            )
        );

        // Optional: Add FirePHP handler for browser debugging
        $this->logger->pushHandler(new FirePHPHandler());
    }

    public static function getInstance() {
        if (self::$instance === null) {
            self::$instance = new self();
        }
        return self::$instance;
    }

    public function info($message, array $context = []) {
        $this->logger->info($message, $context);
    }

    public function error($message, array $context = []) {
        $this->logger->error($message, $context);
    }

    public function debug($message, array $context = []) {
        $this->logger->debug($message, $context);
    }
}

// // Usage example
// try {
//     $logger = LoggerService::getInstance();
//     $logger->info('Application started');
    
//     // In your database class
//     $logger->error('Database connection failed', [
//         'error' => $e->getMessage(),
//         'user_id' => $userId
//     ]);
// } catch (Exception $e) {
//     // Fallback logging
//     error_log('Logger initialization failed: ' . $e->getMessage());
// }
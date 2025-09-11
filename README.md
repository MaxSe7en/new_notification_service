# Notification Service

A high-performance PHP service for managing and delivering real-time notifications across multiple channels.

### 🔔 Features

* **Multi-channel delivery** – supports WebSocket, Email, SMS, and Push notifications.
* **Real-time updates** – built with Swoole and Redis to push messages instantly to connected users.
* **Queue & Retry** – offline users’ messages are queued in Redis for later delivery.
* **Bulk messaging** – efficiently send to thousands of users with batching and limits.
* **Notification management** – create, read, mark as read, and clean up old notifications.
* **Logging & Monitoring** – Monolog integration for structured logs.

### 🛠️ Tech Stack

* **PHP 8+** with Composer autoloading (PSR-4)
* **Swoole** for high-performance WebSocket server
* **Redis (Predis client)** for queues and pub/sub
* **MySQL (PDO)** for persistence
* **Monolog** for logging

### 📂 Structure

* `app/` – Controllers, Models, Services, Config, Exceptions
* `workers/` – WebSocket and background workers (notification processing, queues)
* `public/` – Application entry point

### 🚀 Use Cases

* In-app notification systems
* Real-time dashboards
* Messaging/alerting features in SaaS products
* Event-driven systems requiring fast, reliable delivery

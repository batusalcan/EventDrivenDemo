# Event-Driven Microservices Architecture — PoC

A university assignment for the **Large-Scale Systems** course. This project is a Proof of Concept that demonstrates an **Event-Driven Architecture** using multiple cloud message brokers, with a strong focus on solving **Vendor Lock-in** through software architecture and design patterns.

---

## Table of Contents

1. [Project Goal](#1-project-goal)
2. [Architecture Overview](#2-architecture-overview)
3. [Design Patterns](#3-design-patterns)
4. [Tech Stack](#4-tech-stack)
5. [Project Structure](#5-project-structure)
6. [Services & Ports](#6-services--ports)
7. [Message Brokers](#7-message-brokers)
8. [Core Domain Flow](#8-core-domain-flow)
9. [Key Demo Scenarios](#9-key-demo-scenarios)
10. [API Reference](#10-api-reference)
11. [Running the Project](#11-running-the-project)
12. [Frontend](#12-frontend)

---

## 1. Project Goal

Modern distributed systems face a critical challenge: once you build against a specific message broker (Kafka, AWS SQS, Google Pub/Sub), switching to another one requires rewriting large portions of your codebase. This is called **Vendor Lock-in**.

This PoC demonstrates that with the right abstractions, a system can:

- Publish and consume messages without knowing which broker is active
- Switch between Kafka, AWS SNS/SQS, and Google Cloud Pub/Sub **at runtime** via a single API call or UI click
- Add a new broker in the future by implementing one interface — no business logic changes required

---

## 2. Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                        React Frontend                           │
│         BrokerSelector │ OrderForm │ SignalR Event Monitor       │
└──────────────┬──────────────────────────────┬───────────────────┘
               │ HTTP REST                    │ WebSocket (SignalR)
               ▼                              ▼
┌──────────────────────────┐    ┌─────────────────────────────────┐
│       Order API          │    │   SignalR EventHub (all 3 APIs) │
│  (Publisher / Producer)  │    └─────────────────────────────────┘
│                          │
│  OrdersController        │
│  SystemController        │──── BrokerSwitcher (Strategy Pattern)
│  EventLogController      │         │
│                          │    ┌────┴─────────────────┐
└──────────────────────────┘    │                     │
                                ▼                     ▼
                         KafkaPublisher        AwsPublisher / GcpPublisher
                                │
                         ┌──────┴──────────────────────────┐
                         │       Message Broker Layer       │
                         │  Kafka │ AWS SNS+SQS │ GCP Pub/Sub │
                         └──────┬──────────────────────────┘
                                │ Fan-out
                    ┌───────────┴───────────┐
                    ▼                       ▼
        ┌──────────────────┐    ┌────────────────────────┐
        │   Invoice API    │    │    Notification API    │
        │  (Subscriber)    │    │      (Subscriber)      │
        │                  │    │                        │
        │  Generates       │    │  Sends Email / SMS     │
        │  invoice number  │    │  (VIP gets both)       │
        └──────────────────┘    └────────────────────────┘
```

One `OrderCreatedEvent` published by Order API is consumed by **both** Invoice API and Notification API simultaneously. This is the **Fan-out** pattern.

---

## 3. Design Patterns

### Strategy Pattern — `BrokerSwitcher`

The `BrokerSwitcher` class in Order API is the central implementation of the Strategy Pattern. It holds a reference to the currently active `IMessagePublisher` and can swap it at runtime:

```
IMessagePublisher (interface — defined in Shared)
    │
    ├── KafkaPublisher      ← active by default
    ├── AwsSnsPublisher     ← swapped in when broker = AWS
    └── GcpPubSubPublisher  ← swapped in when broker = GCP
```

The `OrdersController` only knows about `IMessagePublisher`. It never imports Kafka, AWS, or GCP packages directly. This is the abstraction that prevents vendor lock-in.

### Adapter Pattern

Each broker implementation (`KafkaPublisher`, `AwsSnsPublisher`, etc.) acts as an **Adapter** — it translates the generic `PublishAsync(topic, message, headers)` call into the specific API of that broker. The caller never changes; only the adapter does.

### Observer Pattern — SignalR

Every consumer in every API broadcasts processed events to all connected browser clients in real time via `IHubContext<EventHub>`. The browser observes the system without polling.

---

## 4. Tech Stack

| Layer | Technology |
|---|---|
| Backend framework | C# / .NET 10 (ASP.NET Core Web API) |
| Message brokers | Apache Kafka, AWS SNS + SQS, GCP Pub/Sub |
| Real-time streaming | ASP.NET Core SignalR (WebSockets) |
| Frontend | React 19 + Vite |
| Containerization | Docker + Docker Compose |
| Kafka client | Confluent.Kafka |

---

## 5. Project Structure

```
EventDrivenDemo/
│
├── EventDrivenDemo.Shared/          # Shared contracts — no broker dependencies
│   ├── Interfaces/
│   │   ├── IMessagePublisher.cs     # Core abstraction for publishing
│   │   └── IMessageConsumer.cs     # Core abstraction for consuming
│   ├── Models/
│   │   ├── OrderCreatedEvent.cs    # The domain event passed between services
│   │   └── MessageHeaders.cs       # Metadata dictionary (e.g. CustomerTier)
│   └── Enums/
│       └── BrokerType.cs           # Kafka | Aws | Gcp
│
├── EventDrivenDemo.Api/             # Microservice 1: Order API (Publisher)
│   ├── Controllers/
│   │   ├── OrdersController.cs     # POST /api/orders — places an order
│   │   ├── SystemController.cs     # GET/POST /api/system — broker switching
│   │   └── EventLogController.cs   # GET /api/events/logs — in-memory log
│   ├── Messaging/
│   │   ├── Kafka/
│   │   │   ├── KafkaPublisher.cs   # Produces messages to Kafka topic
│   │   │   └── KafkaConsumer.cs    # Consumes own echo (for SignalR logging)
│   │   └── Stubs/
│   │       ├── AwsPublisherStub.cs # Placeholder — logs instead of publishing
│   │       └── GcpPublisherStub.cs # Placeholder — logs instead of publishing
│   ├── Services/
│   │   ├── BrokerSwitcher.cs       # Strategy context — hot-swaps publishers
│   │   └── EventLogStore.cs        # In-memory list of processed events
│   ├── Hubs/
│   │   └── EventHub.cs             # SignalR hub endpoint
│   └── Program.cs
│
├── EventDrivenDemo.InvoiceApi/      # Microservice 2: Invoice API (Subscriber)
│   ├── Controllers/
│   │   └── InvoiceController.cs    # GET /api/invoices/logs
│   ├── Messaging/
│   │   └── InvoiceKafkaConsumer.cs # BackgroundService — polling loop
│   ├── Services/
│   │   └── InvoiceEventLogStore.cs
│   ├── Hubs/
│   │   └── EventHub.cs
│   └── Program.cs
│
├── EventDrivenDemo.NotificationApi/ # Microservice 3: Notification API (Subscriber)
│   ├── Controllers/
│   │   └── NotificationController.cs
│   ├── Messaging/
│   │   └── NotificationKafkaConsumer.cs
│   ├── Services/
│   │   └── NotificationEventLogStore.cs
│   ├── Hubs/
│   │   └── EventHub.cs
│   └── Program.cs
│
├── frontend/                        # React SPA
│   ├── src/
│   │   ├── components/
│   │   │   ├── BrokerSelector.jsx  # Cards to switch active broker
│   │   │   ├── OrderForm.jsx       # Form to place an order (VIP flag)
│   │   │   └── EventMonitor.jsx    # Dark console — real-time SignalR log
│   │   └── services/
│   │       ├── api.js              # REST calls to Order API
│   │       └── signalr.js          # SignalR connection manager (3 APIs)
│   ├── Dockerfile
│   └── nginx.conf
│
├── docker-compose.yml               # Brings up all 7 services with one command
├── .dockerignore
└── README.md
```

---

## 6. Services & Ports

| Service | Local Port | Docker Internal Port |
|---|---|---|
| Order API | `5263` | `8080` |
| Invoice API | `5034` | `8080` |
| Notification API | `5059` | `8080` |
| React Frontend (nginx) | `3000` | `80` |
| Kafka (external listener) | `29092` | `9092` |
| Zookeeper | — | `2181` |

---

## 7. Message Brokers

### Apache Kafka (fully implemented)

- **Publisher:** `KafkaPublisher` — uses `Confluent.Kafka` `IProducer` to send JSON-serialized events to a topic. Message headers are attached as Kafka `Headers` (byte arrays).
- **Consumer:** `InvoiceKafkaConsumer` / `NotificationKafkaConsumer` — `BackgroundService` running a `while` loop. Uses `AutoOffsetReset.Earliest` so messages are replayed from the beginning if the consumer was offline. Kafka retains messages; consumers track their own offsets via **Consumer Groups**.
- **Key architectural note:** Kafka never deletes a message after it is consumed. The consumer's position (offset) is what determines what has and hasn't been processed. This enables fault tolerance and replay.

### AWS SNS + SQS (stub — Phase 8)

- **Publisher:** Would call `AmazonSNSClient.PublishAsync()` with message attributes for routing.
- **Consumer:** Would poll `AmazonSQSClient.ReceiveMessageAsync()` and **must call `DeleteMessageAsync` after processing**. Without deletion, SQS re-delivers the message after the Visibility Timeout expires.
- **Fan-out topology:** One SNS Topic → two SQS Queues (one per downstream service). SNS handles the fan-out automatically.

### Google Cloud Pub/Sub (stub — Phase 8)

- **Publisher:** Would call `PublisherClient.PublishAsync()`. Message attributes map directly to `PubsubMessage.Attributes`.
- **Consumer:** Uses event-driven `SubscriberClient`. Instead of writing a polling loop, you provide a handler function. The SDK calls it for each message and expects either `Reply.Ack` (success) or `Reply.Nack` (failure — will be redelivered).
- **Key architectural note:** This is fundamentally different from the Kafka `while` loop — the SDK drives the loop, not your code.

---

## 8. Core Domain Flow

When a user places an order, the following happens:

```
1. Browser sends POST /api/orders { customerId, amount, items, isVip }

2. OrdersController creates an OrderCreatedEvent:
   {
     orderId:    "a1b2c3...",
     customerId: "customer-42",
     amount:     149.99,
     items:      ["Item A", "Item B"],
     timestamp:  "2026-04-14T10:00:00Z"
   }

3. A MessageHeaders object is created:
   { "CustomerTier": "VIP" }   ← if isVip = true
   { "CustomerTier": "Standard" } ← otherwise

4. BrokerSwitcher.PublishAsync(topic, event, headers) is called.
   The active publisher (Kafka/AWS/GCP) serializes and sends it.

5. All consumers subscribed to the topic receive the event in parallel:

   InvoiceKafkaConsumer:
   → Reads CustomerTier from headers (no deserialization needed for routing)
   → Generates invoice number: "INV-A1B2C3D4"
   → Broadcasts to SignalR: "[InvoiceApi] Invoice generated — INV-A1B2C3D4 | ..."

   NotificationKafkaConsumer:
   → Reads CustomerTier from headers
   → VIP: sends "SMS + Email" / Standard: sends "Email"
   → Broadcasts to SignalR: "[NotificationApi] Notification sent via SMS + Email | ..."

6. Browser receives both events via WebSocket and appends them to the live console.
```

---

## 9. Key Demo Scenarios

### Fan-Out

Place one order. Watch the EventMonitor receive two independent events simultaneously — one from Invoice API and one from Notification API — from a single published message.

### Fault Tolerance (Message Durability)

1. Stop Invoice API (`docker-compose stop invoice-api`)
2. Place several orders — they are queued in Kafka
3. Restart Invoice API (`docker-compose start invoice-api`)
4. Watch it catch up and process all missed messages immediately

This works because Kafka retains messages. The consumer picks up from the last committed offset.

### Horizontal Scaling (Consumer Groups)

```bash
docker-compose up --scale invoice-api=2
```

Both Invoice API instances join the same Kafka Consumer Group (`invoice-consumer-group`). Kafka distributes partitions between them — each message is processed by **exactly one** instance (round-robin distribution). This demonstrates load balancing at the broker level.

### Message Routing via Attributes

The `CustomerTier` header is set **before** publishing and read **before** deserializing the payload body:

```
isVip = true  →  header: CustomerTier = "VIP"   →  Notification sends: SMS + Email
isVip = false →  header: CustomerTier = "Standard" →  Notification sends: Email only
```

Routing decisions are made from metadata, not payload content — a key pattern in message-driven systems.

### Hot-Swap Broker (Strategy Pattern live demo)

Click a broker card in the UI (or call `POST /api/system/switch-broker`). The `BrokerSwitcher` disposes the current publisher and replaces it with the new one. The next `PlaceOrder` call uses the new broker — with zero code changes and zero restart.

---

## 10. API Reference

### Order API — `http://localhost:5263`

| Method | Endpoint | Description |
|---|---|---|
| `POST` | `/api/orders` | Place an order and publish an event |
| `GET` | `/api/system/active-broker` | Get the currently active broker |
| `POST` | `/api/system/switch-broker` | Switch the active broker at runtime |
| `GET` | `/api/events/logs` | Get in-memory event log |
| `DELETE` | `/api/events/logs` | Clear the event log |

**POST /api/orders — request body:**
```json
{
  "customerId": "customer-42",
  "amount": 149.99,
  "items": ["Item A", "Item B"],
  "isVip": true
}
```

**POST /api/system/switch-broker — request body:**
```json
{
  "brokerType": "Kafka"
}
```
Valid values: `Kafka`, `Aws`, `Gcp`

### Invoice API — `http://localhost:5034`

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/api/invoices/logs` | Get processed invoices log |

### Notification API — `http://localhost:5059`

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/api/notifications/logs` | Get sent notifications log |

### SignalR Hubs (WebSocket)

| API | Hub URL |
|---|---|
| Order API | `ws://localhost:5263/hubs/events` |
| Invoice API | `ws://localhost:5034/hubs/events` |
| Notification API | `ws://localhost:5059/hubs/events` |

Event name: `ReceiveEvent` — payload is a plain string log entry.

---

## 11. Running the Project

### Option A — Docker (recommended, single command)

Requires: Docker Desktop

```bash
docker-compose up --build
```

This starts all 7 services: Zookeeper, Kafka, Order API, Invoice API, Notification API, and the React frontend. The APIs wait for Kafka to be healthy before starting.

Open the frontend at **http://localhost:3000**

To stop:
```bash
docker-compose down
```

To demonstrate horizontal scaling:
```bash
docker-compose up --build --scale invoice-api=2
```

### Option B — Local Development

Requires: .NET 10 SDK, Node.js 22, Docker (for Kafka only)

**Step 1 — Start Kafka:**
```bash
docker-compose up zookeeper kafka
```

**Step 2 — Start the APIs** (3 separate terminals):
```bash
cd EventDrivenDemo.Api && dotnet run
cd EventDrivenDemo.InvoiceApi && dotnet run
cd EventDrivenDemo.NotificationApi && dotnet run
```

**Step 3 — Start the frontend:**
```bash
cd frontend && npm install && npm run dev
```

Open the frontend at **http://localhost:5173**

---

## 12. Frontend

The React frontend serves as a **System Control Center** with three sections:

**Broker Selector** — Three clickable cards (Kafka, AWS, GCP). Clicking one calls `POST /api/system/switch-broker` and highlights the active broker. This is the live Strategy Pattern demo.

**Order Form** — Input fields for Customer ID, amount, and items, plus a VIP toggle. Submitting calls `POST /api/orders`. The VIP flag controls the `CustomerTier` message header and changes the notification channel.

**Event Monitor** — A dark, console-style scrolling window that shows real-time events from all three APIs simultaneously via three SignalR WebSocket connections. No page refresh needed. Each line is timestamped and prefixed with its source API (`[OrderApi]`, `[InvoiceApi]`, `[NotificationApi]`).

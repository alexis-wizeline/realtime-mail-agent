# REAL TIME MAIL AGENT

Basic application that implements the outbox pattern, allowing to recives emails as events and be processed in real time.

## Overview

The Real Time Mail Agent is desinged ro recive events from an email server and process them in real time, It creates a queue of outbox events that will be processed by a worker queue.

By generating seprating an event into an incomming and outbox event, the application can crate multiple tasks that can be processed in parallel, and delivered to ist respective destination.

## Getting Started

### Prerequisites to running locally

- Go 1.26.3 or higher
- Docker

### Running the application

- Postgres database
- Kafka broker

```bash
docker-compose up -d
goose postgres "postgres://postgres:postgres@localhost:5432/mail_agent" up --dir sql/migrations
```

- Run the application

```bash
go run main.go
```

### Configuration

Th eppliaction is configured to read an .env file in the root directory, with the following variables:

```bash
DATABASE_URL=postgres://postgres:postgres@localhost:5432/mail_agent
PORT=8080
LOG_FILE=app.log
KAFKA_URL=localhost:9092
```

## Usage

```
curl -X POST http://localhost:8080/v1/mail-events -H "Content-Type: application/json" -d '{
  "event_id": "1asdasdnsd2",
  "user_id": "dsfdfsd2weadzc",
  "message_id": "23r4sfsfsdf",
  "type": "email"
  }'
```

## Testing

```bash
DATABASE_URL=postgres://user:password@localhost:5432/real_time_mail_agent_test?sslmode=disable KAFKA_URL=localhost:9092 go test ./... -v
```

## Retrospective

This project was a learning exercise covering event-driven architecture, the outbox pattern, Kafka, idempotency, atomicity, and concurrency. For the full write-up of what I learned and the design decisions behind it, see [RETROSPECTIVE.md](./docs/retrospective.md).

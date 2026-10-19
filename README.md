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
```

- Run the application

```bash
go run main.go
```

### Configuration

Th eppliaction is configured to read an .env file in the root directory, with the following variables:

```bash
DATABASE_URL=postgres://user:password@localhost:5432/real_time_mail_agent?sslmode=disable
PORT=8080
LOG_FILE=app.log
KAFKa_URL=localhost:9092
```

## Usage

```
CURLE -X POST http://localhost:8080/v1/mail-events -H "Content-Type: application/json" -d '{
  "event_id": "1asdasdnsd2",
  "user_id": "dsfdfsd2weadzc",
  "message_id": "23r4sfsfsdf",
  "type": "email"
  }'
```

## Testing

```bash
DATABASE_URL=postgres://user:password@localhost:5432/real_time_mail_agent_test?sslmode=disable KAFKA_URL=localhost:9092 go test ./... -v`
```

## Retrospective

This porject is enterly drived by curiosity and preparation for future projects, it was a great opportunity to learn about several topics including:

- Event driven architecture
- Outbox pattern
- Kafka
- Idempotency
- Atomicity
- Concurrency

In some examples I can provide is intersting to see how just a single event can be derived into multiple events, in esence we can increasing the number of events to process as the requeriments of the application grows, making distinct types for the same event.

When i start i didn't know in reality what was the magic behind idempotency being handled not by the application, rather by the database, if a request is amde at the same time by two diferents clients, we can add a unique constraint to the database and let it handle the idempotency, this is a great example of how we can delegate responsabilities to the right layer of the application. But still depeding of how we desing the application or which technologies we use, this deliver errors that we need to interpret and handle in the application to deliver the correct response.

Also by implementing this unique constraint we ended up facing another set of problems like which entity shoudl be idempotent, what would happen if the unique constraint is violated, and we need to appply atomicity in the application by ensuring that all data si commited before teh transaction is completed.

also in database operations this application uses 2 types of locks, pessimistic and optimistic locks, pesimistic lock is used when a worker is clamming a batch of events to process, it uses the SKIP LOCK FOR UPDATE clause to ensure that otehr works can't see the same events, while another worker is claiming these events, optimistic lock is used when a worker is done with the event and wants to update the status of the event, it ensure that the event is in the correct status and is the correct worker that is updating the event, if no of these conditions the update will fail.

In terms of the architecture of the application, what i understand of a good desing is that we need to ensure that we don't allow bad operations to be permoed, by example when we introduce a new event, this start in an status of pending, when we define a life cicel of an event we also need to define the valid transition between these statuses. Once we have defined the valid transtions we can implent the satte machine in the application also defining when and how transitions can be made, this is a great way to ensure that we don't allow bad operations to be performed in the application.

Something that i foufn very intesrting is the amount of things we need to think by defining each section of the application, by example when we define an event entity we need to think about how are we aiming to store the entity, how many entities we wil derived and which statuses are valid for each entity.

In other hand, when defining the worker queue we need to think about how a worker will behave when processing an event, what sates of the event are valid for processing, and what an attempt is when we either claim an event or process and event, and where or polciy or rules for retrying lives.

Adding to this when start exposing our app to other services we need to think about how will we cmunicate with other services, what is the contract of the API or how the other application will work. BUt not only this we also be thinking on cases were comunication with other fails and we don't know if it failed or succed, leaving us in a state of uncertainty, and we need to define how we will handle this cases.

Working with a worker queue i think is very similar to working with distributed systems, we need to start with a core desing on how a single thing will work, the is scalated to multiple workers, but also you need tos tart think what will happen when X happens, like a worker stops before processing an event or while processing the event or if the application stops while processing events? I stil think i'm not if I fully understand the implications and rules of concurrency, but at least I got exposed to this topic and I was able to learn a little more of this.

In terms of Kafka I never use kafka before, I use RabbiMQ once before and in some ways to me seems semilar, but it has other concepts that are not the same, like an example for a producer to publisha message to topic, the producer can or should support aknoledgements, this is to ensure our message fot delivered to the topic, while in RabbitMQ we just assume that the message got delivered, on the other hadn, a consumer in Kafka is responsible for commiting the offset of the message so it does not read the same message again, this is because kafka stores the message in disk and the consumer is responsible to let the broker which message is already processed, while in RabbitMQ the consumer is responsible for aknowledging the message, and this message will disappear from the queue.

To close as this is just a personal project it still lets some questions open, like what attemps == max attempts + expired lease means? or when an incomming event is considered processed or failed or when these status change? in this case i focus on the outbox events, working pool and kafka deliveries. My idea was that probably we will handle this in distinct processes, every certain time we query relative outbox events of incomming events and update their status, is one of the many ways we can handle this.

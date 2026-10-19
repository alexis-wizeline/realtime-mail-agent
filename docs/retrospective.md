# Retrospective

This project was driven entirely by curiosity and by a desire to prepare for future projects. It was a great opportunity to learn about several topics, including:

- Event-driven architecture
- The outbox pattern
- Kafka
- Idempotency
- Atomicity
- Concurrency

## Deriving multiple events from one

One interesting takeaway is how a single event can be derived into multiple events. In essence, we can increase the number of events to process as the requirements of the application grow, creating distinct types from the same event.

## Idempotency at the database layer

When I started, I didn't really understand the magic behind idempotency being handled by the database rather than the application. If a request is made at the same time by two different clients, we can add a unique constraint to the database and let it handle idempotency. This is a great example of delegating a responsibility to the right layer of the application.

Still, depending on how we design the application and which technologies we use, this approach produces errors that we need to interpret and handle in order to return the correct response. Implementing the unique constraint led to another set of questions, such as which entity should be idempotent and what should happen when the constraint is violated. It also meant applying atomicity in the application by ensuring that all data is committed before the transaction completes.

## Pessimistic locking

For database operations, this application uses pessimistic locks:

- The **pessimistic lock** is used when a worker claims a batch of events to process. It uses the `FOR UPDATE SKIP LOCKED` clause so that other workers can't see the same events while one worker is claiming them.

## Safe transitions

Once a worker is done with an event and wants to update its status, it uses a mechanism similar to an optimistioc lock. It ensures that the event is in the correct status and that it is the correct worker updating the event. If either of these conditions is not met, the update will fail.

## Designing to prevent invalid operations

In terms of architecture, what I understand of a good design is that we should prevent invalid operations from being performed. For example, when a new event is introduced it starts in a `pending` status. When we define the lifecycle of an event, we also need to define the valid transitions between statuses. Once those transitions are defined, we can implement a state machine that governs when and how transitions are made. This is a great way to ensure that invalid operations are rejected.

## How much there is to consider per entity

Something I found very interesting is how many things we need to consider when defining each part of the application. For example, when defining an event entity, we need to think about how we intend to store it, how many entities it will be derived into, and which statuses are valid for it.

When defining the worker queue, we need to think about how a worker behaves while processing an event, which event states are valid for processing, what an "attempt" means when we claim or process an event, and where the retry policy and rules live.

## Communicating with other services

When we start exposing our app to other services, we need to think about how we will communicate with them and what the API contract is. Beyond that, we also need to handle cases where communication with another service fails and we don't know whether it failed or succeeded, leaving us in a state of uncertainty. We need to define how we handle those cases.

## Worker queues and distributed systems

Working with a worker queue is, I think, very similar to working with distributed systems. We start with a core design for how a single unit works, then scale it to multiple workers. We also need to anticipate failure: what happens when a worker stops before processing an event, stops while processing it, or when the application stops mid-processing?

## Kafka vs. RabbitMQ

I had never used Kafka before; I had used RabbitMQ once. In some ways they seem similar, but Kafka has concepts that are not the same:

- To publish a message to a topic, a Kafka producer can (and should) support acknowledgements to ensure the message was delivered to the topic. With RabbitMQ, we often just assume the message was delivered.
- A Kafka consumer is responsible for committing the offset of a message so it does not read the same message again. This is because Kafka stores messages on disk, and the consumer tells the broker which messages have been processed. In RabbitMQ, the consumer acknowledges the message, and the message then disappears from the queue.

## Open questions

As this is a personal project, some questions remain open — for example, what `attempts == max_attempts` combined with an expired lease should mean, or exactly when an incoming event is considered processed or failed and when those statuses change. Here I focused on the outbox events, the worker pool, and Kafka deliveries. My idea was that these cases would likely be handled in separate processes: on a schedule, we would query the relevant outbox and incoming events and update their statuses. That is one of many ways to handle it.

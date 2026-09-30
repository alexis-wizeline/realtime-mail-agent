# Current Architecture

```
- entrypoint: /create-event
|
|
|
- ----|----------------------|
    incoming event        outbox-event
|----------------------------|
                             |
                             |
                             - working pool -|
                                             |-----------|---------|
                                            worker 1   worker 2  worker 3
                                             |           |         |
                                             |___________|_________|
                                                         |
                                                         |_______Processor_______ output: kafka
```

Entrypoint: the current start of the flow is the `/create-event` enpoint, which is resposible to recieve a new event, this endpoint is responsible of validating the event payload, and create an `incomming event` with the respective `outbox-event` to be processed by the workers.

Incoming event: the `incoming event` is the representation of the event that was recived by the entry point, this events is created in database at the same time as the `outbox-event`, the re is a constraint, only a single event can be reated for agiven `event_id`.

Outbox-event: the `outbox-event` is the representation of the event that will be processed by the workers, these events are created at the same time as the `incoming event`, if one of the 2 creations fails, the result will be rolled back.

Working pool: the `working pool` is in charge of start the workers that will process the `outbox-events`, once the pool start it will wait until all workers are done to return.

Workers: the workers are responsible to process the `outbox-events`, each worker can take batches of up to N events but each event will be worked sequentially, the worker will send the event to the `Processor` and wait for the response, if the response is provided as successful (no error was returned), the event will be marked as processed, if the response is provided as failed (an error is returned), based on the type of the error, the event will be marked as failed to be retried or discarded.

Processor: The `Processor` is the component that will send the event to kafka, depending on the result, it will return an error to specify if the event can be retried or not. if no error is returned, this means the operation was successful.

## Events DB schema

```sql
TABLE incoming_events (
id UUID PRIMARY KEY,
event_id STRING NOT NULL UNIQUE,
user_id STRING NOT NULL,
mesage_id STRING NOT NULL,
payload JSONB NOT NULL,
status STRING NOT NULL DEFAULT 'recived',
created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
processed_at TIMESTAMPTZ NULL,
)

TABLE outbox_events (
id UUID PRIMARY KEY,
incoming_event_id UUID NOT NULL,
type STRING NOT NULL,
payload JSONB NOT NULL,
status STRING NOT NULL DEFAULT 'pending',
current_error STRING NULL,
attempts INT NOT NULL DEFAULT 0,
max_attempts INT NOT NULL DEFAULT 5,
locked_by STRING NULL,
locked_until TIMESTAMPTZ NULL,
next_attempt TIMESTAMPTZ NULL,
created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(
processed_at TIMESTAMPTZ NULL
)
```

The creation of the events should be idempotent, meaning that only one event for the same `event_id` can be created no matter how many attemps are made to create the event. This is handled by the database as a constratint on the `event_id` column of the `incoming_events` table. If an attempt is made to create an event with a duplicate `event_id`, the database will reject the operation, ensuring that only one unique event is stored for each `event_id`.

This operation is also atomic means that if the creation of the `incoming_event` fails, the `outbox_event` will not be created, and vice versa. The transaction will be rolled back to maintain data integrity.

Also when a worker intends to process an `outobox-event`, is important to ensure that the event is not ebing processed by another worker at the same time. This is handled by the `locked_by` and `locked_until` columns in the `outbox_events` table. Also while the worker is claiming the event, it needs to be handled in a trasaction using the SKIP LOCKED FOR UPDATE clause to ensure no other worker can claim the same event at the same time.

For `outbox-events` the maxt_attemps represents the maximum number of times a worker can claim an event. Because this decission an event reching the max_attemps can not describe if then was sent to kafka or not.

## Work flow cases

### Case 1: Event is created and processed succesfully

1. A request is made to the /created-event-enpoint with a valid event payload.
2. The endpoint validates the payload and creates an `incoming_event` and an `outbox_event` in the database within a single trsanaction.
3. The `working pool` is already running
4. One of the workers claims the `outbox_event` for processing.
5. The worker sends the event to the `Processor`.
6. The `Processor` suceessfully sends the event to Kafka and returns no error.
7. The worker marks the `outbox_event` as processed in the database.

### Case 2: Event is created but fails to be processed.

1. A request is made to the /created-event-enpoint with a valid event payload.
2. The endpoint validates the payload and creates an `incoming_event` and an `outbox_event` in the database within a single transaction.
3. The `working pool` is already running
4. One of the workers claims the `outbox_event` for processing.
5. The worker sends the event to the `Processor`.
6. The `Processor` fails to send the event to Kafka and returns an error indicating that the event can be retried.
7. The worker marks the `outbox_event` as failed in the database, the attemps was already incresed while claiming the event, and the `next_attempt_at` is set to a future time base on the retry policy.

### Case 3: Event is created but fails to be processed and reaches max attempts.

1. A request is made to the /created-event-enpoint with a valid event payload.
2. The endpoint validates the payload and creates an `incoming_event` and an `outbox_event` in the database within a single transaction.
3. The `working pool` is already running
4. One of the workers claims the `outbox_event` for processing.
5. The worker sends the event to the `Processor`.
6. The `Processor` fails to send the event to Kafka and return an error indicating that the event can be retried.
7. The worker marks the `outbox_event` as failed in database, the attemps was already increased while claiming the event, and the next_attempt_at is set to a future time based on the retry policy.
8. The worker tries to claim the same `outbox_event` again after the `next_attempt_at` time has passed.
9. The worker sends the event to the `Processor` again.
10. The `Processor` fails to send the event to Kafka and returns an error indicating that the event can be retried.
11. The worker marks the `outbox_event` as failed in the database, the attempts are increased again, and the `next_attempt_at` is set to a future time based on the retry policy.
12. Steps 8-11 are repeated until the `outbox_event` reaches the maximum number of attempts (5).
13. Once the maximum attempts is reached, the worker marks the `outbox_event` as discarded in the database, and it will no longer be claimed for processing.

## Case 4: Event is created but fails to be processed and is discarded.

1. A request is made to the /created-event-enpoint with a valid event payload.
2. The endpoint validates the payload and creates an `incoming_event` and an `outbox_event` in the database within a single transaction.
3. The `working pool` is already running
4. One of the workers claims the `outbox_event` for processing.
5. The worker sends the event to the `Processor`.
6. The `Processor` fails to send the event to Kafka and returns an error indicating that the event cannot be retried (e.g., due to a validation error).
7. The worker marks the `outbox_event` as discarded in the database, and it will no longer be claimed for processing.

## Case 5: Event is created and processed but the worker stops before marked as processed.

1. A request is made to the /created-event-enpoint with a valid event payload.
2. The endpoint validates the payload and creates an `incoming_event` and an `outbox_event` in the database within a single transaction.
3. The `working pool` is already running
4. One of the workers claims the `outbox_event` for processing.
5. The worker sends the event to the `Processor`.
6. The `Processor` successfully sends the event to Kafka and returns no error.
7. Before the worker can mark the `outbox_event` as processed, the worker stops unexpectedly (e.g., due to a crash or shutdown).
8. The `outbox_event` remains in the database with its status still set to 'processing' and the `locked_by` and `locked_until` fields indicating that it was being processed by the worker.
9. When the ` working pool` restarts, it will check for any `outbox_events` that are still locked and have exceeded their `locked_until` time. The `outbox_event` will be elegible to be processed again.
10. A worker can claim the `outbox_event` again and send it to the `Processor` for processing.
11. The `Processor` successfully sends the event to Kafka and returns no error.
12. The worker marks the `outbox_event` as processed in the database, completing the workflow successfully.

## Case 6: Event is created and processed but the worker stops before marked as failed.

1. A request is made to the /created-event-enpoint with a valid event payload.
2. The endpoint validates the payload and creates an `incoming_event` and an `outbox_event` in the database within a single transaction.
3. The `working pool` is already running
4. One of the workers claims the `outbox_event` for processing.
5. The worker sends the event to the `Processor`.
6. The `Processor` fails to send the event to Kafka and returns an error indicating that the event can be retried.
7. Before the worker can mark the `outbox_event` as failed, the worker stops unexpectedly (e.g., due to a crash or shutdown).
8. The `outbox_event` remains in the database with its status still set to 'processing' and the `locked_by` and `locked_until` fields indicating that it was being processed by the worker.
9. When the `working pool` restarts, it will check for any `outbox_events` that are still locked and have exceeded their `locked_until` time. The `outbox_event` will be elegible to be processed again.
10. A worker can claim the `outbox_event` again and send it to the `Processor` for processing.
11. The `Processor` fails to send the event to Kafka and returns an error indicating that the event can be retried.
12. The worker marks the `outbox_event` as failed in the database, the `next_attempt_at` is set to a future time based on the retry policy.

## Case 7: Event is created and processed but the worker stops before marked as discarded.

1. A request is made to the /created-event-enpoint with a valid event payload.
2. The endpoint validates the payload and creates an `incoming_event` and an `outbox_event` in the database within a single transaction.
3. The `working pool` is already running
4. One of the workers claims the `outbox_event` for processing.
5. The worker sends the event to the `Processor`.
6. The `Processor` fails to send the event to Kafka and returns an error indicating that the event cannot be retried (e.g., due to a validation error).
7. Before the worker can mark the `outbox_event` as discarded, the worker stops unexpectedly (e.g., due to a crash or shutdown).
8. The `outbox_event` remains in the database with its status still set to 'processing' and the `locked_by` and `locked_until` fields indicating that it was being processed by the worker.
9. When the `working pool` restarts, it will check for any `outbox_events` that are still locked and have exceeded their `locked_until` time. The `outbox_event` will be elegible to be processed again.
10. A worker can claim the `outbox_event` again and send it to the `Processor` for processing.
11. The `Processor` fails to send the event to Kafka and returns an error indicating that the event cannot be retried (e.g., due to a validation error).
12. The worker marks the `outbox_event` as discarded in the database, and it will no longer be claimed for processing.

## Case 8: Event is tried to be created multiple times concurrently.

1. Multiple requests are made to the /created-event-enpoint with the same valid event payload concurrently.
2. The endpoint validates the payload and attempts to create an `incoming_event` and an `outbox_event` in the database within a single transaction for each request.
3. Due to the unique constraint on the `event_id` column in the `incoming_events` table, only one of the requests will create the `incoming_event` and `outbox_event`. The other requests will face a database error, but it will be treated as expected in the same database layer, returnning nothing but no error.
4. All request will return a success response.

for the case 5 when the event was already processed by the `Processor` and in consequence already delivered to Kafka, but the worker stops before completed the marking to `delivered` in the database. These events will be processed again and it will be delivered again to Kafka, this is know issue. and this expose that the syste is maded to be delivered at least once, but not exactly once. In consequence, all the downstream systems that cosnumes these events must be idepotent, meaning they need to handle the same evetn even if its delivered multiple times.

## What an attempt means?

An attempt represents a worker claim of an `outbox-event`

A claimed event can crash before being marked as processed, means that can be crashed during, or after the external publishing to Kafka. So an attempt can not garantee tyhat an event if Kafka recieves the event.

An expired lease represent an abigous state of the processing state.

because this sytem is desinged as at least once delivery, Kafka can be receivening duplicated events, and the downstream systems must be able to handle this situation.

open questions:

- what attempts == max_attemps + expired lease means for the system?

## Status of the events

The `outbox-events` can have the following status:

- pending: the event is waiting to be processed by a worker.
- processing: the event is being processed by a worker.
- published: the event was successfully processed and sent to Kafka.
- failed: the event was processed by a worker but failed to be sent to Kafka, and it can be retried.
- discarded: the event was processed by a worker but failed to be sent to Kafka, and it cannot be retried anymore (e.g., due to reaching the maximum number of attempts or a non-retryable error).

a valid transition of the status of an `outbox-event` is as follows:

- pending -> processing -> published
- pending -> processing -> failed (if the event can be retried)
- pending -> processing -> discarded (if the event cannot be retried anymore)
- failed -> processing -> published (if the event can be retried)
- failed -> processing -> discarded (if the event cannot be retried anymore)
- processing -> processing (if the event is claimed by another worker after the `locked_until` time has passed)

there are no valid transitions from published or discarded to any other status, as these represent terminal states for the `outbox-event` and there are no queries that can change their status.

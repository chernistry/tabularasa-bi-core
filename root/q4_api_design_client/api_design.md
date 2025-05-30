# Q4 Data Ingestion API Design

## Endpoint Design

### `POST /v1/bi-events`

This endpoint allows external systems to push event data to Taboola's BI platform.

#### Request Headers
- `Content-Type: application/json` (required)
- `Authorization: Bearer <token>` (required) - JWT-based authentication token
- `X-Request-ID: <uuid>` (optional) - Client-generated unique identifier for request tracing
- `X-Batch-ID: <uuid>` (optional) - For idempotency of batch processing (see Idempotency section)

#### Request Payload

```json
{
  "batch_id": "uuid-string",      // Required: Unique identifier for this batch
  "source": "string",             // Required: Source system identifier
  "events": [                     // Required: Array of events
    {
      "event_id": "uuid-string",  // Required: Unique identifier for this event
      "timestamp": "2024-07-25T14:32:10.123Z", // Required: ISO 8601 format
      "event_name": "string",     // Required: Type of event
      "attributes": {             // Required: Event-specific details
        "property1": "value1",
        "property2": 123,
        "property3": true,
        "nested": {
          "subproperty": "value"  // Flexible schema for event-specific data
        },
        "array_property": [1, 2, 3]
      }
    }
  ]
}
```

#### Successful Response (202 Accepted)

```json
{
  "status": "accepted",
  "batch_id": "uuid-string",     // Echo of the submitted batch_id
  "message": "Batch accepted for processing",
  "request_id": "system-generated-request-id"
}
```

#### Error Responses

**400 Bad Request**

```json
{
  "status": "error",
  "code": "validation_error",
  "message": "Validation error in batch",
  "details": [
    {
      "field": "events[0].timestamp",
      "error": "Invalid timestamp format"
    }
  ],
  "request_id": "system-generated-request-id"
}
```

**401 Unauthorized**

```json
{
  "status": "error",
  "code": "unauthorized",
  "message": "Invalid or missing authentication token",
  "request_id": "system-generated-request-id"
}
```

**409 Conflict** (Duplicate batch)

```json
{
  "status": "error",
  "code": "duplicate_batch",
  "message": "This batch has already been processed",
  "previous_request_id": "id-of-original-request",
  "request_id": "system-generated-request-id"
}
```

**500 Internal Server Error**

```json
{
  "status": "error",
  "code": "server_error",
  "message": "An unexpected error occurred",
  "request_id": "system-generated-request-id"
}
```

## Key Design Considerations

### 1. Idempotency

To handle retries safely, the API implements the following idempotency mechanism:

- Each event batch must include a client-generated `batch_id` that uniquely identifies the batch.
- The system stores processed `batch_id` values with their processing status.
- If a duplicate `batch_id` is received, the system returns a 409 Conflict response with a reference to the original request.
- This allows clients to safely retry failed requests without risking duplicate event processing.
- The system maintains batch_id records for a reasonable time period (e.g., 7 days).

### 2. Authentication and Authorization

- The API uses JWT-based bearer token authentication.
- Tokens must be obtained from Taboola's authentication service.
- Different clients may have different permissions (read-only, write-specific-event-types, etc.).
- Token scopes determine which event types a client can publish.

### 3. Validation

The API performs several validation checks:
- Structural validation (required fields, data types)
- Schema validation for known event types
- Business rule validation (e.g., timestamps must be recent, not future-dated)
- Size limits (max batch size = 1000 events, max payload size = 5MB)

### 4. Rate Limiting

- Rate limits are enforced based on client identity.
- Limit: 100 requests per minute per client by default.
- Limit: 100,000 events per hour per client by default.
- Custom limits can be configured for high-volume clients.
- Rate limit headers are included in responses (X-RateLimit-Limit, X-RateLimit-Remaining, X-RateLimit-Reset).

### 5. Asynchronous Processing

- The API accepts events asynchronously (202 Accepted).
- Events are placed in a queue for processing.
- Clients cannot assume immediate processing or storage.
- For critical events where confirmation is required, a separate status check endpoint would be available.

### 6. Security

- All API traffic is encrypted using TLS 1.2+.
- Input validation protects against injection attacks.
- Event payloads are limited in size to prevent DoS attacks.
- Authentication tokens have limited lifetimes and scopes. 
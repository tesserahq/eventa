# Architecture

This document provides an overview of Eventa's system design, core data models, and integration patterns.

## System Overview

Eventa is built as a FastAPI-based microservice that receives, stores, and queries events in a structured timeline. It follows a service-oriented architecture with clear separation of concerns between models, services, routers, and event ingestion mechanisms. Eventa is designed as a passive event consumer—it receives events from other services via NATS and the REST API, storing them for later analysis and consumption.

## Technology Stack

- **Framework**: FastAPI (Python 3.11+)
- **Database**: PostgreSQL with SQLAlchemy ORM (TimescaleDB extension recommended for time-series queries)
- **Event Streaming**: NATS JetStream for event ingestion
- **Migrations**: Alembic
- **Authentication**: OIDC (OpenID Connect) with JWT tokens via Identies service
- **Dependency Management**: Poetry
- **Observability**: OpenTelemetry, Prometheus metrics, Rollbar error tracking

## Core Data Models

### Event Model

The `Event` model is the central data structure in Eventa, representing events received from various sources in the platform.

**Key Fields:**
- `id` (UUID): Primary key
- `source` (String): The source system that generated the event
- `spec_version` (String): Event specification version (e.g., "1.0")
- `event_type` (String): Type of event (e.g., "user.created", "order.completed")
- `event_data` (JSONB): Complete event payload stored as JSON
- `data_content_type` (String): Content type of the event data (e.g., "application/json")
- `subject` (String): Subject or identifier related to the event
- `time` (DateTime): Timestamp when the event occurred
- `tags` (ARRAY[String]): Array of tags for filtering and categorization
- `labels` (JSONB): Key-value pairs for advanced filtering and metadata
- `privy` (Boolean): Privacy flag indicating if the event is private
- `user_id` (UUID, optional): Foreign key to User, if the event is associated with a user

**Relationships:**
- Many-to-one with `User` (events can optionally be associated with users)

**Indexes:**
- Index on `user_id` for efficient user-based queries
- Index on `time` for time-series queries (especially with TimescaleDB)
- GIN indexes on `tags` and `labels` for efficient JSONB queries

**Features:**
- Supports soft deletion via `SoftDeleteMixin`
- Timestamp tracking via `TimestampMixin` (created_at, updated_at, deleted_at)
- JSONB storage enables flexible event schemas without schema migrations

### User Model

The `User` model represents users in the system, primarily synced from the Identies service.

**Key Fields:**
- `id` (UUID): Primary key
- `email` (String): Unique email address
- `first_name`, `last_name` (String): User's name
- `external_id` (String, optional): ID from external identity provider
- `provider` (String, optional): Identity provider name (e.g., "google", "github")
- `verified` (Boolean): Email verification status
- `verified_at` (DateTime, optional): Timestamp of verification
- `confirmed_at` (DateTime, optional): Timestamp of confirmation
- `avatar_url` (String, optional): URL to user's avatar image

**Relationships:**
- One-to-many with `Event` (users can have multiple associated events)

**Indexes:**
- Unique index on `external_id` (where not null)
- Unique constraint on `email`

**Features:**
- Supports soft deletion via `SoftDeleteMixin`
- Timestamp tracking via `TimestampMixin`
- Users are typically onboarded from Identies service when events reference them

## Event Ingestion Architecture

### NATS JetStream Integration

NATS plays a **critical role** in Eventa's architecture as the primary mechanism for event ingestion. Eventa uses NATS JetStream to receive events from other services in the platform in a reliable, scalable manner.

**Key Components:**

1. **NATS Worker** (`run_nats_worker.py`): A dedicated worker process that subscribes to NATS subjects and processes incoming events
2. **FastStream Integration**: Uses FastStream library for NATS broker integration and message handling
3. **JetStream Streams**: Events are stored in JetStream streams for durability and replay capabilities

**Event Ingestion Flow:**

1. **Event Publication**: Other services in the platform publish events to NATS subjects (e.g., `com.mylinden.user.created`, `com.mylinden.order.completed`)
2. **NATS Subscription**: Eventa's NATS worker subscribes to subjects matching the configured pattern (default: `com.>`)
3. **Message Processing**: When an event is received:
   - The event payload is parsed and validated
   - An `EventCreate` schema is constructed from the message
   - The event is persisted to PostgreSQL via `EventRepository`
   - If the event contains a `user_id`, the worker attempts to fetch and onboard the user from Identies (if not already present)
4. **Durability**: JetStream ensures message durability and supports:
   - Durable consumers for reliable message processing
   - Delivery policies (e.g., `DeliverPolicy.LAST` for new consumers)
   - Queue groups for load balancing across multiple worker instances

**Configuration:**

- `NATS_ENABLED`: Enable/disable NATS integration
- `NATS_URL`: NATS server connection URL
- `NATS_QUEUE`: Queue group name for load balancing
- `NATS_SUBJECTS`: Comma-separated list of subject patterns to subscribe to
- `NATS_STREAM_NAME`: JetStream stream name for event storage

**Benefits of NATS Integration:**

- **Decoupled Architecture**: Event producers don't need direct knowledge of Eventa's API
- **Scalability**: Multiple Eventa workers can process events in parallel using queue groups
- **Reliability**: JetStream provides message persistence and guaranteed delivery
- **Replay Capability**: Events can be replayed from JetStream streams if needed
- **High Throughput**: NATS handles high-volume event streams efficiently

### REST API Event Ingestion

In addition to NATS, Eventa also accepts events via REST API endpoints, allowing services to directly POST events when NATS is not available or when synchronous event storage is required.

## Database Integration

### PostgreSQL Configuration

Eventa uses PostgreSQL as its primary data store with the following features:

- **Connection Pooling**: Configurable pool size and max overflow
- **Connection Management**: Automatic reconnection with `pool_pre_ping`
- **Application Naming**: Database connections tagged with application name for monitoring
- **Transaction Management**: SQLAlchemy session-based transactions
- **TimescaleDB Extension**: Recommended for time-series queries on event data

### Database Manager

The system uses `tessera_sdk.core.database_manager.DatabaseManager` for database operations, providing:

- Centralized connection management
- Session lifecycle handling
- Migration support via Alembic

### Migration Strategy

Database schema changes are managed through Alembic migrations:

- Migrations stored in `alembic/versions/`
- Version control for schema evolution
- Rollback support for failed migrations

## Authentication & Authorization

### Identies Integration

Eventa integrates with the **Identies** service for authentication and user management. Identies serves as the central identity provider for the platform.

**Authentication Flow:**

1. **Token Validation**: Eventa uses `AuthenticationMiddleware` from `tessera_sdk` to validate JWT tokens
2. **Identies Communication**: The middleware communicates with Identies service to validate tokens and fetch user information
3. **User Onboarding**: When events reference a `user_id` that doesn't exist locally, Eventa fetches the user from Identies and stores it locally
4. **Request Context**: Validated user information is attached to request context for use in API endpoints

**Configuration:**

- `IDENTIES_HOST`: Base URL of the Identies service
- `OIDC_DOMAIN`: OIDC provider domain (used by Identies)
- `OIDC_API_AUDIENCE`: API audience for token validation
- `OIDC_ISSUER`: OIDC issuer URL
- `DISABLE_AUTH`: Development flag to bypass authentication (should be `false` in production)

**User Onboarding from Events:**

When processing events via NATS, if an event contains a `user_id` that doesn't exist in Eventa's database:

1. The NATS worker fetches an M2M (machine-to-machine) token from Vaulta
2. Uses the Identies client to fetch user information
3. Creates a local `User` record using `UserRepository.onboard_user()`
4. Associates the event with the newly onboarded user

This ensures that user information is available for event queries and relationships, even if the user was never directly authenticated with Eventa.

### Authentication Middleware

The `AuthenticationMiddleware` processes incoming API requests:

1. Extracts JWT tokens from request headers
2. Validates tokens via Identies service
3. Onboards new users automatically if needed
4. Attaches user context to requests for use in endpoints

## Repository Layer Architecture

### Repository Pattern

Repositories encapsulate data access and database operations:

- **EventRepository**: Event CRUD operations, filtering, and queries
  - `create_event()`: Store new events
  - `get_events_by_user_id_query()`: Query events for a specific user
  - `get_events_by_tags_and_labels_query()`: Query events by tags and labels
  - `search()`: Dynamic filtering with various operators
  - Soft delete operations for event lifecycle management
- **UserRepository**: User CRUD operations and onboarding
  - `onboard_user()`: Create or update user from Identies
  - `get_user()`: Retrieve user by ID
  - User lifecycle management

### Query Patterns

Eventa supports several query patterns for retrieving events:

1. **User-Based Queries**: Filter events by `user_id` for personalized timelines
2. **Tag-Based Queries**: Filter events by tags (array containment)
3. **Label-Based Queries**: Filter events by JSONB label key-value pairs
4. **Combined Filters**: Tags and labels can be combined for complex queries
5. **Time-Based Queries**: Leverage TimescaleDB for efficient time-series queries
6. **Soft Delete Support**: Query active or deleted events separately

### Event Filtering

The system provides powerful filtering capabilities:

- **Tags**: Array-based filtering using PostgreSQL array containment (`@>`)
- **Labels**: JSONB-based filtering for key-value pair matching
- **Privy Flag**: Filter private vs. public events
- **Time Ranges**: Efficient time-based queries with TimescaleDB
- **User Association**: Filter events associated with specific users

## API Design

### Router Structure

Routers organize endpoints by domain:

- `/events`: Event query and management endpoints
  - `GET /events`: List events with filtering (by user_id OR tags/labels)
  - Supports pagination via `fastapi-pagination`
- `/health`: Health check endpoint
- `/metrics`: Prometheus metrics endpoint
- `/docs`: OpenAPI/Swagger documentation

### Event Query Endpoints

**List Events** (`GET /events`):

- **Filtering Options**:
  - `user_id`: Filter events by user (mutually exclusive with tags)
  - `tags`: Array of tags to match (requires at least one tag)
  - `labels`: JSON object with label key-value pairs
- **Pagination**: Uses `fastapi-pagination` for offset-based pagination
- **Validation**: Either `user_id` OR `tags` must be provided (not both)

**Query Examples:**

```bash
# Get events for a specific user
GET /events?user_id=123e4567-e89b-12d3-a456-426614174000

# Get events by tags
GET /events?tags=order&tags=completed

# Get events by tags and labels
GET /events?tags=order&labels={"status":"completed","priority":"high"}
```

### Response Format

All API responses follow a consistent format:

```json
{
  "data": [...]
}
```

Pagination responses include additional metadata:

```json
{
  "items": [...],
  "total": 100,
  "page": 1,
  "size": 20,
  "pages": 5
}
```

### Pagination

List endpoints support pagination via `fastapi-pagination`:

- Configurable page size
- Offset-based pagination
- Metadata included in responses (total count, page numbers, etc.)

## Observability

### Metrics

Prometheus metrics exposed at `/metrics`:

- Request counts and durations
- Database query metrics
- Custom business metrics

### Tracing

OpenTelemetry integration for distributed tracing:

- Request tracing across services
- Database query tracing
- Custom span creation

### Logging

Structured logging with configurable levels:

- JSON-formatted logs
- Contextual information
- Error tracking via Rollbar (production)

## Security Considerations

### Event Data Security

- **Privy Flag**: Events can be marked as private (`privy=true`) to restrict access
- **User Association**: Events can be associated with users for access control
- **Soft Deletes**: Deleted events are preserved for audit trails

### Database Security

- Parameterized queries prevent SQL injection
- Connection pooling limits resource exposure
- Soft deletes preserve audit trails
- JSONB fields are validated before storage

### Authentication Security

- JWT token validation with signature verification via Identies
- Token expiration enforcement
- Secure secret management via Vaulta
- M2M tokens for service-to-service authentication
- `DISABLE_AUTH` flag should never be `true` in production

### NATS Security

- NATS connection should use TLS in production
- JetStream streams provide message durability and replay protection
- Queue groups ensure message processing even if workers fail

## Event Storage Patterns

### Event Schema Flexibility

Eventa uses JSONB for event data storage, providing:

- **Schema Evolution**: Events can have different structures without migrations
- **Flexible Metadata**: Tags and labels allow dynamic categorization
- **Query Flexibility**: PostgreSQL JSONB operators enable complex queries
- **Storage Efficiency**: JSONB is compressed and indexed efficiently

### Time-Series Optimization

With TimescaleDB extension:

- **Hypertables**: Events table can be converted to a hypertable for time-series optimization
- **Automatic Partitioning**: Events partitioned by time for efficient queries
- **Retention Policies**: Automatic data retention and archival
- **Time-Based Queries**: Optimized queries for time ranges and aggregations

## Development Patterns

### Testing

- Pytest for unit and integration tests
- Fixtures for test data generation
- Faker for realistic test data
- Test database isolation

### Code Organization

```text
app/
├── models/          # SQLAlchemy models (Event, User)
├── schemas/         # Pydantic schemas (EventCreate, Event, etc.)
├── repositories/    # Data access (EventRepository, UserRepository)
├── routers/         # API endpoints (event, user)
├── messaging/       # NATS integration (nats_subscriber)
├── middleware/      # Request middleware (db_session, auth)
├── utils/           # Utility functions (filtering, cache, metrics)
└── tasks/           # Background tasks (fetch_user)
```

### Worker Processes

Eventa runs multiple processes:

1. **API Server**: FastAPI application serving REST endpoints
2. **NATS Worker**: Dedicated worker for processing NATS events (`run_nats_worker.py`)
3. **Celery Worker** (optional): For background task processing

## Future Considerations

- GraphQL API support for flexible event queries
- Enhanced event filtering with full-text search
- Event aggregation and analytics endpoints
- Webhook support for event notifications
- Rate limiting and throttling for API endpoints
- Event replay capabilities from JetStream streams
- Advanced time-series analytics with TimescaleDB
- Event schema validation and versioning

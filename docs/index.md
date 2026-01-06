# What is Eventa

**Eventa** is a backend service that receives and stores events in a structured, searchable timeline. It makes it easy to filter, search, and explore what happened in your platform and when. Unlike event producers, Eventa itself does not create events—it passively receives event data from other systems and persists it for later analysis and consumption by other services. Eventa leverages TimescaleDB to efficiently store, manage, and query event data at scale, enabling rich time-based queries and fast lookups.

# Core Responsibilities

Eventa is designed with the following key responsibilities:

- **Event Ingestion**: Receives events from other services and persistently stores them for future queries
- **Event Storage**: Persists events in a structured format using PostgreSQL with TimescaleDB, supporting efficient time-series queries and JSONB metadata
- **Event Filtering**: Enables fast filtering and querying across all event data
- **Timeline Management**: Organizes events chronologically to help you understand what has happened in your platform and when
- **Metadata Handling**: Stores event metadata such as tags, labels, and timestamps for advanced filtering and categorization
- **User Association**: Optionally associates events with users, supporting personalized timelines when user context is available

# Getting Started

- **[Quick Setup Guide](quick_setup.md)** - Fast track setup for the first system administrator
- **[Architecture](architecture.md)** - Learn about the system design, core data models, and how Eventa integrates with PostgreSQL and event systems.


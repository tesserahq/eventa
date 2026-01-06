# Quick Setup Guide

This guide will help you get Eventa up and running quickly for the first time.

## Prerequisites

Before you begin, ensure you have the following installed:

- **Python 3.11 or higher** - Check with `python --version`
- **PostgreSQL** - Version 12 or higher (TimescaleDB extension recommended for time-series queries)
- **Poetry** - Python dependency manager ([Install Poetry](https://python-poetry.org/docs/#installation))
- **Redis** (optional) - For caching and LlamaIndex integration
- **NATS** (optional) - For event streaming integration

## Step 1: Clone and Install Dependencies

```bash
# Navigate to the project directory
cd eventa

# Install dependencies using Poetry
poetry install
```

## Step 2: Database Setup

Create a PostgreSQL database for Eventa:

```bash
# Connect to PostgreSQL
psql -U postgres

# Create the database
CREATE DATABASE eventa;

# (Optional) Enable TimescaleDB extension for better time-series performance
# \c eventa
# CREATE EXTENSION IF NOT EXISTS timescaledb;

# Exit psql
\q
```

## Step 3: Environment Configuration

Create a `.env` file in the project root with the following variables:

```env
# Database Configuration
DATABASE_URL=postgresql://postgres:postgres@localhost:5432/eventa

# Environment
ENVIRONMENT=development
LOG_LEVEL=INFO

# Authentication (OIDC)
OIDC_DOMAIN=your-oidc-domain.com
OIDC_API_AUDIENCE=https://your-api-audience
OIDC_ISSUER=https://your-oidc-domain.com/
OIDC_ALGORITHMS=RS256

# Identies Integration (for authentication)
IDENTIES_HOST=http://localhost:8001

# Vaulta Integration (for secrets management)
VAULTA_API_URL=http://localhost:8004

# Optional: Disable Auth (for development only)
DISABLE_AUTH=false

# Optional: Redis Configuration
REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_NAMESPACE=llama_index

# Optional: NATS Configuration
NATS_ENABLED=false
NATS_URL=nats://localhost:4222
NATS_QUEUE=eventa_worker_all
NATS_SUBJECTS=com.mylinden.>
NATS_STREAM_NAME=EVT_LINDEN

# Optional: OpenTelemetry
OTEL_ENABLED=false
OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318
```

## Step 4: Run Database Migrations

```bash
# Activate the Poetry environment
poetry shell

# Run migrations
alembic upgrade head
```

## Step 5: Start the Server

```bash
# Development mode with auto-reload
poetry run dev
```

The API will be available at `http://localhost:8000` by default.

## Step 6: Verify Installation

Test that the service is running:

```bash
# Health check
curl http://localhost:8000/

# Expected response: {"message": "Hey, It is me Goku"}
```

## Next Steps

- Review the [Architecture](architecture.md) documentation to understand the system design
- Configure your OIDC provider settings
- Set up Identies integration for authentication
- Configure Vaulta for secrets management (if needed)
- Set up NATS for event streaming (optional)
- Start sending events to Eventa via the API

## Troubleshooting

### Database Connection Issues

- Verify PostgreSQL is running: `pg_isready`
- Check database credentials in `.env`
- Ensure the database exists: `psql -U postgres -l | grep eventa`
- Verify the database URL format is `postgresql://` (not `TimescaleDB://`)

### Migration Errors

- Ensure you're using the correct database URL
- Check that all previous migrations have been applied
- Review Alembic logs for specific errors
- Verify PostgreSQL version is 12 or higher

### Authentication Issues

- Verify OIDC configuration matches your provider
- Check that `DISABLE_AUTH=false` in production
- Ensure Identies service is accessible at the configured `IDENTIES_HOST`
- Verify `IDENTIES_HOST` is set correctly in your `.env` file

# DataStream API Documentation

## Overview

DataStream provides a RESTful HTTP API for managing CDC (Change Data Capture) tasks.

## Base URL

```
http://localhost:8300
```

## Authentication

Currently, the API does not require authentication. In production, consider adding:
- API key authentication
- OAuth 2.0 / JWT tokens
- mTLS for service-to-service communication

## Content Types

All API requests and responses use JSON:

```
Content-Type: application/json
```

## Quick Reference

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/health` | Health check |
| GET | `/api/v1/tasks` | List all tasks |
| POST | `/api/v1/tasks` | Create task |
| GET | `/api/v1/tasks/{id}` | Get task details |
| DELETE | `/api/v1/tasks/{id}` | Delete task |
| POST | `/api/v1/tasks/{id}/start` | Start task |
| POST | `/api/v1/tasks/{id}/stop` | Stop task |
| POST | `/api/v1/tasks/{id}/pause` | Pause task |
| POST | `/api/v1/tasks/{id}/resume` | Resume task |
| GET | `/api/v1/tasks/{id}/position` | Get position |
| PUT | `/api/v1/tasks/{id}/position` | Set position |
| GET | `/api/v1/nodes` | List nodes |
| GET | `/metrics` | Prometheus metrics |

## Examples

### Create a MySQL to Kafka task

```bash
curl -X POST http://localhost:8300/api/v1/tasks \
  -H "Content-Type: application/json" \
  -d '{
    "id": "mysql-to-kafka",
    "name": "Sync MySQL to Kafka",
    "config": {
      "sourceType": "mysql",
      "sinkType": "kafka",
      "batchSize": 1000
    }
  }'
```

### Start a task

```bash
curl -X POST http://localhost:8300/api/v1/tasks/mysql-to-kafka/start
```

### Get task position

```bash
curl http://localhost:8300/api/v1/tasks/mysql-to-kafka/position
```

### List all nodes

```bash
curl http://localhost:8300/api/v1/nodes
```

## Error Responses

All error responses follow this format:

```json
{
  "error": "Error message",
  "code": "500"
}
```

## OpenAPI Specification

The full OpenAPI 3.0 specification is available at `openapi.yaml`.

To view the interactive API documentation:

1. Use [Swagger UI](https://swagger.io/tools/swagger-ui/)
2. Import the `openapi.yaml` file
3. Explore and test the API endpoints

## CLI Usage

The `datastream-ctl` CLI provides a convenient interface:

```bash
# List tasks
datastream-ctl task list

# Create task
datastream-ctl task create my-task "My Sync Task" --config task.toml

# Start task
datastream-ctl task start my-task

# Stop task
datastream-ctl task stop my-task

# Get task details
datastream-ctl task get my-task

# Delete task
datastream-ctl task delete my-task
```

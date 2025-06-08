# Data Forge API Endpoints

This document describes all available API endpoints for the Data Forge platform.

## Base URL
```
http://localhost:8080/api/v1
```

## Available Schemas
- `fields_aliases` - Field prices data
- `well_production` - Well production data

---

## 📋 GET /schemas
Get list of available schemas/tables

**Response:**
```json
[
  {
    "name": "well_production",
    "description": "Schema for well production data.",
    "table_name": "well_production",
    "properties": [
      {"name": "field_code", "type": "integer", "required": true},
      {"name": "field_name", "type": "string", "required": false}
    ]
  }
]
```

---

## 📖 READ ENDPOINTS (High Performance)

### GET /records/{schema_name}
Get paginated records with filtering and sorting

**Parameters:**
- `page` (int): Page number (default: 1, min: 1)
- `size` (int): Records per page (1-1000, default: 10)
- `filters` (string): JSON array of filters
- `sort` (string): JSON array of sort specifications

**Example:**
```bash
curl "http://localhost:8080/api/v1/records/well_production?page=1&size=100&filters=[{\"field\":\"field_code\",\"operator\":\"eq\",\"value\":\"123\"}]&sort=[{\"field\":\"created_at\",\"order\":\"desc\"}]"
```

**Filter Operators:**
- `eq`, `ne`, `gt`, `gte`, `lt`, `lte`, `in`, `like`, `ilike`, `is_null`, `is_not_null`

**Response:**
```json
{
  "success": true,
  "message": "Successfully retrieved 10 records from well_production",
  "schema_name": "well_production",
  "data": {
    "items": [
      {
        "id": "uuid",
        "schema_name": "well_production",
        "data": {...},
        "created_at": "2024-01-15T10:30:00Z",
        "version": 1
      }
    ],
    "total": 1000,
    "page": 1,
    "size": 10,
    "has_next": true,
    "has_previous": false
  },
  "execution_time_ms": 25.3
}
```

### GET /records/{schema_name}/stream
🚀 **High-Performance Streaming** for large datasets

**Parameters:**
- `filters` (string): JSON array of filters
- `sort` (string): JSON array of sort specifications  
- `limit` (int): Max records to stream (1-10000)

**Example:**
```bash
curl "http://localhost:8080/api/v1/records/well_production/stream?limit=5000&filters=[{\"field\":\"field_code\",\"operator\":\"gt\",\"value\":\"100\"}]"
```

**Response:** NDJSON (Newline Delimited JSON) for efficient streaming
```
{"id": "uuid1", "schema_name": "well_production", "data": {...}, "created_at": "2024-01-15T10:30:00Z", "version": 1}
{"id": "uuid2", "schema_name": "well_production", "data": {...}, "created_at": "2024-01-15T10:30:00Z", "version": 1}
```

### GET /records/{schema_name}/count
Get record count with optional filtering

**Parameters:**
- `filters` (string): JSON array of filters

**Example:**
```bash
curl "http://localhost:8080/api/v1/records/well_production/count?filters=[{\"field\":\"field_code\",\"operator\":\"gt\",\"value\":\"100\"}]"
```

**Response:**
```json
{
  "success": true,
  "message": "Successfully counted records in well_production",
  "schema_name": "well_production",
  "count": 1500,
  "execution_time_ms": 12.1
}
```

### GET /records/{schema_name}/{record_id}
Get specific record by ID

**Example:**
```bash
curl "http://localhost:8080/api/v1/records/well_production/550e8400-e29b-41d4-a716-446655440000"
```

**Response:**
```json
{
  "id": "550e8400-e29b-41d4-a716-446655440000",
  "schema_name": "well_production",
  "data": {...},
  "created_at": "2024-01-15T10:30:00Z",
  "version": 1
}
```

---

## ✍️ WRITE ENDPOINTS (Bulk Operations)

### POST /records
Create single record

**Request:**
```json
{
  "schema_name": "well_production",
  "data": {
    "field_code": 123,
    "field_name": "North Field",
    "well_code": 456,
    "oil_production_kbd": 1500.5
  }
}
```

**Response:**
```json
{
  "success": true,
  "message": "Record created successfully in well_production",
  "record": {
    "id": "550e8400-e29b-41d4-a716-446655440000",
    "schema_name": "well_production",
    "data": {...},
    "created_at": "2024-01-15T10:30:00Z",
    "version": 1
  }
}
```

### POST /records/bulk
🚀 **High-Performance Bulk Creation** for fast operations

**Request:**
```json
{
  "schema_name": "well_production",
  "data": [
    {
      "field_code": 123,
      "field_name": "North Field",
      "oil_production_kbd": 1500.5
    },
    {
      "field_code": 124,
      "field_name": "South Field", 
      "oil_production_kbd": 1200.3
    }
  ]
}
```

**Response:**
```json
{
  "success": true,
  "message": "Successfully created 2 records in well_production",
  "records_created": 2,
  "records": [
    {
      "id": "uuid1",
      "schema_name": "well_production",
      "data": {...},
      "created_at": "2024-01-15T10:30:00Z",
      "version": 1
    },
    {
      "id": "uuid2",
      "schema_name": "well_production",
      "data": {...},
      "created_at": "2024-01-15T10:30:00Z",
      "version": 1
    }
  ]
}
```

---

## 🏗️ Architecture Features

### High Performance
- **DuckDB Backend**: Optimized for analytical workloads
- **Connection Pooling**: Efficient database connections
- **Streaming Responses**: Handle large datasets efficiently
- **Bulk Operations**: Fast batch inserts using `executemany()`

### Following CQRS Pattern
- **Command Handlers**: `CreateDataRecordUseCase`, `CreateBulkDataRecordsUseCase`
- **Query Handlers**: `QueryDataRecordsUseCase`, `StreamDataRecordsUseCase`, `CountDataRecordsUseCase`
- **Separation of Concerns**: Read and write operations optimized independently

### Hexagonal Architecture
- **Domain Layer**: Pure business logic (`DataRecord`, `Schema`)
- **Application Layer**: Use cases and DTOs
- **Infrastructure Layer**: FastAPI routes, DuckDB repositories
- **Dependency Injection**: Clean separation and testability

### Data Validation
- **Pydantic Models**: Strong typing and validation
- **Schema-Driven**: Dynamic validation based on schema definitions
- **Error Handling**: Comprehensive exception handling and logging

---

## 🚀 Performance Tips

1. **Use Bulk Endpoints**: For inserting multiple records, always use `/records/bulk`
2. **Use Streaming**: For large result sets, use `/records/{schema}/stream`
3. **Optimize Filters**: Use indexed fields (`id`, `created_at`) for better performance
4. **Pagination**: Keep page sizes reasonable (100-1000 records)
5. **Count First**: Use `/count` endpoint to estimate result size before querying

---

## Error Responses

All endpoints return consistent error responses:

```json
{
  "detail": "Schema 'invalid_schema' not found"
}
```

**HTTP Status Codes:**
- `200`: Success
- `201`: Created
- `400`: Bad Request (validation error)
- `404`: Not Found (schema/record not found)
- `500`: Internal Server Error 
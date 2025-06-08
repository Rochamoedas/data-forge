# Data Forge - Dynamic Data Platform

## High-Level Project Description

This document presents an overview of the architecture, design principles, and strategic decisions behind our data platform project, designed to manage and interact with a wide variety of "tables" (or *datasets*) in a generic and efficient way.

---

### Overview and Purpose

Our goal is to build a **flexible and scalable data platform** that supports ingestion and querying across different datasets. The key concept is **genericity**: instead of creating a specific API and database schema for each data type (users, products, sales, etc.), the platform will be **schema-driven**. This means the structure of each "table" will be defined by metadata (the schema), and the application will dynamically adapt to that definition to perform read/write operations.

---

### Key Decisions and Principles

1.  **FastAPI as Web Framework:** Chosen for its high performance, ease of use, strong typing (via Pydantic), automatic documentation (Swagger/OpenAPI), and asynchronous support—ideal for efficient I/O operations.
2.  **DuckDB as Database:** For the MVP, we use DuckDB due to its embeddable nature, strong analytical performance for tabular data, and flexibility in handling dynamic schemas (creating tables "on-the-fly" or using "schema-on-read"). This simplifies early persistence management.
3.  **Schemas Defined in File:** For the MVP, schema definitions will be stored in local Python files (e.g., `schemas_description.py`). This reduces complexity during early development by avoiding the need for a schema management API, accelerating delivery. Complex schema management and the us of AI to auto-improve the project will be considered in another phase, not in the MVP.
4.  **Focus on Genericity and DRY (Don't Repeat Yourself):** The architecture is designed so that adding a new dataset (a new "table") requires **minimal coding effort**. Data handling logic is generic, driven by schema definitions rather than hardcoded data types.
5.  **Code Quality and Maintainability:** From the beginning, the project is structured to support collaboration, code clarity, and future evolution.

---

### Architecture and Design Patterns

The project architecture is built around the following principles and patterns:

1.  **Hexagonal Architecture (Ports & Adapters):**
    * **Core Principle:** The application core (business logic) is isolated and independent from external technologies (web frameworks, databases).
    * **Structure:** The project is divided into distinct layers:
        * **Domain (`app/domain/`):** Contains **pure business logic**, entities (`Schema`, `DataRecord`), and interface definitions (Ports) to interact with the external world (Repositories). This is the "why" of the system.
        * **Application (`app/application/`):** Defines **use cases**, orchestrating operations from the domain. This is the "what" the system does.
        * **Infrastructure (`app/infrastructure/`):** Contains **concrete implementations (Adapters)** of domain interfaces, handling technical details like FastAPI, DuckDB, and reading schemas from files. This is the "how" the system does it.
        * **Container (`app/container/`):** Responsible for assembling the application by connecting infrastructure implementations to domain interfaces via Dependency Injection.

2.  **Domain-Driven Design (DDD):**
    * **Core Principle:** Software complexity is managed by aligning code to a rich domain model.
    * **Application:** Reflected in explicit entities like `Schema` and `DataRecord`, which are the conceptual building blocks of our data system. **Domain Services** encapsulate business logic involving multiple entities.

3.  **Command Query Responsibility Segregation (CQRS):**
    * **Core Principle:** Separates state-changing operations (Commands) from read-only operations (Queries).
    * **Application:** Each use case in the Application layer is typically a Command (e.g., `create_data_record`) or a Query (e.g., `get_data_record`). This allows independent optimization of read/write paths in the future.

4.  **Dependency Injection (DI):**
    * **Core Principle:** Classes receive their dependencies from an external source (the DI Container) instead of creating them.
    * **Benefits:** Improves testability, maintainability, and flexibility (e.g., switching from DuckDB to PostgreSQL without changing the Domain or Application layers).

5.  **Pydantic for Validation and Modeling:**
    * **Core Principle:** Leverages static typing and data models for robust input/output validation and entity modeling.
    * **Application:** Widely used in DTOs (Data Transfer Objects) in the Application layer and FastAPI request/response models. Crucial for `Schema` and `DataRecord` entities to ensure metadata and data integrity.

---

### High-Level Operation Flow

1.  **Initialization:**
    * The FastAPI application starts (`main.py`).
    * Configuration is loaded (`config/settings.py`).
    * The **DI Container** (`container/container.py`) initializes and configures all dependencies, including DuckDB connection, file-based schema repository, and the generic data repository.
    * `FileSchemaRepository` loads schema definitions from `schemas_description.py` into memory.
    * The DuckDB `SchemaManager` is injected and can be used to **ensure tables corresponding to defined schemas exist in DuckDB** during startup or on the first write operation.

2.  **Data Request (e.g., Create a Record):**
    * An HTTP POST request is made to the FastAPI endpoint `/api/{schema_name}/data`.
    * FastAPI validates the request payload using a **Pydantic DTO** (`CreateDataRecordRequest`).
    * A **FastAPI dependency** (`common.py`) injects the `create_data_record` use case.
    * The use case requests the schema definition from `ISchemaRepository` (implemented by `FileSchemaRepository`) using the request's `schema_name`.
    * With the `Schema` in hand, the use case invokes `IDataRepository` (implemented by `DuckDBDataRepository`) to store the new data record.
    * `DuckDBDataRepository` uses the schema to dynamically build the SQL INSERT query and persist the data in DuckDB.
    * The use case returns a `DataRecord` (domain entity), which is mapped to a `DataRecordResponse` (DTO) and returned to the client by FastAPI.
	* Focus on performance and reliability.

---

## Folder Structure

```
react-fast-V12/
├── app/
│   ├── application/
│   ├── config/
│   ├── container/
│   ├── domain/
│   ├── infrastructure/
│   └── main.py
├── docs/
└── requirements.txt
```

---

## Main Layers and Architecture Patterns

The project follows a **Hexagonal Architecture (Ports & Adapters)** that ensures clear separation of concerns and isolates core business logic from external technologies.

### `app/domain/` (Business Core - Domain Layer)

* **Purpose:** Contains **pure business logic**, rules, behaviors, and core entities of the system. Completely agnostic to frameworks and persistence technologies.
* **Patterns Used:**
    * **Domain-Driven Design (DDD)**: Entities (`Schema`, `DataRecord`), **Repository Interfaces** (Ports), and **Domain Services** (`data_management.py`).
    * **SOLID Principles** and **Object Calisthenics** compliance for clean modeling.

### `app/application/` (Use Case Orchestrator - Application Layer)

* **Purpose:** Defines the **use cases** of the system. Orchestrates domain operations to fulfill user requirements. Contains no complex business rules itself.
* **Patterns Used:**
    * **CQRS (Command Query Responsibility Segregation)** using separate handlers for commands and queries (`create_data_record.py`, `get_data_record.py`).
    * **DTOs (Data Transfer Objects)** to map external inputs/outputs to internal entities.

### `app/infrastructure/` (Adapters - Infrastructure Layer)

* **Purpose:** Concrete implementations of interfaces declared in the domain layer. Connects business logic to technologies such as DuckDB, FastAPI, file systems, etc.
* **Patterns Used:**
    * Implements **Adapters** in the hexagonal architecture.
    * Uses **Dependency Injection (DI)** to bind implementations to use cases.
    * Includes:
        * `persistence/`: Concrete repositories and `schema_manager` for dynamic DDL.
        * `web/`: FastAPI endpoints and dependencies.
        * `metadata/`: Static schema declarations.

### `app/container/` (Dependency Injection)

* **Purpose:** Centralizes dependency configuration and wiring of components.
* **Patterns Used:**
    * **Inversion of Control (IoC)** and **Dependency Injection (DI)** using a modular container system.

### `app/config/` and `app/main.py`

* **Purpose:**
    * `settings.py`: Environment-based configuration management.
    * `main.py`: Initializes the FastAPI application, mounts routes, and loads container dependencies.

---

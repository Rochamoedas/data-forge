## 5. Supporting Evidence from Code

This section consolidates observations about specific code areas that underpin the hypotheses detailed in Section 4. The most relevant code snippets have already been included directly within each hypothesis's description in Section 4 for contextual clarity and to avoid redundancy. Below is a summary of key files and their direct relation to the identified potential design and performance issues:

-   **`app/infrastructure/persistence/duckdb/connection_pool.py`**:
    -   This file is central to how the application interacts with DuckDB. It illustrates the current connection management strategy, including how connections are created (`_create_connection`) and acquired (`acquire`).
    -   Critically, it shows where performance-related settings for DuckDB (e.g., `SET memory_limit = '4GB'`, `SET threads = 4;`) are hardcoded, potentially overriding configurations intended in `app/config/settings.py`.
    -   The synchronous nature of `duckdb.connect()` and connection operations within an async pool without explicit thread offloading is also evident here.
    -   Relevant to: *Hypothesis 1 (Database Scalability for Writes/Concurrency with DuckDB)* and *Hypothesis 4 (Suboptimal DuckDB Configuration and Management)*.

-   **`app/infrastructure/persistence/repositories/duckdb_data_repository.py`**:
    -   This repository implementation contains the `stream_query_results` method, which uses `fetchall()`. This is a key piece of evidence for potential memory inefficiency when streaming large query results, as the entire result set is loaded into memory.
    -   The direct use of `conn.execute()` and `conn.executemany()` for write operations is shown here, providing insight into how data is persisted and how write operations might contend or block.
    -   Relevant to: *Hypothesis 1 (Database Scalability for Writes/Concurrency with DuckDB)* and *Hypothesis 2 (Memory Inefficiencies in Data Handling)*.

-   **`app/application/use_cases/create_bulk_data_records.py`**:
    -   The `execute` method in this use case demonstrates the pattern of validating all data items in a bulk request and creating a list of `DataRecord` domain objects for the entire batch *before* submitting these to the database repository.
    -   This supports the concern about potential high memory usage for large bulk operations.
    -   Relevant to: *Hypothesis 2 (Memory Inefficiencies in Data Handling)*.

-   **`app/infrastructure/persistence/duckdb/schema_manager.py`**:
    -   The `ensure_table_exists` method within this file explicitly shows the current indexing strategy. It details the creation of indexes only for `id` and `created_at` columns.
    -   The absence of logic to create indexes on other schema-defined properties is key evidence for the hypothesis regarding insufficient indexing.
    -   Relevant to: *Hypothesis 3 (Insufficient Indexing Strategy for Dynamic Schemas)*.

-   **`app/domain/entities/schema.py`**:
    -   The definition of the `SchemaProperty` class within this core domain entity file includes the `db_type` attribute. This attribute is intended to hold database-specific type names (e.g., "VARCHAR", "BIGINT").
    -   This directly illustrates the coupling of the domain model to database-specific details.
    -   Relevant to: *Hypothesis 5 (Type Coupling with `db_type` in Schema)*.

-   **`app/config/settings.py`**:
    -   This file contains the `DUCKDB_PERFORMANCE_CONFIG` dictionary, where application-level configurations for DuckDB (like memory limits and thread counts) are intended to be defined.
    -   The fact that these settings are, in part, overridden or ignored by hardcoded values in the `connection_pool.py` is crucial for understanding configuration discrepancies.
    -   Relevant to: *Hypothesis 4 (Suboptimal DuckDB Configuration and Management)*.

Detailed code snippets supporting these points are co-located with their respective hypotheses in Section 4 for better readability and immediate context. This section serves to reaffirm that the hypotheses are derived from and can be traced back to specific implementations within the existing codebase.

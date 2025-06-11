# Project Folder Structure

```
react-fast-V12/
├── api_bench.py
├── app.bat
├── apply_diff.md
├── folder_tree.md
├── io_bench.py
├── main.py
├── no_output
├── pyarrow_docs.md
├── pyproject.toml
├── README.md
├── requirements.in
├── requirements.lock
├── requirements.txt
├── simplify_rev2.md
├── sprint_3.md
├── test_read_operation.py
├── uv.lock
├── verify_data.py
│
├── app/
│   ├── main.py
│   │
│   ├── application/
│   │   ├── commands/
│   │   │   └── bulk_data_commands.py
│   │   ├── command_handlers/
│   │   │   └── bulk_data_command_handlers.py
│   │   ├── dto/
│   │   │   ├── create_data_dto.py
│   │   │   ├── data_dto.py
│   │   │   ├── query_dto.py
│   │   │   ├── query_request_dto.py
│   │   │   └── schema_dto.py
│   │   └── use_cases/
│   │       ├── create_bulk_data_records.py
│   │       ├── create_data_record.py
│   │       ├── create_ultra_fast_bulk_data.py
│   │       ├── get_data_record.py
│   │       └── query_data_records.py
│   │
│   ├── config/
│   │   ├── api_limits.py
│   │   ├── logging_config.py
│   │   └── settings.py
│   │
│   ├── container/
│   │   └── container.py
│   │
│   ├── domain/
│   │   ├── entities/
│   │   │   ├── data_record.py
│   │   │   └── schema.py
│   │   ├── exceptions.py
│   │   ├── interfaces/
│   │   │   └── query_builder.py
│   │   ├── repositories/
│   │   │   ├── data_repository.py
│   │   │   └── schema_repository.py
│   │   └── services/
│   │       └── data_management.py
│   │
│   └── infrastructure/
│       ├── metadata/
│       │   └── schemas_description.py
│       ├── persistence/
│       │   ├── arrow_bulk_operations.py
│       │   ├── high_performance_data_processor.py
│       │   │
│       │   ├── duckdb/
│       │   │   ├── connection_pool.py
│       │   │   ├── query_builder.py
│       │   │   └── schema_manager.py
│       │   └── repositories/
│       │       ├── duckdb_data_repository.py
│       │       └── file_schema_repository.py
│       │
│       └── web/
│           ├── arrow.py
│           │
│           ├── dependencies/
│           │   ├── common.py
│           │   └── profiling.py
│           └── routers/
│               └── arrow_performance_data.py
│
├── data/
│   └── data.duckdb
│
├── docs/
│   ├── API_ENDPOINTS.md
│   ├── code_review_prompt.md
│   └── RUNME.md
│
├── logs/
│   └── app.log
│
└── 
```
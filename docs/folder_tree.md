# Project Folder Structure

```
react-fast-V12/
├── .env
├── .gitignore
├── app/
│   ├── main.py
│   ├── application/
│   │   ├── dto/
│   │   │   ├── create_data_dto.py
│   │   │   ├── data_dto.py
│   │   │   ├── query_dto.py
│   │   │   ├── query_request_dto.py
│   │   │   └── schema_dto.py
│   │   └── use_cases/
│   │       ├── create_bulk_data_records.py
│   │       ├── create_data_record.py
│   │       ├── get_data_record.py
│   │       └── query_data_records.py
│   ├── config/
│   │   ├── api_limits.py
│   │   ├── logging_config.py
│   │   └── settings.py
│   ├── container/
│   │   └── container.py
│   ├── domain/
│   │   ├── entities/
│   │   │   ├── data_record.py
│   │   │   └── schema.py
│   │   ├── exceptions.py
│   │   ├── repositories/
│   │   │   ├── data_repository.py
│   │   │   └── schema_repository.py
│   │   └── services/
│   │       └── data_management.py
│   ├── infrastructure/
│   │   ├── metadata/
│   │   │   └── schemas_description.py
│   │   ├── persistence/
│   │   │   ├── duckdb/
│   │   │   │   ├── connection_pool.py
│   │   │   │   ├── query_builder.py
│   │   │   │   └── schema_manager.py
│   │   │   ├── mappers/
│   │   │   └── repositories/
│   │   │       ├── duckdb_data_repository.py
│   │   │       └── file_schema_repository.py
│   │   └── web/
│   │       ├── dependencies/
│   │       │   ├── common.py
│   │       │   ├── profiling.py
│   │       │   └── timing.py
│   │       └── routers/
│   │           └── data.py
├── data/
│   └── data.duckdb
├── docs/
│   ├── API_ENDPOINTS.md
│   ├── development_plan.md
│   ├── folder_tree.md
│   ├── README.md
│   ├── sprint_1.md
│   └── sprint_2.md
├── external/
├── frontend/
├── logs/
├── tests/
├── complete_tests.py
├── performance_tests.py
├── requirements.txt
├── requirements.in
├── requirements.lock
└── uv.lock
```
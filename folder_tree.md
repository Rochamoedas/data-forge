# Project Folder Structure

```
react-fast-V12/
├── .cursorignore
├── .env
├── .git/
├── .gitignore
├── .venv/
├── .vscode/
├── app/
│   ├── main.py
│   ├── application/
│   │   ├── command_handlers/
│   │   │   └── bulk_data_command_handlers.py
│   │   ├── commands/
│   │   │   └── bulk_data_commands.py
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
│   ├── config/
│   │   ├── api_limits.py
│   │   ├── logging_config.py
│   │   └── settings.py
│   ├── container/
│   │   └── container.py
│   ├── domain/
│   │   ├── exceptions.py
│   │   ├── entities/
│   │   │   ├── data_record.py
│   │   │   └── schema.py
│   │   ├── interfaces/
│   │   │   └── query_builder.py
│   │   ├── models/
│   │   ├── repositories/
│   │   │   ├── data_repository.py
│   │   │   └── schema_repository.py
│   │   └── services/
│   │       └── data_management.py
│   ├── infrastructure/
│   │   ├── metadata/
│   │   │   └── schemas_description.py
│   │   ├── persistence/
│   │   │   ├── arrow_bulk_operations.py
│   │   │   ├── high_performance_data_processor.py
│   │   │   ├── duckdb/
│   │   │   │   ├── connection_pool.py
│   │   │   │   ├── query_builder.py
│   │   │   │   └── schema_manager.py
│   │   │   └── repositories/
│   │   │       ├── duckdb_data_repository.py
│   │   │       └── file_schema_repository.py
│   │   ├── repositories/
│   │   └── web/
│   │       ├── arrow.py
│   │       ├── dependencies/
│   │       │   ├── common.py
│   │       │   └── profiling.py
│   │       └── routers/
│   │           └── arrow_performance_data.py
├── app.bat
├── app2.bat
├── data/
│   └── data.duckdb
├── docs/
│   ├── code_review_prompt.md
│   ├── pyarrow_docs.md
│   ├── RUNME.md
│   └── sprint_3.md
├── external/
│   ├── mocked_response.json
│   ├── mocked_response_100K-4.json
│   ├── mocked_response_10K.json
│   └── mocked_response_duplicates.json
├── fenv/
│   ├── Include/
│   ├── Lib/
│   │   └── site-packages/
│   └── Scripts/
├── folder_tree.md
├── frontend/
│   ├── .gitkeep
│   └── app.py
├── logs/
│   └── app.log
├── main.py
├── pyproject.toml
├── README.md
├── requirements.in
├── requirements.lock
├── requirements.txt
├── tests/
│   ├── api_bench.py
│   ├── deduplicate_json.py
│   ├── duplicate_json.py
│   ├── io_bench_profiling.py
│   ├── io_bench_standard.py
│   ├── io_bench.py
│   ├── load_data.py
│   ├── verify_data.py
│   └── verify_duplicates.py
├── uv.lock
└── workflows/
```
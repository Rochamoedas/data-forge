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
│   │           ├── data.py
│   │           └── high_performance_data.py
├── data/
│   └── data.duckdb
├── docs/
│   ├── API_ENDPOINTS.md
│   ├── code_review_prompt.md
│   ├── HIGH_PERFORMANCE_DATA_GUIDE.md
│   ├── RUNME.md
│   ├── folder_tree.md
│   ├── README.md
│   ├── sprint_1.md
│   └── sprint_2.md
├── external/
│   ├── mocked_response_100K-4.json
│   ├── mocked_response_10K.json
│   ├── mocked_response_duplicates.json
│   └── mocked_response.json
├── frontend/
│   ├── app.py
├── logs/
│   └── app.log
├── tests/
│   ├── complete_tests.py
│   ├── conftest.py
│   ├── deduplicate_json.py
│   ├── duplicate_json.py
│   ├── load_data.py
│   ├── main.py
│   ├── performance_tests.py
│   ├── pyproject.toml
│   ├── pytest.ini
│   ├── requirements.in
│   ├── requirements.lock
│   ├── test_arrow_extension.py
│   ├── test_composite_keys.py
│   ├── test_high_performance.py
│   ├── test_performance_monitoring.py
│   ├── test_query_benchmarks.py
│   ├── test_runner.py
│   ├── test_simple_large_pages.py
│   ├── test_simple_streaming.py
│   ├── test_streaming_performance.py
│   ├── test_streaming_simple.py
│   ├── trequirements.txt
│   ├── verify_duplicates.py
│   ├── test_config/
│   ├── test_domain/
│   └── test_infrastructure/
├── app.bat
├── complete_tests.py
├── io_bench.py
├── main.py
├── performance_tests.py
├── pyarrow_docs.md
├── pyproject.toml
├── README.md
├── requirements.in
├── requirements.lock
├── requirements.txt
├── uv.lock
└── no_output
```
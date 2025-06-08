# Project Folder Structure

```
react-fast-V12/
├── .env
├── .gitignore
├── app/
│   ├── main.py
│   ├── application/
│   │   ├── dto/
│   │   │   ├── data_dto.py
│   │   │   └── schema_dto.py
│   │   └── use_cases/
│   │       ├── create_data_record.py
│   │       └── get_data_record.py
│   ├── config/
│   │   ├── logging_config.py
│   │   └── settings.py
│   ├── container/
│   │   └── container.py
│   ├── domain/
│   │   ├── entities/
│   │   │   ├── data_record.py
│   │   │   └── schema.py
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
│   │   │   │   ├── connection.py
│   │   │   │   ├── connection_pool.py
│   │   │   │   └── schema_manager.py
│   │   │   ├── mappers/
│   │   │   │   └── generic_mapper.py
│   │   │   └── repositories/
│   │   │       ├── duckdb_data_repository.py
│   │   │       └── file_schema_repository.py
│   │   └── web/
│   │       ├── dependencies/
│   │       │   └── common.py
│   │       └── routers/
│   │           └── data.py
├── docs/
│   ├── development_plan.md
│   ├── folder_tree.md
│   ├── README.md
│   ├── sprint_1.md
│   └── sprint_2.md
├── requirements.txt
```
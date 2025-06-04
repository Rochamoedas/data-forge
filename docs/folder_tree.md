```
REACT-FAST-V12/
├── .env
├── .gitignore
├── app/
│   ├── __init__.py
│   ├── application/
│   │   ├── __init__.py
│   │   ├── dto/
│   │   │   ├── __init__.py
│   │   │   ├── data_dto.py
│   │   │   └── schema_dto.py
│   │   └── use_cases/
│   │       ├── __init__.py
│   │       ├── create_data_record.py
│   │       └── get_data_record.py
│   ├── config/
│   │   ├── __init__.py
│   │   └── settings.py
│   ├── container/
│   │   ├── __init__.py
│   │   └── container.py
│   ├── domain/
│   │   ├── __init__.py
│   │   ├── entities/
│   │   │   ├── __init__.py
│   │   │   ├── data_record.py
│   │   │   └── schema.py
│   │   ├── repositories/
│   │   │   ├── __init__.py
│   │   │   ├── data_repository.py
│   │   │   └── schema_repository.py
│   │   └── services/
│   │       ├── __init__.py
│   │       └── data_management.py
│   ├── infrastructure/
│   │   ├── __init__.py
│   │   ├── metadata/
│   │   │   ├── __init__.py
│   │   │   └── schemas_description.py
│   │   ├── persistence/
│   │   │   ├── __init__.py
│   │   │   ├── duckdb/
│   │   │   │   ├── __init__.py
│   │   │   │   ├── connection.py
│   │   │   │   └── schema_manager.py
│   │   │   ├── mappers/
│   │   │   │   ├── __init__.py
│   │   │   │   └── generic_mapper.py
│   │   │   └── repositories/
│   │   │       ├── __init__.py
│   │   │       ├── duckdb_data_repository.py
│   │   │       └── file_schema_repository.py
│   │   └── web/
│   │       ├── __init__.py
│   │       ├── dependencies/
│   │       │   ├── __init__.py
│   │       │   └── common.py
│   │       └── routers/
│   │           ├── __init__.py
│   │           └── data.py
│   └── main.py
├── docs/
│   ├── development_plan.md
│   ├── folder_tree.md
│   ├── README.md
│   ├── sprint_1.md
│   └── sprint_2.md
└── requirements.txt
```
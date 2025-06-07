from app.domain.entities.schema import Schema, SchemaField

# Field Prices Schema
fields_prices_schema = Schema(
    name="fields_prices",
    fields=[
        SchemaField(name="id", type="UUID"),  # UUID for primary key matching
        SchemaField(name="field_code", type="INTEGER"),
        SchemaField(name="field_name", type="STRING"),
        SchemaField(name="production_period", type="TIMESTAMP"),
        SchemaField(name="price_brl_m3", type="DOUBLE"),
        SchemaField(name="price_brl_mmcf", type="DOUBLE")
    ]
)

# Well Production Schema
well_production_schema = Schema(
    name="well_production",
    fields=[
        SchemaField(name="field_code", type="INTEGER"),
        SchemaField(name="field_name", type="STRING"),
        SchemaField(name="well_code", type="INTEGER"),
        SchemaField(name="well_reference", type="STRING"),
        SchemaField(name="well_name", type="STRING"),
        SchemaField(name="production_period", type="TIMESTAMP"),
        SchemaField(name="days_on_production", type="INTEGER"),
        SchemaField(name="oil_production_kbd", type="DOUBLE"),
        SchemaField(name="gas_production_mmcfd", type="DOUBLE"),
        SchemaField(name="liquids_production_kbd", type="DOUBLE"),
        SchemaField(name="water_production_kbd", type="DOUBLE"),
        SchemaField(name="data_source", type="STRING"),
        SchemaField(name="source_data", type="STRING"),
        SchemaField(name="partition_0", type="STRING"),
        SchemaField(name="id", type="UUID")
    ]
)

# Registry of all available schemas
ALL_SCHEMAS = {
    "fields_prices": fields_prices_schema,
    "well_production": well_production_schema
}

# Helper function to get schema by name
def get_schema_by_name(name: str) -> Schema:
    """Get a schema by its name."""
    if name not in ALL_SCHEMAS:
        raise ValueError(f"Schema '{name}' not found. Available schemas: {list(ALL_SCHEMAS.keys())}")
    return ALL_SCHEMAS[name]

# Helper function to list available schemas
def list_available_schemas() -> list[str]:
    """Get a list of all available schema names."""
    return list(ALL_SCHEMAS.keys())
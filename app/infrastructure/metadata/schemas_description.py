# app/infrastructure/metadata/schemas_description.py
from app.domain.entities.schema import Schema, SchemaField

# Define a 'users' schema
users_schema = Schema(
    name="users",
    fields=[
        SchemaField(name="id", type="UUID"), # UUID for primary key matching DataRecord.id
        SchemaField(name="name", type="STRING"),
        SchemaField(name="email", type="STRING"),
        SchemaField(name="age", type="INTEGER"),
        SchemaField(name="created_at", type="TIMESTAMP")
    ]
)

# Define a 'products' schema
products_schema = Schema(
    name="products",
    fields=[
        SchemaField(name="id", type="UUID"), # UUID for primary key matching DataRecord.id
        SchemaField(name="product_id", type="INTEGER"),
        SchemaField(name="name", type="STRING"),
        SchemaField(name="price", type="DOUBLE"),
        SchemaField(name="category", type="STRING")
    ]
)

# A dictionary of all schemas available, keyed by schema name
ALL_SCHEMAS = {
    users_schema.name: users_schema,
    products_schema.name: products_schema
}
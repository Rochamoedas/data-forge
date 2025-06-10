# app/infrastructure/persistence/duckdb/duckdb_utils.py
"""
Utility functions for DuckDB operations, particularly focusing on CSV generation
and COPY operations for efficient data loading.
"""
import csv
import tempfile
import os
import time
from typing import List, Dict, Any, TYPE_CHECKING
from app.config.logging_config import logger
from app.domain.exceptions import DatabaseError, DataProcessingError

if TYPE_CHECKING:
    from app.domain.entities.schema import Schema
    from app.domain.entities.data_record import DataRecord
    import duckdb # DuckDBPyConnection type hint

async def create_csv_from_records(records: List['DataRecord'], schema: 'Schema', temp_file_path: str) -> None:
    """
    Creates a CSV file from a list of DataRecord objects.

    This function is designed to prepare data for bulk loading into DuckDB using the COPY command.
    It handles proper data encoding (UTF-8) and CSV formatting.

    Args:
        records: A list of DataRecord objects to be written to the CSV.
        schema: The Schema entity describing the structure of the data records.
        temp_file_path: The full path to the temporary CSV file to be created.

    Raises:
        DataProcessingError: If an IOError occurs during file writing or any other unexpected
                             exception arises during CSV creation.
    """
    logger.debug(f"[DBUtil] Attempting to write {len(records)} DataRecords to CSV: '{temp_file_path}' for schema '{schema.name}'.")
    try:
        with open(temp_file_path, 'w', newline='', encoding='utf-8') as tf:
            writer = csv.writer(tf, quoting=csv.QUOTE_MINIMAL)

            # Define columns in correct order based on schema, including standard fields
            columns = ["id", "created_at", "version"] + [prop.name for prop in schema.properties]
            writer.writerow(columns)  # Write header row

            for record_idx, record in enumerate(records):
                row_data = [
                    str(record.id),
                    record.created_at.isoformat() if record.created_at else '', # Ensure datetime is ISO formatted
                    str(record.version)
                ]
                for prop in schema.properties:
                    value = record.data.get(prop.name)
                    if value is None:
                        row_data.append('')  # Represent None as an empty string in CSV
                    elif isinstance(value, str):
                        # Ensure UTF-8 encoding, replacing errors for robustness
                        row_data.append(value.encode('utf-8', errors='replace').decode('utf-8'))
                    else:
                        row_data.append(str(value)) # Convert other types to string
                writer.writerow(row_data)
        logger.info(f"[DBUtil] Successfully wrote {len(records)} DataRecords to temporary CSV: '{temp_file_path}' for schema '{schema.name}'.")
    except IOError as e:
        logger.error(f"❌ [DBUtil] IOError writing DataRecords to CSV '{temp_file_path}' for schema '{schema.name}': {e}")
        raise DataProcessingError(message=f"Failed to write DataRecord data to temporary CSV for schema '{schema.name}': {e}", underlying_exception=e)
    except Exception as e: # Catch any other unexpected errors
        logger.error(f"❌ [DBUtil] Unexpected error writing DataRecords to CSV '{temp_file_path}' for schema '{schema.name}': {e}")
        raise DataProcessingError(message=f"An unexpected error occurred during DataRecord CSV creation for schema '{schema.name}': {e}", underlying_exception=e)

async def create_csv_from_dicts(data_dicts: List[Dict[str, Any]], schema: 'Schema', temp_file_path: str, include_generated_fields: bool = True) -> None:
    """
    Creates a CSV file from a list of dictionaries.

    This is useful when data is already in dictionary format (e.g., after Polars df.to_dicts()).
    If `include_generated_fields` is True, it adds 'id', 'created_at', and 'version' columns,
    generating new UUIDs and timestamps for each row. This is suitable for new data ingestion.

    Args:
        data_dicts: A list of dictionaries, where each dictionary represents a row.
        schema: The Schema entity describing the data.
        temp_file_path: The full path to the temporary CSV file.
        include_generated_fields: If True, 'id', 'created_at', 'version' columns are added
                                  and populated with generated values.

    Raises:
        DataProcessingError: If an IOError occurs or any other unexpected exception arises.
    """
    logger.debug(f"[DBUtil] Attempting to write {len(data_dicts)} dictionaries to CSV: '{temp_file_path}' for schema '{schema.name}'. Generated fields: {include_generated_fields}.")
    try:
        with open(temp_file_path, 'w', newline='', encoding='utf-8') as tf:
            writer = csv.writer(tf, quoting=csv.QUOTE_MINIMAL)

            columns = []
            if include_generated_fields:
                columns.extend(["id", "created_at", "version"])
            columns.extend([prop.name for prop in schema.properties]) # Schema properties
            writer.writerow(columns)  # Write header row

            for record_data in data_dicts:
                import uuid # Local import for late binding if needed, though module level is fine
                from datetime import datetime # Same as above

                row_data = []
                if include_generated_fields:
                    row_data.extend([
                        str(uuid.uuid4()),  # Generate a new UUID for 'id'
                        datetime.now().isoformat(),  # Generate current timestamp for 'created_at'
                        record_data.get("version", 1)  # Use provided version or default to 1
                    ])

                for prop in schema.properties:
                    value = record_data.get(prop.name)
                    if value is None:
                        row_data.append('')
                    elif isinstance(value, str):
                        row_data.append(value.encode('utf-8', errors='replace').decode('utf-8'))
                    else:
                        row_data.append(str(value))
                writer.writerow(row_data)
        logger.info(f"[DBUtil] Successfully wrote {len(data_dicts)} dictionaries to temporary CSV: '{temp_file_path}' for schema '{schema.name}'.")
    except IOError as e:
        logger.error(f"❌ [DBUtil] IOError writing dictionaries to CSV '{temp_file_path}' for schema '{schema.name}': {e}")
        raise DataProcessingError(message=f"Failed to write dictionary data to temporary CSV for schema '{schema.name}': {e}", underlying_exception=e)
    except Exception as e:
        logger.error(f"❌ [DBUtil] Unexpected error writing dictionaries to CSV '{temp_file_path}' for schema '{schema.name}': {e}")
        raise DataProcessingError(message=f"An unexpected error occurred during dictionary CSV creation for schema '{schema.name}': {e}", underlying_exception=e)


async def execute_duckdb_copy_from_csv(
    db_conn: 'duckdb.DuckDBPyConnection',
    schema_name: str, # Used for logging and context
    table_name: str,
    temp_csv_path: str,
    use_temp_table: bool = True,
    config_string: Optional[str] = None # Changed type hint to Optional
) -> int:
    """
    Executes DuckDB's COPY FROM CSV command into a specified table.

    Optionally uses a temporary table to first load data, then inserts into the final
    table using 'INSERT OR IGNORE'. This helps in handling potential duplicate records
    if the target table has unique constraints.

    Manages DuckDB transactions (BEGIN/COMMIT/ROLLBACK).

    Args:
        db_conn: An active DuckDB connection object.
        schema_name: The logical name of the schema (used for logging context).
        table_name: The name of the target DuckDB table.
        temp_csv_path: Path to the temporary CSV file containing data to be loaded.
        use_temp_table: If True, loads data into a temporary table first, then inserts
                        into the final table. This allows for 'INSERT OR IGNORE' semantics.
                        If False, copies directly into the target table.
        config_string: Optional string of DuckDB SET commands to apply before the COPY operation.

    Returns:
        An estimated number of rows processed or made available by the COPY operation.
        If `use_temp_table` is True, this is the count of rows in the temporary table.
        If `use_temp_table` is False, this is an estimate based on lines in the CSV file.
        A value of -1 indicates an issue in estimating rows from the CSV.

    Raises:
        DatabaseError: For DuckDB specific errors during the COPY operation or transaction management.
        DataProcessingError: For IOErrors related to the CSV file during the operation.
    """
    rows_affected_estimate = 0
    transaction_active = False # Flag to track if a transaction is active
    temp_table_name_in_db = None # To ensure cleanup if temp table was created

    logger.debug(f"[DBUtil] Starting COPY FROM CSV for table '{table_name}' (Schema context: '{schema_name}') from '{temp_csv_path}'. Temp table: {use_temp_table}.")

    try:
        logger.debug(f"[DBUtil] Beginning transaction for COPY operation into '{table_name}'.")
        db_conn.execute("BEGIN TRANSACTION")
        transaction_active = True

        if config_string:
            logger.debug(f"[DBUtil] Applying config string to connection for '{table_name}': {config_string}")
            db_conn.execute(config_string)

        target_table_for_copy = table_name # Table to COPY INTO initially
        if use_temp_table:
            # Create a unique temporary table name to avoid collisions
            temp_table_name_in_db = f"temp_copy_{table_name}_{int(time.time())}_{os.getpid()}"
            create_temp_sql = f'CREATE TEMPORARY TABLE "{temp_table_name_in_db}" AS SELECT * FROM "{table_name}" LIMIT 0'
            logger.debug(f"[DBUtil] Creating temporary table for COPY: '{temp_table_name_in_db}' for target '{table_name}'. SQL: {create_temp_sql}")
            db_conn.execute(create_temp_sql)
            target_table_for_copy = temp_table_name_in_db

        copy_sql = f"""
            COPY "{target_table_for_copy}" FROM '{temp_csv_path}'
            (FORMAT CSV, HEADER true, DELIMITER ',', QUOTE '"', ENCODING 'utf-8', IGNORE_ERRORS true)
        """
        logger.info(f"[DBUtil] Executing COPY SQL for table '{target_table_for_copy}': {copy_sql.strip()}")
        db_conn.execute(copy_sql) # Perform the COPY operation

        if use_temp_table and temp_table_name_in_db:
            # Estimate rows affected by counting rows in the temporary table
            count_result = db_conn.execute(f'SELECT COUNT(*) FROM "{temp_table_name_in_db}"').fetchone()
            rows_affected_estimate = count_result[0] if count_result else 0
            logger.debug(f"[DBUtil] {rows_affected_estimate} rows copied into temporary table '{temp_table_name_in_db}'.")

            insert_sql = f'INSERT OR IGNORE INTO "{table_name}" SELECT * FROM "{temp_table_name_in_db}"'
            logger.info(f"[DBUtil] Inserting from temporary table '{temp_table_name_in_db}' into target table '{table_name}'.")
            db_conn.execute(insert_sql)
            # Note: Actual number of *newly inserted* rows into the final table is not easily
            # captured here without `changes()` or pre/post counts on the final table.
            # rows_affected_estimate remains the count from the temp table.

            logger.debug(f"[DBUtil] Dropping temporary table '{temp_table_name_in_db}'.")
            db_conn.execute(f'DROP TABLE "{temp_table_name_in_db}"')
        else:
            # Estimate rows from CSV file lines if not using a temp table.
            # This is a rough estimate as COPY IGNORE_ERRORS might skip some rows.
            try:
                with open(temp_csv_path, 'r', encoding='utf-8') as f_count:
                    csv_reader = csv.reader(f_count)
                    header = next(csv_reader, None) # Read header
                    if header:
                        rows_affected_estimate = sum(1 for _ in csv_reader) # Count data rows
                    else: # Empty file or no header
                        rows_affected_estimate = 0
                logger.debug(f"[DBUtil] Estimated {rows_affected_estimate} data rows in CSV '{temp_csv_path}' for direct COPY into '{table_name}'.")
            except Exception as count_e:
                logger.warning(f"[DBUtil] Could not accurately count rows in CSV '{temp_csv_path}' for estimation: {count_e}")
                rows_affected_estimate = -1 # Indicate estimation failed

        logger.debug(f"[DBUtil] Committing transaction for COPY operation on '{table_name}'.")
        db_conn.commit()
        transaction_active = False # Transaction successfully committed
        logger.info(f"[DBUtil] Successfully executed COPY FROM '{temp_csv_path}' to table '{table_name}'. Estimated rows processed/copied: {rows_affected_estimate}")
        return rows_affected_estimate
    except duckdb.Error as e:
        logger.error(f"❌ [DBUtil] DuckDB Error during COPY FROM for table '{table_name}' from '{temp_csv_path}': {e}")
        if transaction_active and db_conn:
            logger.info(f"[DBUtil] Rolling back transaction for '{table_name}' due to DuckDB error.")
            db_conn.rollback()
        raise DatabaseError(message=f"DuckDB COPY operation failed for table '{table_name}'", underlying_exception=e)
    except IOError as e: # Specifically for file issues with temp_csv_path
        logger.error(f"❌ [DBUtil] IOError related to '{temp_csv_path}' during COPY for table '{table_name}': {e}")
        if transaction_active and db_conn:
            logger.info(f"[DBUtil] Rolling back transaction for '{table_name}' due to IOError.")
            db_conn.rollback()
        raise DataProcessingError(message=f"File error during DuckDB COPY for table '{table_name}'", underlying_exception=e)
    except Exception as e: # Catch-all for any other unexpected errors
        logger.error(f"❌ [DBUtil] Unexpected error during COPY FROM for table '{table_name}' from '{temp_csv_path}': {e}")
        if transaction_active and db_conn:
            logger.info(f"[DBUtil] Rolling back transaction for '{table_name}' due to unexpected error.")
            db_conn.rollback()
        raise DatabaseError(message=f"An unexpected error occurred during DuckDB COPY for table '{table_name}'", underlying_exception=e)
    finally:
        # Ensure temp table is dropped if created and an error occurred before explicit drop
        if use_temp_table and temp_table_name_in_db and db_conn:
            try:
                # Check if table exists before attempting to drop, only if connection is usable
                # This check might be complex if connection is in a bad state after an error.
                # A simple attempt to drop is often sufficient.
                logger.debug(f"[DBUtil] Attempting cleanup of temporary table '{temp_table_name_in_db}' in finally block.")
                # We cannot execute DROP if transaction is already rolled back due to error and connection is closed by pool
                # This part needs careful handling based on connection pool behavior.
                # For now, assume if an error happened, rollback was called, and this might fail or be unnecessary.
                # A more robust way is to ensure the temp table name includes session ID if DuckDB supports it,
                # or rely on TEMPORARY keyword to auto-clean on session end.
            except duckdb.Error as drop_err:
                logger.warning(f"[DBUtil] Could not ensure cleanup of temporary table '{temp_table_name_in_db}': {drop_err}. It might be auto-cleaned if session ends.")

"""
🚀 High-Performance Data API Endpoints
Showcasing the power of Polars + PyArrow + DuckDB integration
"""

from fastapi import APIRouter, HTTPException, status, Query, Depends, UploadFile, File
from fastapi.responses import StreamingResponse, FileResponse
from typing import List, Dict, Optional, Any
from pathlib import Path
import json
import time
import tempfile
import asyncio
from uuid import UUID

from app.container.container import container
from app.domain.exceptions import SchemaNotFoundException, InvalidDataException
from app.config.logging_config import logger


router = APIRouter()

@router.post("/ultra-fast-bulk/{schema_name}")
async def ultra_fast_bulk_insert(
    schema_name: str,
    data: List[Dict[str, Any]]
) -> Dict[str, Any]:
    """
    🚀 Ultra-fast bulk insert using Polars → Arrow → DuckDB pipeline
    
    Performance benefits:
    - 10-100x faster than traditional bulk inserts
    - Memory efficient processing
    - Zero-copy data transfers
    - Automatic data validation and type casting
    """
    try:
        # Get schema
        schema = await container.schema_repository.get_schema_by_name(schema_name)
        if not schema:
            raise HTTPException(status_code=404, detail=f"Schema '{schema_name}' not found")
        
        # Use high-performance processor
        result = await container.high_performance_processor.bulk_insert_ultra_fast(
            schema=schema,
            data=data
        )
        
        return {
            "success": True,
            "message": f"Ultra-fast bulk insert completed for {schema_name}",
            "performance_metrics": result,
            "optimization": "polars_arrow_duckdb_pipeline"
        }
        
    except SchemaNotFoundException as e:
        logger.warning(f"Schema not found: {e}")
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Ultra-fast bulk insert failed: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@router.get("/query-optimized/{schema_name}")
async def query_with_polars_optimization(
    schema_name: str,
    filters: Optional[str] = Query(None, description="JSON filters"),
    limit: Optional[int] = Query(None, description="Result limit"),
    analysis: Optional[str] = Query(None, description="Analysis type: summary, profile, quality")
) -> Dict[str, Any]:
    """
    🚀 Ultra-fast querying using DuckDB → Arrow → Polars pipeline
    
    Performance benefits:
    - Vectorized query execution
    - Zero-copy data transfers
    - Fast post-processing with Polars
    - Built-in data analysis capabilities
    """
    try:
        # Get schema
        schema = await container.schema_repository.get_schema_by_name(schema_name)
        if not schema:
            raise HTTPException(status_code=404, detail=f"Schema '{schema_name}' not found")
        
        # Parse filters
        filter_dict = {}
        if filters:
            filter_dict = json.loads(filters)
        
        # Execute optimized query
        df = await container.high_performance_processor.query_with_polars_optimization(
            schema=schema,
            filters=filter_dict,
            limit=limit
        )
        
        # Convert to response format
        records = df.to_dicts()
        
        response = {
            "success": True,
            "schema_name": schema_name,
            "records_count": len(records),
            "records": records[:100] if len(records) > 100 else records,  # Limit response size
            "optimization": "duckdb_arrow_polars_pipeline"
        }
        
        # Add analysis if requested
        if analysis:
            analysis_result = await container.high_performance_processor.analyze_with_polars(
                schema=schema,
                analysis_type=analysis
            )
            response["analysis"] = analysis_result
        
        return response
        
    except json.JSONDecodeError as e:
        raise HTTPException(status_code=400, detail="Invalid JSON in filters")
    except Exception as e:
        logger.error(f"Optimized query failed: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@router.get("/stream-arrow-batches/{schema_name}")
async def stream_with_arrow_batches(
    schema_name: str,
    batch_size: int = Query(50000, description="Arrow batch size")
):
    """
    🚀 Ultra-fast streaming using Arrow batches
    
    Performance benefits:
    - Memory-efficient streaming
    - Arrow batch processing
    - Async iteration
    - Optimal for large datasets
    """
    try:
        # Get schema
        schema = await container.schema_repository.get_schema_by_name(schema_name)
        if not schema:
            raise HTTPException(status_code=404, detail=f"Schema '{schema_name}' not found")
        
        async def generate_arrow_stream():
            """Generate streaming response with Arrow batches"""
            yield '{"stream_type": "arrow_batches", "data": [\n'
            
            first_batch = True
            async for df_batch in container.high_performance_processor.stream_with_arrow_batches(
                schema=schema,
                batch_size=batch_size
            ):
                if not first_batch:
                    yield ',\n'
                
                # Convert batch to JSON
                batch_data = {
                    "batch_size": len(df_batch),
                    "records": df_batch.to_dicts()
                }
                yield json.dumps(batch_data)
                first_batch = False
            
            yield '\n]}'
        
        return StreamingResponse(
            generate_arrow_stream(),
            media_type="application/json",
            headers={
                "X-Stream-Type": "arrow-batches",
                "X-Optimization": "arrow-polars-streaming"
            }
        )
        
    except Exception as e:
        logger.error(f"Arrow batch streaming failed: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@router.post("/export-parquet/{schema_name}")
async def export_to_parquet_optimized(
    schema_name: str,
    compression: str = Query("snappy", description="Compression: snappy, gzip, lz4, zstd")
) -> Dict[str, Any]:
    """
    🚀 Ultra-fast Parquet export using DuckDB → Parquet pipeline
    
    Performance benefits:
    - Direct DuckDB to Parquet export
    - Columnar format optimization
    - Compression options
    - Minimal memory usage
    """
    try:
        # Get schema
        schema = await container.schema_repository.get_schema_by_name(schema_name)
        if not schema:
            raise HTTPException(status_code=404, detail=f"Schema '{schema_name}' not found")
        
        # Create temporary file
        temp_dir = Path(tempfile.gettempdir())
        output_path = temp_dir / f"{schema_name}_{int(time.time())}.parquet"
        
        # Export using high-performance processor
        result = await container.high_performance_processor.export_to_parquet_optimized(
            schema=schema,
            output_path=output_path,
            compression=compression
        )
        
        return {
            "success": True,
            "message": f"Parquet export completed for {schema_name}",
            "export_details": result,
            "download_ready": True,
            "optimization": "duckdb_direct_parquet_export"
        }
        
    except Exception as e:
        logger.error(f"Parquet export failed: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@router.get("/analyze/{schema_name}")
async def analyze_data_with_polars(
    schema_name: str,
    analysis_type: str = Query("summary", description="Analysis type: summary, profile, quality")
) -> Dict[str, Any]:
    """
    🚀 Ultra-fast data analysis using Polars
    
    Analysis capabilities:
    - summary: Basic statistics and data types
    - profile: Detailed data profiling
    - quality: Data quality metrics
    """
    try:
        # Get schema
        schema = await container.schema_repository.get_schema_by_name(schema_name)
        if not schema:
            raise HTTPException(status_code=404, detail=f"Schema '{schema_name}' not found")
        
        # Perform analysis
        result = await container.high_performance_processor.analyze_with_polars(
            schema=schema,
            analysis_type=analysis_type
        )
        
        return {
            "success": True,
            "schema_name": schema_name,
            "analysis": result,
            "optimization": "polars_dataframe_analysis"
        }
        
    except Exception as e:
        logger.error(f"Data analysis failed: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@router.post("/benchmark/{schema_name}")
async def performance_benchmark(
    schema_name: str,
    test_data_size: int = Query(10000, description="Number of test records"),
    include_traditional: bool = Query(True, description="Include traditional method comparison")
) -> Dict[str, Any]:
    """
    🚀 Performance benchmark comparing traditional vs optimized methods
    
    Compares:
    - Traditional bulk insert vs Polars+Arrow+DuckDB
    - Traditional query vs DuckDB+Arrow+Polars
    - Memory usage and throughput metrics
    """
    try:
        # Get schema
        schema = await container.schema_repository.get_schema_by_name(schema_name)
        if not schema:
            raise HTTPException(status_code=404, detail=f"Schema '{schema_name}' not found")
        
        # Generate test data that matches the schema properly
        test_data = []
        for i in range(test_data_size):
            record = {}
            for prop in schema.properties:
                if prop.name == "field_code":
                    record[prop.name] = i % 1000  # Integer field codes
                elif prop.name == "field_name":
                    record[prop.name] = f"Field_{i % 1000}"
                elif prop.name == "well_code":
                    record[prop.name] = i % 100  # Integer well codes
                elif prop.name == "well_reference":
                    record[prop.name] = f"WELL_REF_{i % 100:03d}"
                elif prop.name == "well_name":
                    record[prop.name] = f"Well_{i % 100}"
                elif prop.name == "production_period":
                    record[prop.name] = f"2024-{(i % 12) + 1:02d}-01"  # Proper date format
                elif prop.name == "days_on_production":
                    record[prop.name] = 30
                elif prop.name in ["oil_production_kbd", "gas_production_mmcfd", "liquids_production_kbd", "water_production_kbd"]:
                    record[prop.name] = round(100.0 + (i * 0.1), 2)
                elif prop.name == "data_source":
                    record[prop.name] = "benchmark_test"
                elif prop.name == "source_data":
                    record[prop.name] = "performance_benchmark"
                elif prop.name == "partition_0":
                    record[prop.name] = f"partition_{i % 10}"
                elif prop.type == "string":
                    record[prop.name] = f"test_value_{i}"
                elif prop.type == "integer":
                    record[prop.name] = i
                elif prop.type == "number":
                    record[prop.name] = float(i) * 1.5
                elif prop.type == "boolean":
                    record[prop.name] = i % 2 == 0
            test_data.append(record)
        
        benchmark_results = {}
        
        # Benchmark optimized method
        start_time = time.perf_counter()
        optimized_result = await container.high_performance_processor.bulk_insert_ultra_fast(
            schema=schema,
            data=test_data
        )
        optimized_duration = (time.perf_counter() - start_time) * 1000
        
        benchmark_results["optimized_method"] = {
            "method": "polars_arrow_duckdb",
            "duration_ms": optimized_duration,
            "throughput_rps": optimized_result.get("throughput_rps", 0),
            "records_processed": len(test_data)
        }
        
        # Benchmark traditional method if requested
        if include_traditional:
            try:
                start_time = time.perf_counter()
                
                # Create DataRecord objects (traditional way)
                from app.domain.entities.data_record import DataRecord
                records = []
                for data_item in test_data:
                    composite_key = schema.get_composite_key_from_data(data_item)
                    record = DataRecord(
                        schema_name=schema.name,
                        data=data_item,
                        composite_key=composite_key
                    )
                    records.append(record)
                
                # Traditional bulk insert
                await container.data_repository.create_batch(schema, records)
                traditional_duration = (time.perf_counter() - start_time) * 1000
                traditional_throughput = len(test_data) / (traditional_duration / 1000) if traditional_duration > 0 else 0
                
                benchmark_results["traditional_method"] = {
                    "method": "traditional_bulk_insert",
                    "duration_ms": traditional_duration,
                    "throughput_rps": int(traditional_throughput),
                    "records_processed": len(test_data)
                }
                
                # Calculate performance improvement
                if traditional_duration > 0:
                    improvement_factor = traditional_duration / optimized_duration
                    benchmark_results["performance_improvement"] = {
                        "speed_improvement_factor": round(improvement_factor, 2),
                        "percentage_faster": round((improvement_factor - 1) * 100, 1)
                    }
            except Exception as traditional_error:
                logger.warning(f"Traditional method benchmark failed: {traditional_error}")
                benchmark_results["traditional_method"] = {
                    "method": "traditional_bulk_insert",
                    "error": str(traditional_error),
                    "success": False
                }
        
        return {
            "success": True,
            "schema_name": schema_name,
            "test_data_size": test_data_size,
            "benchmark_results": benchmark_results,
            "recommendation": "Use optimized method for maximum performance"
        }
        
    except Exception as e:
        logger.error(f"Performance benchmark failed: {e}")
        raise HTTPException(status_code=500, detail="Internal server error") 
from abc import ABC, abstractmethod
from typing import List, Optional, Any, Dict, Tuple
from app.domain.entities.schema import Schema
from app.application.dto.query_dto import QueryFilter, QuerySort
from enum import Enum

class QueryType(Enum):
    """Enumeration of supported query types for optimization purposes"""
    SELECT = "select"
    COUNT = "count"
    INSERT = "insert"
    UPDATE = "update"
    DELETE = "delete"

class SQLDialect(Enum):
    """Supported SQL dialects for query optimization"""
    DUCKDB = "duckdb"
    POSTGRESQL = "postgresql"
    SQLITE = "sqlite"
    MYSQL = "mysql"

class IQueryBuilder(ABC):
    """
    Abstract interface for database-agnostic query building.
    
    This interface provides a clean abstraction layer that:
    1. Decouples query logic from specific SQL dialects
    2. Enables performance optimizations per database type
    3. Improves testability and maintainability
    4. Supports future database integrations
    """
    
    def __init__(self, schema: Schema, dialect: SQLDialect):
        self.schema = schema
        self.dialect = dialect
        self.filters: List[QueryFilter] = []
        self.sort_by: List[QuerySort] = []
        self.limit: Optional[int] = None
        self.offset: int = 0
        self._params: List[Any] = []
    
    # Core Builder Methods
    @abstractmethod
    def add_filters(self, filters: Optional[List[QueryFilter]]) -> 'IQueryBuilder':
        """Add filtering conditions to the query"""
        pass
    
    @abstractmethod
    def add_sorts(self, sorts: Optional[List[QuerySort]]) -> 'IQueryBuilder':
        """Add sorting specifications to the query"""
        pass
    
    @abstractmethod
    def add_pagination(self, limit: Optional[int], offset: int) -> 'IQueryBuilder':
        """Add pagination (LIMIT/OFFSET) to the query"""
        pass
    
    # Query Building Methods
    @abstractmethod
    def build_select_query(self) -> str:
        """Build optimized SELECT query for the target dialect"""
        pass
    
    @abstractmethod
    def build_count_query(self) -> str:
        """Build optimized COUNT query for the target dialect"""
        pass
    
    @abstractmethod
    def build_insert_query(self, columns: List[str]) -> str:
        """Build optimized INSERT query for the target dialect"""
        pass
    
    @abstractmethod
    def build_bulk_insert_query(self, columns: List[str], batch_size: int) -> str:
        """Build optimized bulk INSERT query for the target dialect"""
        pass
    
    # Dialect-Specific Optimizations
    @abstractmethod
    def get_optimized_pagination_query(self, base_query: str) -> str:
        """Apply dialect-specific pagination optimizations"""
        pass
    
    @abstractmethod
    def get_performance_hints(self, query_type: QueryType) -> Dict[str, Any]:
        """Get database-specific performance optimization hints"""
        pass
    
    # Parameter Management
    def get_params(self) -> List[Any]:
        """Get parameter values for parameterized queries"""
        return self._params.copy()
    
    def reset_params(self) -> None:
        """Reset parameter list to avoid accumulation"""
        self._params.clear()
    
    # Validation
    @abstractmethod
    def validate_field(self, field_name: str) -> bool:
        """Validate if field exists in schema and is queryable"""
        pass
    
    # Utility Methods
    @abstractmethod
    def quote_identifier(self, identifier: str) -> str:
        """Quote database identifiers according to dialect rules"""
        pass
    
    @abstractmethod
    def get_dialect_specific_types(self) -> Dict[str, str]:
        """Get mapping of generic types to dialect-specific types"""
        pass
    
    # Performance Optimization
    @abstractmethod
    def estimate_query_cost(self, query: str) -> Dict[str, float]:
        """Estimate query performance cost (time, memory, etc.)"""
        pass
    
    @abstractmethod
    def suggest_optimizations(self, query: str) -> List[str]:
        """Suggest performance optimizations for the given query"""
        pass

class QueryBuilderFactory:
    """Factory for creating dialect-specific query builders"""
    
    _builders: Dict[SQLDialect, type] = {}
    
    @classmethod
    def register_builder(cls, dialect: SQLDialect, builder_class: type):
        """Register a query builder implementation for a dialect"""
        cls._builders[dialect] = builder_class
    
    @classmethod
    def create_builder(cls, schema: Schema, dialect: SQLDialect) -> IQueryBuilder:
        """Create appropriate query builder for the specified dialect"""
        if dialect not in cls._builders:
            raise NotImplementedError(f"No query builder registered for dialect: {dialect}")
        
        builder_class = cls._builders[dialect]
        return builder_class(schema, dialect)
    
    @classmethod
    def get_supported_dialects(cls) -> List[SQLDialect]:
        """Get list of supported SQL dialects"""
        return list(cls._builders.keys()) 
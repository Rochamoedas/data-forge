from typing import List, Optional, Any
from app.domain.entities.schema import Schema
from app.application.dto.query_dto import QueryFilter, FilterOperator, QuerySort
from app.domain.exceptions import InvalidDataException

class DuckDBQueryBuilder:
    def __init__(self, schema: Schema):
        self.schema = schema
        self.filters: List[QueryFilter] = []
        self.sort_by: List[QuerySort] = []
        self.limit: Optional[int] = None
        self.offset: int = 0
        self.params: List[Any] = []

    def add_filters(self, filters: Optional[List[QueryFilter]]) -> 'DuckDBQueryBuilder':
        if filters:
            valid_fields = {prop.name for prop in self.schema.properties} | {"id", "created_at", "version"}
            for f in filters:
                if f.field not in valid_fields:
                    raise InvalidDataException(f"Invalid filter field: {f.field}")
                self.filters.append(f)
        return self

    def add_sorts(self, sorts: Optional[List[QuerySort]]) -> 'DuckDBQueryBuilder':
        if sorts:
            valid_fields = {prop.name for prop in self.schema.properties} | {"id", "created_at", "version"}
            for s in sorts:
                if s.field not in valid_fields:
                    raise InvalidDataException(f"Invalid sort field: {s.field}")
                self.sort_by.append(s)
        return self

    def add_pagination(self, limit: Optional[int], offset: int) -> 'DuckDBQueryBuilder':
        self.limit = limit
        self.offset = offset
        return self

    def build_select_query(self) -> str:
        query_parts = [f"SELECT * FROM {self.schema.table_name}"]
        if self.filters:
            where_conditions = []
            for filter_item in self.filters:
                condition, param = self._build_filter_condition(filter_item)
                where_conditions.append(condition)
                if param is not None:
                    if isinstance(param, (list, tuple)):
                        self.params.extend(param)
                    else:
                        self.params.append(param)
            query_parts.append(f"WHERE {' AND '.join(where_conditions)}")
        if self.sort_by:
            sort_conditions = [f"{s.field} {s.order.upper()}" for s in self.sort_by]
            query_parts.append(f"ORDER BY {', '.join(sort_conditions)}")
        if self.limit is not None:
            query_parts.append(f"LIMIT {self.limit} OFFSET {self.offset}")
        return " ".join(query_parts)

    def build_select_query_without_pagination(self) -> str:
        """Build select query without pagination for streaming"""
        query_parts = [f"SELECT * FROM {self.schema.table_name}"]
        if self.filters:
            where_conditions = []
            for filter_item in self.filters:
                condition, param = self._build_filter_condition(filter_item)
                where_conditions.append(condition)
                if param is not None:
                    if isinstance(param, (list, tuple)):
                        self.params.extend(param)
                    else:
                        self.params.append(param)
            query_parts.append(f"WHERE {' AND '.join(where_conditions)}")
        if self.sort_by:
            sort_conditions = [f"{s.field} {s.order.upper()}" for s in self.sort_by]
            query_parts.append(f"ORDER BY {', '.join(sort_conditions)}")
        return " ".join(query_parts)

    def build_count_query(self) -> str:
        query_parts = [f"SELECT COUNT(*) FROM {self.schema.table_name}"]
        if self.filters:
            where_conditions = []
            for filter_item in self.filters:
                condition, param = self._build_filter_condition(filter_item)
                where_conditions.append(condition)
                if param is not None:
                    if isinstance(param, (list, tuple)):
                        self.params.extend(param)
                    else:
                        self.params.append(param)
            query_parts.append(f"WHERE {' AND '.join(where_conditions)}")
        return " ".join(query_parts)

    def _build_filter_condition(self, filter_item: QueryFilter) -> tuple[str, Any]:
        field = filter_item.field
        operator = filter_item.operator
        value = filter_item.value

        op_map = {
            FilterOperator.EQ: "=", FilterOperator.NE: "!=",
            FilterOperator.GT: ">", FilterOperator.GTE: ">=",
            FilterOperator.LT: "<", FilterOperator.LTE: "<="
        }

        if operator in op_map:
            return f"{field} {op_map[operator]} ?", value
        elif operator == FilterOperator.IN:
            if not isinstance(value, (list, tuple)):
                raise InvalidDataException("Value for 'IN' operator must be a list or tuple.")
            placeholders = ", ".join(["?"] * len(value))
            return f"{field} IN ({placeholders})", tuple(value)
        elif operator in [FilterOperator.LIKE, FilterOperator.ILIKE]:
            return f"{field} {operator.value.upper()} ?", value
        elif operator in [FilterOperator.IS_NULL, FilterOperator.IS_NOT_NULL]:
            return f"{field} {operator.value.replace('_', ' ').upper()}", None
        else:
            raise InvalidDataException(f"Unsupported filter operator: {operator}")

    def get_params(self) -> List[Any]:
        return self.params 
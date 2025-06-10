# app/domain/exceptions.py
class DomainException(Exception):
    """Base exception for all domain-related errors."""
    pass

class SchemaNotFoundException(DomainException):
    """Raised when a requested schema does not exist."""
    pass

class RecordNotFoundException(DomainException):
    """Raised when a requested data record does not exist."""
    pass

class InvalidDataException(DomainException):
    """Raised when data provided does not conform to the schema or business rules."""
    pass

class SchemaValidationException(DomainException):
    """Raised when schema validation fails during table creation or data operations."""
    pass

class DataProcessingError(DomainException):
    """Raised when an error occurs during data processing operations."""
    def __init__(self, message="Error during data processing", underlying_exception=None):
        super().__init__(message)
        self.underlying_exception = underlying_exception

class DatabaseError(DomainException):
    """Raised for database-related errors not covered by more specific exceptions."""
    def __init__(self, message="Database operation failed", underlying_exception=None):
        super().__init__(message)
        self.underlying_exception = underlying_exception

class RepositoryException(DomainException):
    """Raised for errors occurring within the data repository layer."""
    def __init__(self, message="Repository operation failed", underlying_exception=None):
        super().__init__(message)
        self.underlying_exception = underlying_exception

class DuplicateRecordException(RepositoryException):
    """Raised when attempting to create a record that already exists (e.g., unique constraint violation)."""
    def __init__(self, message="Duplicate record", record_id=None, underlying_exception=None):
        super().__init__(message, underlying_exception)
        self.record_id = record_id
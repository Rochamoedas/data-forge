# tests/test_domain/test_exceptions.py
import unittest
from app.domain.exceptions import (
    DomainException,
    SchemaNotFoundException,
    RecordNotFoundException,
    InvalidDataException,
    SchemaValidationException,
    DataProcessingError,
    DatabaseError,
    RepositoryException,
    DuplicateRecordException,
)

class TestDomainExceptions(unittest.TestCase):

    def test_domain_exception_inheritance(self):
        self.assertTrue(issubclass(DomainException, Exception))
        self.assertTrue(issubclass(SchemaNotFoundException, DomainException))
        self.assertTrue(issubclass(RecordNotFoundException, DomainException))
        self.assertTrue(issubclass(InvalidDataException, DomainException))
        self.assertTrue(issubclass(SchemaValidationException, DomainException))
        self.assertTrue(issubclass(DataProcessingError, DomainException))
        self.assertTrue(issubclass(DatabaseError, DomainException))
        self.assertTrue(issubclass(RepositoryException, DomainException))
        self.assertTrue(issubclass(DuplicateRecordException, RepositoryException))

    def test_instantiate_exceptions(self):
        try:
            raise DomainException("Test DomainException")
        except DomainException as e:
            self.assertEqual(str(e), "Test DomainException")

        try:
            raise SchemaNotFoundException("Test SchemaNotFoundException")
        except SchemaNotFoundException as e:
            self.assertEqual(str(e), "Test SchemaNotFoundException")

        try:
            raise RecordNotFoundException("Test RecordNotFoundException")
        except RecordNotFoundException as e:
            self.assertEqual(str(e), "Test RecordNotFoundException")

        try:
            raise InvalidDataException("Test InvalidDataException")
        except InvalidDataException as e:
            self.assertEqual(str(e), "Test InvalidDataException")

        try:
            raise SchemaValidationException("Test SchemaValidationException")
        except SchemaValidationException as e:
            self.assertEqual(str(e), "Test SchemaValidationException")

    def test_data_processing_error(self):
        original_exc = ValueError("Original error")
        try:
            raise DataProcessingError("DP error", underlying_exception=original_exc)
        except DataProcessingError as e:
            self.assertEqual(str(e), "DP error")
            self.assertIs(e.underlying_exception, original_exc)

    def test_database_error(self):
        original_exc = ConnectionError("DB connection failed")
        try:
            raise DatabaseError("DB op failed", underlying_exception=original_exc)
        except DatabaseError as e:
            self.assertEqual(str(e), "DB op failed")
            self.assertIs(e.underlying_exception, original_exc)

    def test_repository_exception(self):
        original_exc = TypeError("Repo type issue")
        try:
            raise RepositoryException("Repo issue", underlying_exception=original_exc)
        except RepositoryException as e:
            self.assertEqual(str(e), "Repo issue")
            self.assertIs(e.underlying_exception, original_exc)

    def test_duplicate_record_exception(self):
        original_exc = ValueError("duplicate key")
        record_id = "record123"
        try:
            raise DuplicateRecordException("Dupe rec", record_id=record_id, underlying_exception=original_exc)
        except DuplicateRecordException as e:
            self.assertEqual(str(e), "Dupe rec")
            self.assertEqual(e.record_id, record_id)
            self.assertIs(e.underlying_exception, original_exc)
            self.assertTrue(isinstance(e, RepositoryException)) # Check inheritance

if __name__ == '__main__':
    unittest.main()

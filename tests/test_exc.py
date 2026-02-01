"""Exception tests."""

import pytest

from pgsync.exc import (
    ColumnNotFoundError,
    FetchColumnForeignKeysError,
    ForeignKeyError,
    InvalidSchemaError,
    InvalidTGOPError,
    LogicalSlotParseError,
    MultipleThroughTablesError,
    NodeAttributeError,
    PrimaryKeyNotFoundError,
    RDSError,
    RelationshipAttributeError,
    RelationshipError,
    RelationshipForeignKeyError,
    RelationshipTypeError,
    RelationshipVariantError,
    ReplicationSlotError,
    SchemaError,
    TableNotFoundError,
    TableNotInNodeError,
)


class TestExceptions:
    """Tests for custom exception classes."""

    def test_relationship_type_error(self):
        """Test RelationshipTypeError exception."""
        error = RelationshipTypeError("Invalid type: xyz")
        assert error.value == "Invalid type: xyz"
        assert "Invalid type: xyz" in str(error)

        with pytest.raises(RelationshipTypeError):
            raise RelationshipTypeError("Test error")

    def test_relationship_variant_error(self):
        """Test RelationshipVariantError exception."""
        error = RelationshipVariantError("Invalid variant: abc")
        assert error.value == "Invalid variant: abc"
        assert "Invalid variant: abc" in str(error)

        with pytest.raises(RelationshipVariantError):
            raise RelationshipVariantError("Test error")

    def test_relationship_foreign_key_error(self):
        """Test RelationshipForeignKeyError exception."""
        error = RelationshipForeignKeyError("Missing foreign key")
        assert error.value == "Missing foreign key"
        assert "Missing foreign key" in str(error)

        with pytest.raises(RelationshipForeignKeyError):
            raise RelationshipForeignKeyError("Test error")

    def test_relationship_attribute_error(self):
        """Test RelationshipAttributeError exception."""
        error = RelationshipAttributeError("Unknown attribute")
        assert error.value == "Unknown attribute"
        assert "Unknown attribute" in str(error)

        with pytest.raises(RelationshipAttributeError):
            raise RelationshipAttributeError("Test error")

    def test_table_not_found_error(self):
        """Test TableNotFoundError exception."""
        error = TableNotFoundError("Table 'users' not found")
        assert error.value == "Table 'users' not found"
        assert "Table 'users' not found" in str(error)

        with pytest.raises(TableNotFoundError):
            raise TableNotFoundError("Test error")

    def test_table_not_in_node_error(self):
        """Test TableNotInNodeError exception."""
        error = TableNotInNodeError("Table not specified in node")
        assert error.value == "Table not specified in node"
        assert "Table not specified in node" in str(error)

        with pytest.raises(TableNotInNodeError):
            raise TableNotInNodeError("Test error")

    def test_invalid_schema_error(self):
        """Test InvalidSchemaError exception."""
        error = InvalidSchemaError("Schema 'invalid' not found")
        assert error.value == "Schema 'invalid' not found"
        assert "Schema 'invalid' not found" in str(error)

        with pytest.raises(InvalidSchemaError):
            raise InvalidSchemaError("Test error")

    def test_invalid_tgop_error(self):
        """Test InvalidTGOPError exception."""
        error = InvalidTGOPError("Unknown tg_op: UNKNOWN")
        assert error.value == "Unknown tg_op: UNKNOWN"
        assert "Unknown tg_op: UNKNOWN" in str(error)

        with pytest.raises(InvalidTGOPError):
            raise InvalidTGOPError("Test error")

    def test_node_attribute_error(self):
        """Test NodeAttributeError exception."""
        error = NodeAttributeError("Unknown attribute 'foo'")
        assert error.value == "Unknown attribute 'foo'"
        assert "Unknown attribute 'foo'" in str(error)

        with pytest.raises(NodeAttributeError):
            raise NodeAttributeError("Test error")

    def test_column_not_found_error(self):
        """Test ColumnNotFoundError exception."""
        error = ColumnNotFoundError("Column 'xyz' not found")
        assert error.value == "Column 'xyz' not found"
        assert "Column 'xyz' not found" in str(error)

        with pytest.raises(ColumnNotFoundError):
            raise ColumnNotFoundError("Test error")

    def test_foreign_key_error(self):
        """Test ForeignKeyError exception."""
        error = ForeignKeyError("Foreign key constraint violation")
        assert error.value == "Foreign key constraint violation"
        assert "Foreign key constraint violation" in str(error)

        with pytest.raises(ForeignKeyError):
            raise ForeignKeyError("Test error")

    def test_relationship_error(self):
        """Test RelationshipError exception."""
        error = RelationshipError("Invalid relationship")
        assert error.value == "Invalid relationship"
        assert "Invalid relationship" in str(error)

        with pytest.raises(RelationshipError):
            raise RelationshipError("Test error")

    def test_multiple_through_tables_error(self):
        """Test MultipleThroughTablesError exception."""
        error = MultipleThroughTablesError(
            "Multiple through tables not allowed"
        )
        assert error.value == "Multiple through tables not allowed"
        assert "Multiple through tables not allowed" in str(error)

        with pytest.raises(MultipleThroughTablesError):
            raise MultipleThroughTablesError("Test error")

    def test_replication_slot_error(self):
        """Test ReplicationSlotError exception."""
        error = ReplicationSlotError("Slot 'myslot' already exists")
        assert error.value == "Slot 'myslot' already exists"
        assert "Slot 'myslot' already exists" in str(error)

        with pytest.raises(ReplicationSlotError):
            raise ReplicationSlotError("Test error")

    def test_schema_error(self):
        """Test SchemaError exception."""
        error = SchemaError("Schema validation failed")
        assert error.value == "Schema validation failed"
        assert "Schema validation failed" in str(error)

        with pytest.raises(SchemaError):
            raise SchemaError("Test error")

    def test_fetch_column_foreign_keys_error(self):
        """Test FetchColumnForeignKeysError exception."""
        error = FetchColumnForeignKeysError("Could not fetch foreign keys")
        assert error.value == "Could not fetch foreign keys"
        assert "Could not fetch foreign keys" in str(error)

        with pytest.raises(FetchColumnForeignKeysError):
            raise FetchColumnForeignKeysError("Test error")

    def test_primary_key_not_found_error(self):
        """Test PrimaryKeyNotFoundError exception."""
        error = PrimaryKeyNotFoundError("Primary key not found for table")
        assert error.value == "Primary key not found for table"
        assert "Primary key not found for table" in str(error)

        with pytest.raises(PrimaryKeyNotFoundError):
            raise PrimaryKeyNotFoundError("Test error")

    def test_logical_slot_parse_error(self):
        """Test LogicalSlotParseError exception."""
        error = LogicalSlotParseError("Could not parse slot data")
        assert error.value == "Could not parse slot data"
        assert "Could not parse slot data" in str(error)

        with pytest.raises(LogicalSlotParseError):
            raise LogicalSlotParseError("Test error")

    def test_rds_error(self):
        """Test RDSError exception."""
        error = RDSError("RDS specific error")
        assert error.value == "RDS specific error"
        assert "RDS specific error" in str(error)

        with pytest.raises(RDSError):
            raise RDSError("Test error")


# ============================================================================
# PHASE 9 EXTENDED TESTS - Exc.py Final Coverage
# ============================================================================


class TestExceptionsExtended:
    """Extended tests for exception classes to achieve final coverage."""

    def test_exception_base_class_inheritance(self):
        """Test all custom exceptions inherit from Exception."""
        from pgsync.exc import (
            ForeignKeyError,
            PrimaryKeyNotFoundError,
            SchemaError,
        )

        # All should be Exception subclasses
        assert issubclass(SchemaError, Exception)
        assert issubclass(ForeignKeyError, Exception)
        assert issubclass(PrimaryKeyNotFoundError, Exception)

    def test_schema_error_with_detailed_message(self):
        """Test SchemaError with detailed error message."""
        from pgsync.exc import SchemaError

        detailed_msg = "Invalid schema configuration: missing 'nodes' key in schema definition"
        error = SchemaError(detailed_msg)

        assert error.value == detailed_msg
        assert "missing 'nodes' key" in str(error)

    def test_foreign_key_error_with_table_info(self):
        """Test ForeignKeyError with table information."""
        from pgsync.exc import ForeignKeyError

        error = ForeignKeyError(
            "No foreign key found between book and publisher"
        )

        assert "book" in str(error)
        assert "publisher" in str(error)

    def test_primary_key_not_found_error_context(self):
        """Test PrimaryKeyNotFoundError with table context."""
        from pgsync.exc import PrimaryKeyNotFoundError

        error = PrimaryKeyNotFoundError(
            "Table 'book' has no primary key defined"
        )

        assert "book" in str(error)
        assert "primary key" in str(error).lower()

    def test_invalid_tgop_error_operation_name(self):
        """Test InvalidTGOPError includes operation name."""
        from pgsync.exc import InvalidTGOPError

        error = InvalidTGOPError("Invalid operation: INVALID_OP")

        assert "INVALID_OP" in str(error)

    def test_relationship_type_error_specific_type(self):
        """Test RelationshipTypeError with specific relationship type."""
        from pgsync.exc import RelationshipTypeError

        error = RelationshipTypeError("Invalid relationship type: one_to_none")

        assert "one_to_none" in str(error)

    def test_relationship_variant_error_specific_variant(self):
        """Test RelationshipVariantError with specific variant."""
        from pgsync.exc import RelationshipVariantError

        error = RelationshipVariantError("Invalid variant: invalid_variant")

        assert "invalid_variant" in str(error)

    def test_relationship_foreign_key_error_details(self):
        """Test RelationshipForeignKeyError with detailed key info."""
        from pgsync.exc import RelationshipForeignKeyError

        error = RelationshipForeignKeyError(
            "Foreign key mismatch: expected ['id'], got ['book_id']"
        )

        assert "id" in str(error)
        assert "book_id" in str(error)

    def test_exception_string_representation(self):
        """Test exception __str__ method returns value."""
        from pgsync.exc import SchemaError

        error = SchemaError("Test error message")

        # String representation should return the value (wrapped in quotes)
        assert "Test error message" in str(error)

    def test_multiple_exceptions_can_be_raised(self):
        """Test multiple different exceptions can be raised in sequence."""
        from pgsync.exc import ForeignKeyError, InvalidTGOPError, SchemaError

        # Should be able to raise multiple different exceptions
        try:
            raise SchemaError("First error")
        except SchemaError:
            pass

        try:
            raise ForeignKeyError("Second error")
        except ForeignKeyError:
            pass

        try:
            raise InvalidTGOPError("Third error")
        except InvalidTGOPError:
            pass

        # All exceptions handled successfully

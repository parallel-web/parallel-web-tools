"""Tests for the sql_utils module."""

import pytest

from parallel_web_tools.core.sql_utils import quote_identifier, validate_table_name


class TestQuoteIdentifier:
    """Tests for quote_identifier function."""

    def test_simple_name(self):
        """Should quote a simple table name."""
        assert quote_identifier("my_table") == '"my_table"'

    def test_schema_qualified_name(self):
        """Should quote each part of a schema.table name."""
        assert quote_identifier("schema.table") == '"schema"."table"'

    def test_fully_qualified_name(self):
        """Should quote each part of catalog.schema.table."""
        assert quote_identifier("catalog.schema.table") == '"catalog"."schema"."table"'

    def test_escapes_embedded_double_quotes(self):
        """Should escape double quotes by doubling them."""
        assert quote_identifier('my"table') == '"my""table"'

    def test_escapes_double_quotes_in_qualified_name(self):
        """Should escape quotes in each part of a qualified name."""
        assert quote_identifier('sc"hema.ta"ble') == '"sc""hema"."ta""ble"'

    def test_strips_whitespace_from_parts(self):
        """Should strip whitespace from each component."""
        assert quote_identifier(" schema . table ") == '"schema"."table"'

    def test_empty_string_raises(self):
        """Should raise ValueError for empty string."""
        with pytest.raises(ValueError, match="cannot be empty"):
            quote_identifier("")

    def test_whitespace_only_raises(self):
        """Should raise ValueError for whitespace-only string."""
        with pytest.raises(ValueError, match="cannot be empty"):
            quote_identifier("   ")

    def test_empty_part_raises(self):
        """Should raise ValueError when a dot-separated part is empty."""
        with pytest.raises(ValueError, match="Invalid identifier"):
            quote_identifier("schema..table")

    def test_leading_dot_raises(self):
        """Should raise ValueError for leading dot."""
        with pytest.raises(ValueError, match="Invalid identifier"):
            quote_identifier(".table")

    def test_trailing_dot_raises(self):
        """Should raise ValueError for trailing dot."""
        with pytest.raises(ValueError, match="Invalid identifier"):
            quote_identifier("table.")

    def test_name_with_spaces(self):
        """Should quote names containing spaces."""
        assert quote_identifier("my table") == '"my table"'

    def test_name_with_special_chars(self):
        """Should quote names with special characters."""
        assert quote_identifier("table-name!@#") == '"table-name!@#"'


class TestValidateTableName:
    """Tests for validate_table_name function."""

    def test_simple_valid_name(self):
        """Should accept a simple alphanumeric name."""
        assert validate_table_name("my_table") == "my_table"

    def test_name_with_dots(self):
        """Should accept schema-qualified names."""
        assert validate_table_name("schema.table") == "schema.table"

    def test_name_starting_with_underscore(self):
        """Should accept names starting with underscore."""
        assert validate_table_name("_private_table") == "_private_table"

    def test_name_with_numbers(self):
        """Should accept names with numbers (not leading)."""
        assert validate_table_name("table123") == "table123"

    def test_empty_string_raises(self):
        """Should raise ValueError for empty string."""
        with pytest.raises(ValueError, match="cannot be empty"):
            validate_table_name("")

    def test_whitespace_only_raises(self):
        """Should raise ValueError for whitespace-only string."""
        with pytest.raises(ValueError, match="cannot be empty"):
            validate_table_name("   ")

    def test_name_starting_with_number_raises(self):
        """Should reject names starting with a number."""
        with pytest.raises(ValueError, match="Invalid table name"):
            validate_table_name("123table")

    def test_name_with_spaces_raises(self):
        """Should reject names with spaces."""
        with pytest.raises(ValueError, match="Invalid table name"):
            validate_table_name("my table")

    def test_name_with_special_chars_raises(self):
        """Should reject names with special characters."""
        with pytest.raises(ValueError, match="Invalid table name"):
            validate_table_name("table; DROP TABLE users")

    def test_name_with_quotes_raises(self):
        """Should reject names with quotes (SQL injection attempt)."""
        with pytest.raises(ValueError, match="Invalid table name"):
            validate_table_name('table"name')

    def test_name_with_semicolon_raises(self):
        """Should reject names with semicolons."""
        with pytest.raises(ValueError, match="Invalid table name"):
            validate_table_name("table;name")

    def test_name_with_hyphen_raises(self):
        """Should reject names with hyphens."""
        with pytest.raises(ValueError, match="Invalid table name"):
            validate_table_name("table-name")

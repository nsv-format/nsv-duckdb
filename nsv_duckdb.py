"""
NSV DuckDB Extension PoC

This module provides NSV (Newline-Separated Values) support for DuckDB.
It allows reading NSV files into DuckDB tables and exporting DuckDB results to NSV format.
"""

import duckdb
from typing import List, Iterable, Optional
import sys
import os

# Add the nsv-python module to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'nsv-python'))

try:
    import nsv
except ImportError:
    print("Warning: nsv module not found. Please ensure nsv-python is in the parent directory.")
    # Fallback: implement minimal NSV parsing inline
    class nsv:
        @staticmethod
        def loads(s: str) -> List[List[str]]:
            """Minimal NSV parser."""
            data = []
            row = []
            start = 0
            for pos, c in enumerate(s):
                if c == '\n':
                    if pos - start >= 1:
                        cell = s[start:pos]
                        # Unescape
                        if cell == '\\':
                            row.append('')
                        elif '\\' in cell:
                            cell = cell.replace('\\\\', '\x00').replace('\\n', '\n').replace('\x00', '\\')
                            row.append(cell)
                        else:
                            row.append(cell)
                    else:
                        data.append(row)
                        row = []
                    start = pos + 1
            return data

        @staticmethod
        def load(file_obj):
            """Load NSV from file object."""
            return nsv.loads(file_obj.read())

        @staticmethod
        def dumps(data: Iterable[Iterable[str]]) -> str:
            """Write elements to an NSV string."""
            lines = []
            for row in data:
                for cell in row:
                    # Escape
                    if cell == '':
                        lines.append('\\')
                    elif '\n' in cell or '\\' in cell:
                        escaped = cell.replace("\\", "\\\\").replace("\n", "\\n")
                        lines.append(escaped)
                    else:
                        lines.append(cell)
                lines.append('')
            return ''.join(f'{line}\n' for line in lines)


class NSVDuckDB:
    """NSV integration for DuckDB."""

    def __init__(self, con: Optional[duckdb.DuckDBPyConnection] = None):
        """
        Initialize NSV DuckDB integration.

        Args:
            con: DuckDB connection. If None, creates a new connection.
        """
        self.con = con if con is not None else duckdb.connect()

    def read_nsv(self, filepath: str, column_names: Optional[List[str]] = None) -> duckdb.DuckDBPyRelation:
        """
        Read an NSV file into a DuckDB relation.

        Args:
            filepath: Path to the NSV file
            column_names: Optional column names. If None, uses first row as header.

        Returns:
            DuckDB relation containing the data
        """
        with open(filepath, 'r') as f:
            data = nsv.load(f)

        if not data:
            raise ValueError("Empty NSV file")

        # If column names not provided, use first row
        if column_names is None and data:
            column_names = data[0]
            data = data[1:]

        # If still no column names, generate them
        if not column_names and data:
            num_cols = len(data[0]) if data else 0
            column_names = [f'col{i}' for i in range(num_cols)]

        # Convert to DuckDB relation
        if not data:
            # Create empty relation with just column names
            return self.con.from_df(
                __import__('pandas').DataFrame(columns=column_names)
            )

        # Create a relation from the data
        # DuckDB can work with list of lists
        return self.con.from_df(
            __import__('pandas').DataFrame(data, columns=column_names)
        )

    def to_nsv(self, relation: duckdb.DuckDBPyRelation, filepath: str,
               include_header: bool = True) -> None:
        """
        Write a DuckDB relation to an NSV file.

        Args:
            relation: DuckDB relation to export
            filepath: Path to output NSV file
            include_header: Whether to include column names as first row
        """
        # Get data as list of lists
        df = relation.df()
        data = df.values.tolist()

        # Add header if requested
        if include_header:
            data.insert(0, df.columns.tolist())

        # Convert all values to strings
        str_data = [[str(cell) for cell in row] for row in data]

        # Write to file
        with open(filepath, 'w') as f:
            f.write(nsv.dumps(str_data))

    def query_nsv(self, filepath: str, sql: str, column_names: Optional[List[str]] = None):
        """
        Read an NSV file and immediately query it with SQL.

        Args:
            filepath: Path to the NSV file
            sql: SQL query to execute. Use 'nsv_data' as the table name.
            column_names: Optional column names

        Returns:
            Query results as a DuckDB relation
        """
        rel = self.read_nsv(filepath, column_names)
        # Register as a temporary view
        self.con.register('nsv_data', rel)
        return self.con.query(sql)


def read_nsv(filepath: str, column_names: Optional[List[str]] = None,
             con: Optional[duckdb.DuckDBPyConnection] = None) -> duckdb.DuckDBPyRelation:
    """
    Convenience function to read an NSV file into a DuckDB relation.

    Args:
        filepath: Path to the NSV file
        column_names: Optional column names. If None, uses first row as header.
        con: DuckDB connection. If None, creates a new connection.

    Returns:
        DuckDB relation containing the data
    """
    nsv_db = NSVDuckDB(con)
    return nsv_db.read_nsv(filepath, column_names)


def to_nsv(relation: duckdb.DuckDBPyRelation, filepath: str,
           include_header: bool = True) -> None:
    """
    Convenience function to write a DuckDB relation to an NSV file.

    Args:
        relation: DuckDB relation to export
        filepath: Path to output NSV file
        include_header: Whether to include column names as first row
    """
    con = relation.connection if hasattr(relation, 'connection') else duckdb.connect()
    nsv_db = NSVDuckDB(con)
    nsv_db.to_nsv(relation, filepath, include_header)

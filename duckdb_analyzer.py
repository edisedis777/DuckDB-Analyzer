"""
DuckDB Data Analysis Project
----------------------------
A tool for analyzing large CSV datasets using DuckDB.
"""
import os
import argparse
import pandas as pd
import duckdb
import time


class DuckDBAnalyzer:
    """Class to handle DuckDB operations for large data analysis."""

    def __init__(self, database_name: str = ":memory:") -> None:
        """
        Initialize DuckDB connection.

        Args:
            database_name (str): Path to database file or ":memory:" for in-memory database
        """
        self.conn = duckdb.connect(database=database_name, read_only=False)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.conn.close()

    def execute_query(self, query: str, show: bool = True) -> duckdb.DuckDBPyRelation:
        """
        Execute a SQL query and optionally display results.

        Args:
            query (str): SQL query to execute
            show (bool): Whether to display the results

        Returns:
            duckdb.DuckDBPyRelation: Query result object

        Example:
            >>> analyzer.execute_query("SELECT * FROM data LIMIT 5")
        """
        result = self.conn.execute(query)
        if show:
            result.show()
        return result

    def count_rows(self, source_path: str) -> int:
        """
        Count rows in a data source.

        Args:
            source_path (str): Path to the data source (file or table)

        Returns:
            int: Number of rows

        Example:
            >>> analyzer.count_rows("data.csv")
            1000
        """
        query = f"SELECT COUNT(*) AS rows FROM '{source_path}'"
        result = self.execute_query(query, show=False)
        return result.fetchone()[0]

    def import_csv_to_table(self, csv_path: str, table_name: str, sample_rows: int = None) -> None:
        """
        Import CSV data into a DuckDB table with performance timing.

        Args:
            csv_path (str): Path to CSV file
            table_name (str): Name for the new table
            sample_rows (int, optional): Limit import to a sample of rows

        Example:
            >>> analyzer.import_csv_to_table("data.csv", "data_table")
            Imported data into table 'data_table' in 0.25 seconds
        """
        start_time = time.time()
        if sample_rows:
            query = f"CREATE TABLE {table_name} AS SELECT * FROM read_csv_auto('{csv_path}') LIMIT {sample_rows}"
        else:
            query = f"CREATE TABLE {table_name} AS SELECT * FROM read_csv_auto('{csv_path}')"
        self.execute_query(query, show=False)
        end_time = time.time()
        print(f"Imported data into table '{table_name}' in {end_time - start_time:.2f} seconds")

    def get_table_schema(self, table_name: str) -> pd.DataFrame:
        """
        Get the schema of a table.

        Args:
            table_name (str): Name of the table

        Returns:
            pd.DataFrame: Table schema with column names and types

        Example:
            >>> analyzer.get_table_schema("data_table")
        """
        query = f"DESCRIBE {table_name}"
        result = self.execute_query(query, show=False)
        return result.df()

    def get_compression_info(self, table_name: str) -> pd.DataFrame:
        """
        Get compression information for a table.

        Args:
            table_name (str): Name of the table

        Returns:
            pd.DataFrame: Compression info excluding less relevant columns

        Example:
            >>> analyzer.get_compression_info("data_table")
        """
        query = f"""
        SELECT * EXCLUDE (column_path, segment_id, start, persistent, 
                         block_id, stats, block_offset, has_updates)
        FROM pragma_storage_info('{table_name}')
        USING SAMPLE 10 ROWS
        ORDER BY row_group_id
        """
        result = self.execute_query(query, show=False)
        return result.df()

    def sample_data(self, source: str, rows: int = 5, random: bool = False) -> pd.DataFrame:
        """
        Retrieve a sample of data from a source.

        Args:
            source (str): Table name or file path to sample from
            rows (int): Number of rows to return (default: 5)
            random (bool): Whether to use random sampling (default: False)

        Returns:
            pd.DataFrame: A pandas DataFrame containing the sampled rows

        Example:
            >>> analyzer.sample_data("data.csv", rows=10, random=True)
        """
        if random:
            query = f"SELECT * FROM '{source}' USING SAMPLE {rows} ROWS"
        else:
            query = f"SELECT * FROM '{source}' LIMIT {rows}"
        result = self.execute_query(query, show=False)
        return result.df()

    def get_column_stats(self, source: str, column_name: str) -> dict:
        """
        Get statistics for a specific column.

        Args:
            source (str): Table name or file path
            column_name (str): Name of the column

        Returns:
            dict: Column statistics including count, unique_values, min_value, max_value

        Example:
            >>> analyzer.get_column_stats("data.csv", "age")
            {'count': 1000, 'unique_values': 50, 'min_value': 18, 'max_value': 75}
        """
        # Handle column names with spaces
        if " " in column_name:
            column_name = f'"{column_name}"'

        stats_query = f"""
        SELECT 
            COUNT({column_name}) as count,
            COUNT(DISTINCT {column_name}) as unique_values,
            MIN({column_name}) as min_value,
            MAX({column_name}) as max_value
        FROM '{source}'
        """
        stats = self.execute_query(stats_query, show=False).fetchone()

        return {
            "count": stats[0],
            "unique_values": stats[1],
            "min_value": stats[2],
            "max_value": stats[3]
        }

    def group_by_analysis(self, source: str, group_column: str, count_column: str = "*") -> pd.DataFrame:
        """
        Perform a GROUP BY analysis.

        Args:
            source (str): Table name or file path
            group_column (str): Column to group by
            count_column (str): Column to count (default: "*")

        Returns:
            pd.DataFrame: Group by results

        Example:
            >>> analyzer.group_by_analysis("data.csv", "category")
        """
        # Handle column names with spaces
        if " " in group_column:
            group_column = f'"{group_column}"'

        query = f"""
        SELECT 
            {group_column},
            COUNT({count_column}) as count
        FROM '{source}'
        GROUP BY {group_column}
        ORDER BY count DESC
        """
        result = self.execute_query(query, show=False)
        return result.df()

    def get_column_names(self, source: str) -> list[str]:
        """
        Retrieve column names from a source (file or table).

        Args:
            source (str): Table name or file path

        Returns:
            list[str]: List of column names

        Example:
            >>> analyzer.get_column_names("data.csv")
            ["id", "name", "age"]
        """
        query = f"SELECT * FROM '{source}' LIMIT 0"
        result = self.conn.execute(query)
        return [desc[0] for desc in result.description]

    def table_exists(self, table_name: str) -> bool:
        """
        Check if a table exists in the database.

        Args:
            table_name (str): Name of the table

        Returns:
            bool: True if the table exists, False otherwise

        Example:
            >>> analyzer.table_exists("data_table")
            True
        """
        query = f"SELECT COUNT(*) FROM duckdb_tables() WHERE table_name = '{table_name}'"
        result = self.conn.execute(query).fetchone()
        return result[0] > 0


def get_file_size_mb(file_path: str) -> float:
    """
    Get file size in megabytes.

    Args:
        file_path (str): Path to the file

    Returns:
        float: File size in MB
    """
    return os.path.getsize(file_path) / (1024 * 1024)


def main():
    """Main function to run the CLI tool."""
    parser = argparse.ArgumentParser(
        description="Analyze large datasets with DuckDB",
        epilog="Examples:\n  python script.py --file data.csv --action count\n  python script.py --file data.csv --action sample --limit 10 --random",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    parser.add_argument("--file", type=str, help="Path to the CSV file")
    parser.add_argument("--db", type=str, default=":memory:",
                        help="Path to save the DuckDB database (default: in-memory)")
    parser.add_argument("--action", type=str, choices=[
        "count", "sample", "import", "stats", "schema", "compression", "group", "query"
    ], required=True, help="Action to perform")
    parser.add_argument("--table", type=str, default="data",
                        help="Table name (for import action)")
    parser.add_argument("--column", type=str,
                        help="Column name (for stats or group action)")
    parser.add_argument("--limit", type=int, default=5,
                        help="Limit sample rows (default: 5)")
    parser.add_argument("--random", action="store_true", help="Use random sampling for 'sample' action")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing table for 'import' action")
    parser.add_argument("--sql", type=str, help="SQL query for 'query' action")

    args = parser.parse_args()

    # Check if file exists for actions that require it
    if args.file and not os.path.exists(args.file):
        print(f"Error: File '{args.file}' not found")
        return 1

    try:
        with DuckDBAnalyzer(args.db) as analyzer:
            if args.action == "count":
                if not args.file:
                    parser.error("--file is required for 'count' action")
                count = analyzer.count_rows(args.file)
                print(f"File contains {count:,} rows")
                print(f"File size: {get_file_size_mb(args.file):.2f} MB")

            elif args.action == "sample":
                if not args.file:
                    parser.error("--file is required for 'sample' action")
                print(f"Sample of {args.limit} rows{' (random)' if args.random else ''}:")
                print(analyzer.sample_data(args.file, args.limit, args.random))

            elif args.action == "import":
                if not args.file:
                    parser.error("--file is required for 'import' action")
                if analyzer.table_exists(args.table):
                    if args.overwrite:
                        analyzer.conn.execute(f"DROP TABLE {args.table}")
                    else:
                        print(f"Table '{args.table}' already exists. Use --overwrite to replace it.")
                        return 1
                analyzer.import_csv_to_table(args.file, args.table)
                row_count = analyzer.count_rows(args.table)
                print(f"Imported {row_count:,} rows into table '{args.table}'")

            elif args.action == "stats":
                if not args.file:
                    parser.error("--file is required for 'stats' action")
                if not args.column:
                    parser.error("--column is required for 'stats' action")
                columns = analyzer.get_column_names(args.file)
                if args.column not in columns:
                    print(f"Error: Column '{args.column}' not found in {args.file}")
                    return 1
                stats = analyzer.get_column_stats(args.file, args.column)
                print(f"Statistics for column '{args.column}':")
                for key, value in stats.items():
                    print(f"  {key}: {value}")

            elif args.action == "schema":
                if not args.table:
                    parser.error("--table is required for 'schema' action")
                if not analyzer.table_exists(args.table):
                    print(f"Error: Table '{args.table}' does not exist")
                    return 1
                print(f"Schema for table '{args.table}':")
                print(analyzer.get_table_schema(args.table))

            elif args.action == "compression":
                if not args.table:
                    parser.error("--table is required for 'compression' action")
                if not analyzer.table_exists(args.table):
                    print(f"Error: Table '{args.table}' does not exist")
                    return 1
                print(f"Compression info for table '{args.table}':")
                print(analyzer.get_compression_info(args.table))

            elif args.action == "group":
                if not args.file:
                    parser.error("--file is required for 'group' action")
                if not args.column:
                    parser.error("--column is required for 'group' action")
                columns = analyzer.get_column_names(args.file)
                if args.column not in columns:
                    print(f"Error: Column '{args.column}' not found in {args.file}")
                    return 1
                print(f"Group by analysis of column '{args.column}':")
                groups = analyzer.group_by_analysis(args.file, args.column)
                print(groups.head(10))  # Show top 10 groups
                print(f"(Showing top 10 of {len(groups)} groups)")

            elif args.action == "query":
                if not args.sql:
                    parser.error("--sql is required for 'query' action")
                print("Executing custom query:")
                analyzer.execute_query(args.sql)

    except duckdb.IOException as e:
        print(f"IO Error: Unable to access file or database - {str(e)}")
        return 1
    except duckdb.CatalogException as e:
        print(f"Catalog Error: Table or database issue - {str(e)}")
        return 1
    except Exception as e:
        print(f"Unexpected Error: {str(e)}")
        return 1

    return 0


if __name__ == "__main__":
    exit(main())

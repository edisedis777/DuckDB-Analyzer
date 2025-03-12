"""
DuckDB Data Analysis Project
----------------------------
A tool for analyzing large CSV datasets using DuckDB.
"""
import os
import argparse
import pandas as pd
import duckdb


class DuckDBAnalyzer:
    """Class to handle DuckDB operations for large data analysis."""
    
    def __init__(self, database_name=":memory:"):
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
        
    def execute_query(self, query, show=True):
        """
        Execute a SQL query and optionally display results.
        
        Args:
            query (str): SQL query to execute
            show (bool): Whether to display the results
            
        Returns:
            DuckDB result object
        """
        result = self.conn.execute(query)
        if show:
            result.show()
        return result
    
    def count_rows(self, source_path):
        """
        Count rows in a data source.
        
        Args:
            source_path (str): Path to the data source
            
        Returns:
            int: Number of rows
        """
        query = f"SELECT COUNT(*) AS rows FROM '{source_path}'"
        result = self.execute_query(query, show=False)
        return result.fetchone()[0]
    
    def import_csv_to_table(self, csv_path, table_name, sample_rows=None):
        """
        Import CSV data into a DuckDB table.
        
        Args:
            csv_path (str): Path to CSV file
            table_name (str): Name for the new table
            sample_rows (int, optional): Limit import to a sample of rows
        """
        if sample_rows:
            query = f"CREATE TABLE {table_name} AS SELECT * FROM read_csv_auto('{csv_path}') LIMIT {sample_rows}"
        else:
            query = f"CREATE TABLE {table_name} AS SELECT * FROM read_csv_auto('{csv_path}')"
        
        self.execute_query(query, show=False)
        print(f"Imported data into table '{table_name}'")
    
    def get_table_schema(self, table_name):
        """
        Get the schema of a table.
        
        Args:
            table_name (str): Name of the table
            
        Returns:
            pandas.DataFrame: Table schema
        """
        query = f"DESCRIBE {table_name}"
        result = self.execute_query(query, show=False)
        return result.df()
    
    def get_compression_info(self, table_name):
        """
        Get compression information for a table.
        
        Args:
            table_name (str): Name of the table
            
        Returns:
            pandas.DataFrame: Compression info
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
    
    def sample_data(self, source, rows=5):
        """
        Get a sample of data.
        
        Args:
            source (str): Table name or file path
            rows (int): Number of rows to sample
            
        Returns:
            pandas.DataFrame: Sample data
        """
        query = f"SELECT * FROM '{source}' LIMIT {rows}"
        result = self.execute_query(query, show=False)
        return result.df()
    
    def get_column_stats(self, source, column_name):
        """
        Get statistics for a specific column.
        
        Args:
            source (str): Table name or file path
            column_name (str): Name of the column
            
        Returns:
            dict: Column statistics
        """
        # Handle column names with spaces
        if ' ' in column_name:
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
            'count': stats[0],
            'unique_values': stats[1],
            'min_value': stats[2],
            'max_value': stats[3]
        }
    
    def group_by_analysis(self, source, group_column, count_column="*"):
        """
        Perform a GROUP BY analysis.
        
        Args:
            source (str): Table name or file path
            group_column (str): Column to group by
            count_column (str): Column to count (default: "*")
            
        Returns:
            pandas.DataFrame: Group by results
        """
        # Handle column names with spaces
        if ' ' in group_column:
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


def get_file_size_mb(file_path):
    """Get file size in MB."""
    return os.path.getsize(file_path) / (1024 * 1024)


def main():
    """Main function to run the CLI tool."""
    parser = argparse.ArgumentParser(description="Analyze large datasets with DuckDB")
    
    parser.add_argument("--file", type=str, help="Path to the CSV file")
    parser.add_argument("--db", type=str, default=":memory:", 
                        help="Path to save the DuckDB database (default: in-memory)")
    parser.add_argument("--action", type=str, choices=[
        "count", "sample", "import", "stats", "schema", "compression", "group"
    ], required=True, help="Action to perform")
    parser.add_argument("--table", type=str, default="data", 
                        help="Table name (for import action)")
    parser.add_argument("--column", type=str, 
                        help="Column name (for stats or group action)")
    parser.add_argument("--limit", type=int, default=5, 
                        help="Limit sample rows (default: 5)")
    
    args = parser.parse_args()
    
    if not args.file and args.action != "schema" and args.action != "compression":
        parser.error("--file is required for most actions")
    
    try:
        with DuckDBAnalyzer(args.db) as analyzer:
            if args.action == "count":
                count = analyzer.count_rows(args.file)
                print(f"File contains {count:,} rows")
                print(f"File size: {get_file_size_mb(args.file):.2f} MB")
                
            elif args.action == "sample":
                print(f"Sample of {args.limit} rows:")
                print(analyzer.sample_data(args.file, args.limit))
                
            elif args.action == "import":
                analyzer.import_csv_to_table(args.file, args.table)
                row_count = analyzer.count_rows(args.table)
                print(f"Imported {row_count:,} rows into table '{args.table}'")
                
            elif args.action == "stats" and args.column:
                stats = analyzer.get_column_stats(args.file, args.column)
                print(f"Statistics for column '{args.column}':")
                for key, value in stats.items():
                    print(f"  {key}: {value}")
                    
            elif args.action == "schema" and args.table:
                print(f"Schema for table '{args.table}':")
                print(analyzer.get_table_schema(args.table))
                
            elif args.action == "compression" and args.table:
                print(f"Compression info for table '{args.table}':")
                print(analyzer.get_compression_info(args.table))
                
            elif args.action == "group" and args.column:
                print(f"Group by analysis of column '{args.column}':")
                groups = analyzer.group_by_analysis(args.file, args.column)
                print(groups.head(10))  # Show top 10 groups
                print(f"(Showing top 10 of {len(groups)} groups)")
                
    except Exception as e:
        print(f"Error: {str(e)}")
        return 1
        
    return 0


if __name__ == "__main__":
    exit(main())
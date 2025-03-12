# DuckDB-Analyzer

## A Python project for analyzing large datasets with DuckDB.

Object-Oriented Approach: DuckDB functionality wrapped in a class to make the code more organized and reusable.

### Enhanced Functionality:
- Methods for column statistics
- Improved GROUP BY analysis
- File size reporting
- Schema inspection
- Compression analysis
- Command-Line Interface: The tool can be used from the command line with various options.
- Error Handling: Uses try/except blocks to handle potential errors gracefully.
- Context Manager: Uses Python's context manager pattern with __enter__ and __exit__ for proper resource management.
- Documentation: Comprehensive docstrings for all methods and functions.

### Usage Examples:
- To count rows in a large CSV file:
bash
python duckdb_analyzer.py --file people.csv --action count

- To sample data:
bash
python duckdb_analyzer.py --file people.csv --action sample --limit 10

- To import a CSV into a persistent DuckDB database:
bash
python duckdb_analyzer.py --file people.csv --action import --db people.duckdb --table people_data

- To analyze a specific column:
bash python duckdb_analyzer.py --file people.csv --action stats --column "Job Title"

- To perform a group by analysis:
bash
Python duckdb_analyzer.py --file people.csv --action group --column "Job Title"

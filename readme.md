# RedshiftHelper

## Overview

The `RedshiftHelper` class is a Python utility for working with Amazon Redshift, a data warehousing service. This class provides methods to simplify the construction of SQL queries and operations on Redshift tables.

## Usage

### Initialization

To use the `RedshiftHelper` class, you need to initialize it by providing the necessary connection parameters:

```python
from redshift_connector import RedshiftHelper

redshift = RedshiftHelper(
    host='your_redshift_host',
    user='your_username',
    password='your_password',
    database='your_database_name',
    port=5439  # Default Redshift port
)
```

### Connecting to Redshift

You can establish a connection to the Redshift cluster using the `connect` method:

```python
redshift.connect()
```

### Building Queries

The `RedshiftHelper` class allows you to build SQL queries easily. You can specify the columns to select, add WHERE conditions, and specify GROUP BY columns:

```python
redshift.select(['column1', 'column2']).where('column3', 'value').groupBy('column4')
```

### Executing Queries

You can execute various types of queries, such as `select`, `count`, `update`, `delete`, and custom SQL queries using the respective methods:

```python
# Execute a SELECT query and fetch results as dictionaries
results = redshift.get('your_table_name')

# Perform a COUNT(*) operation
count = redshift.count('your_table_name')

# Update records in the table
redshift.update('your_table_name', {'column1': 'new_value'})

# Delete records from the table
redshift.delete('your_table_name')

# Execute a custom SQL query
redshift.raw('your_custom_sql_query')
```

### Disconnecting from Redshift

Don't forget to disconnect from the Redshift cluster when you're done:

```python
redshift.disconnect()
```

## Example

Here's a simple example of how to use the `RedshiftHelper` class:

```python
redshift = RedshiftHelper(
    host='your_redshift_host',
    user='your_username',
    password='your_password',
    database='your_database_name'
)

redshift.connect()

# Build and execute a SELECT query
results = redshift.select(['column1', 'column2']).where('column3', 'value').get('your_table_name')

print(results)

redshift.disconnect()
```

## Note

Ensure you have the `redshift_connector` library installed to use this class.

```shell
pip install redshift_connector
```

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

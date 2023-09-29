import redshift_connector


class RedshiftHelper:
    def __init__(self, host, user, password, database, port=5439):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.port = port
        self.connection = None
        self.cursor = None
        self.conditions = []
        self.selected_columns = "*"
        self.group_by_columns = None

    def connect(self):
        """
        Establish a connection to the Redshift cluster.
        """
        self.connection = redshift_connector.connect(
            host=self.host,
            database=self.database,
            user=self.user,
            password=self.password,
            port=self.port
        )
        self.cursor = self.connection.cursor()

    def disconnect(self):
        """
        Disconnect from the Redshift cluster.
        """
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()

    def select(self, columns):
        """
        Specify the columns to select in the query.
        """
        if columns:
            self.selected_columns = ", ".join(columns)
        return self  # Return self to enable method chaining

    def where(self, column, value, operator='='):
        """
        Add a WHERE condition to the query.
        """
        self.conditions.append((column, operator, value))
        return self  # Return self to enable method chaining

    def whereBetween(self, column, start_value, end_value):
        """
        Add a WHERE BETWEEN condition to the query.
        """
        self.conditions.append(
            (column, 'BETWEEN', f"'{start_value}' AND '{end_value}'"))
        return self  # Return self to enable method chaining

    def groupBy(self, *columns):
        """
        Specify columns to use in the GROUP BY clause.
        """
        self.group_by_columns = ", ".join(columns)
        return self  # Return self to enable method chaining

    def build_query(self, table_name):
        """
        Build the SQL query with the specified conditions, selected columns, and GROUP BY clause.
        """
        condition_parts = []
        for column, operator, value in self.conditions:
            condition_parts.append(f"{column} {operator} %s")

        condition_str = " AND ".join(condition_parts)

        query = f"SELECT {self.selected_columns} FROM {table_name}\n"
        if condition_str:
            query += f"WHERE {condition_str}\n"
        if hasattr(self, 'group_by_columns'):
            query += f"GROUP BY {self.group_by_columns}\n"
        return query

    def count(self, table_name):
        """
        Perform a COUNT(*) operation on the specified table with conditions.
        """
        query = self.build_query(table_name)
        self.cursor.execute(query, [val for _, val in self.conditions])
        return self.cursor.fetchone()[0]

    def sql(self, table_name):
        query = self.build_query(table_name)
        values = [val for _, _, val in self.conditions]

        # Manually construct the query with parameter values
        sql = query % tuple(values)

        return sql

    def _fetch_results(self, query):
        """
        Execute the query and fetch results as dictionaries.
        """
        self.cursor.execute(query)
        columns = [desc[0] for desc in self.cursor.description]
        results = []
        for row in self.cursor.fetchall():
            result_dict = {col: value for col, value in zip(columns, row)}
            results.append(result_dict)
        return results

    def get(self, table_name):
        """
        Execute the query with conditions and return the results as dictionaries.
        """
        query = self.sql(table_name)
        return self._fetch_results(query)

    def first(self, table_name):
        """
        Return only the first record from a given table as a dictionary.
        """
        query = f"{self.sql(table_name)} LIMIT 1;"
        results = self._fetch_results(query)
        return results[0] if results else None

    def update(self, table_name, column_values):
        """
        Update records in a given table with the specified column-value pairs and conditions.
        """
        set_columns = ", ".join(
            [f"{col} = %s" for col, _ in column_values.items()])
        query = f"UPDATE {table_name} SET {set_columns}\n"
        if self.conditions:
            condition_str = " AND ".join(
                [f"{col} = %s" for col, _ in self.conditions])
            query += f"WHERE {condition_str}\n"
        values = [val for _, val in column_values.values()] + \
            [val for _, val in self.conditions]
        self.cursor.execute(query, values)
        self.connection.commit()
        return self.cursor.rowcount

    def delete(self, table_name):
        """
        Delete records from a given table based on a condition.
        """
        query = f"DELETE FROM {table_name}\n"
        if self.conditions:
            condition_str = " AND ".join(
                [f"{col} = %s" for col, _ in self.conditions])
            query += f"WHERE {condition_str}\n"
        self.cursor.execute(query, [val for _, val in self.conditions])
        self.connection.commit()
        return self.cursor.rowcount

    def raw(self, query):
        """
        Execute a custom SQL query.
        """
        self.cursor.execute(query)
        self.connection.commit()
        return self.cursor.rowcount

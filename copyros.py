import pyodbc

def copy_rows(source_table, destination_table, condition):
    # Replace these values with your actual database connection details
    server = 'your_server'
    database = 'your_database'
    username = 'your_username'
    password = 'your_password'

    # Define the connection string
    connection_string = f'DRIVER=SQL Server;SERVER={server};DATABASE={database};UID={username};PWD={password}'

    try:
        # Connect to the database
        connection = pyodbc.connect(connection_string)
        cursor = connection.cursor()

        # Select and fetch rows based on the condition from the source table
        select_query = f'SELECT * FROM {source_table} WHERE {condition}'
        cursor.execute(select_query)
        rows_to_copy = cursor.fetchall()

        # Insert selected rows into the destination table
        for row in rows_to_copy:
            insert_query = f'INSERT INTO {destination_table} VALUES ({", ".join(map(str, row))})'
            cursor.execute(insert_query)

        # Commit the changes
        connection.commit()

        print(f'Selected rows from {source_table} copied to {destination_table} successfully.')

    except Exception as e:
        print(f'Error: {e}')

    finally:
        # Close the connection
        if connection:
            connection.close()

# Example usage:
source_table_name = 'your_source_table'
destination_table_name = 'your_destination_table'
condition = 'your_condition'  # e.g., 'column_name = value'

copy_rows(source_table_name, destination_table_name, condition)

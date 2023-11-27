import pyodbc

# Set your database connection parameters
server = 'your_server'
database = 'your_database'
username = 'your_username'
password = 'your_password'
source_table = 'source_table'
destination_table = 'destination_table'

# Establish a connection to the database
conn_str = f'DRIVER={{SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'
connection = pyodbc.connect(conn_str)
cursor = connection.cursor()

try:
    # Disable identity insert on the destination table
    cursor.execute(f'SET IDENTITY_INSERT {destination_table} ON')

    # Fetch data from the source table
    cursor.execute(f'SELECT * FROM {source_table}')
    rows = cursor.fetchall()

    # Insert data into the destination table
    for row in rows:
        # Assuming the columns in the source and destination tables have the same order
        values = ', '.join([f"'{value}'" if isinstance(value, str) else str(value) for value in row])
        cursor.execute(f'INSERT INTO {destination_table} VALUES ({values})')

    # Commit the changes
    connection.commit()

finally:
    # Re-enable identity insert on the destination table
    cursor.execute(f'SET IDENTITY_INSERT {destination_table} OFF')

    # Close the cursor and connection
    cursor.close()
    connection.close()

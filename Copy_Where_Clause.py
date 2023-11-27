from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Create a Spark session
spark = SparkSession.builder \
    .appName("MSSQL Table Move") \
    .config("spark.jars", "/path/to/sqljdbc42.jar")  # Specify the path to your JDBC driver JAR file
    .getOrCreate()

# Configure MSSQL connection properties
mssql_properties = {
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
    "url": "jdbc:sqlserver://<server>:<port>;databaseName=<database>",
    "user": "<username>",
    "password": "<password>"
}

# Define the WHERE clause
where_clause = "your_condition_here"

# Read data from the source MSSQL table with a WHERE clause
source_table = "(SELECT * FROM source_table WHERE {}) as source_table_alias".format(where_clause)
source_df = spark.read.jdbc(url=mssql_properties["url"],
                            table=source_table,
                            properties=mssql_properties)

# Transform the data if needed
# For example, you can add a new column or filter rows
transformed_df = source_df.withColumn("new_column", col("existing_column") * 2)

# Write the transformed data to the destination MSSQL table
destination_table = "destination_table"
transformed_df.write.jdbc(url=mssql_properties["url"],
                           table=destination_table,
                           mode="overwrite",  # You can use "append" or "overwrite" as needed
                           properties=mssql_properties)

# Stop the Spark session
spark.stop()

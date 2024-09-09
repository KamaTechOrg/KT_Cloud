from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as spark_sum, rank, current_timestamp, lit, date_format
from pyspark.sql.window import Window
import sqlite3
import pandas as pd

def etl_process():
    # Initialize Spark session
    spark = SparkSession.builder.appName("CustomerLTVByRegionETL").getOrCreate()

    # Open SQLite connection
    conn = sqlite3.connect("C:\\Users\\User\\Desktop\\p_database.db")

    try:
        # Create customer_ltv table if it doesn't exist
        conn.execute("""
            CREATE TABLE IF NOT EXISTS customer_ltv (
                CustomerId INTEGER,
                FirstName TEXT,
                LastName TEXT,
                Country TEXT,
                CustomerLTV REAL,
                Rank INTEGER,
                created_at TEXT,
                updated_at TEXT,
                updated_by TEXT,
                PRIMARY KEY (CustomerId, Country)
            )
        """)

        # E - Extraction: Read CSV files
        customers_df = spark.read.csv("C:\\Users\\User\\Desktop\\Customer.csv", header=True, inferSchema=True)
        invoice_lines_df = spark.read.csv("C:\\Users\\User\\Desktop\\InvoiceLine.csv", header=True, inferSchema=True)
        invoices_df = spark.read.csv("C:\\Users\\User\\Desktop\\Invoice.csv", header=True, inferSchema=True)

        # T - Transformation: Join customers with invoice data
        joined_df = customers_df.join(invoices_df, "CustomerId").join(invoice_lines_df, "InvoiceId")

        # Group by region (or use 'Country') and customer
        region_customer_df = joined_df.groupBy("Country", "CustomerId", "FirstName", "LastName") \
            .agg(spark_sum("Total").alias("CustomerLTV"))

        # Define window for ranking customers by LTV within each region (Country in this case)
        window_spec = Window.partitionBy("Country").orderBy(region_customer_df["CustomerLTV"].desc())

        # Apply ranking
        ranked_df = region_customer_df.withColumn("Rank", rank().over(window_spec))

        # Add metadata columns
        ranked_df = ranked_df.withColumn("created_at", date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss")) \
                             .withColumn("updated_at", date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss")) \
                             .withColumn("updated_by", lit("process:user_name"))

        # Convert Spark DataFrame to pandas
        result_pandas = ranked_df.toPandas()

        # Perform full loading into SQLite
        for index, row in result_pandas.iterrows():
            cursor = conn.execute("SELECT * FROM customer_ltv WHERE CustomerId = ? AND Country = ?",
                                  (row['CustomerId'], row['Country']))
            data = cursor.fetchone()

            if data:
                # Update existing record
                conn.execute("""
                    UPDATE customer_ltv
                    SET CustomerLTV = ?, Rank = ?, updated_at = ?, updated_by = ?
                    WHERE CustomerId = ? AND Country = ?
                """, (row['CustomerLTV'], row['Rank'], row['updated_at'], row['updated_by'], row['CustomerId'], row['Country']))
            else:
                # Insert new record
                row.to_frame().T.to_sql('customer_ltv', conn, if_exists='append', index=False)

        # Commit the transaction
        conn.commit()

    finally:
        # Stop the Spark session
        spark.stop()
        # Close the SQLite connection
        conn.close()

etl_process()

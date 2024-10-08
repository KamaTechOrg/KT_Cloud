from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import pandas as pd

import sqlite3

def load():
    
    # configure basic variables
    current_user = "User" # when IAM is implemented, get current user for session details
    
    etl_table_name = 'revenue_per_customer_genre'
    
    base_path = "../etl_files/"
    
    # establish sparkSession
    spark = SparkSession.builder.appName("ETL Template with SQLite").getOrCreate()
    
    
    try:
        # connect to sqlite database
        conn = sqlite3.connect(base_path + "database.db")
        
        # extract data from csv files
        # only consider active invoice_lines toward total
        invoice_table = spark.read.option("header", "true").csv(base_path + "Invoice.csv", 
                                                                header=True, inferSchema=True)
        invoice_line_table = spark.read.option("header", "true").csv(base_path + "InvoiceLine.csv", 
                                                                header=True, inferSchema=True)
        invoice_line_table = invoice_line_table.filter(invoice_line_table['status'] == F.lit('active'))
        track_table = spark.read.option("header", "true").csv(base_path + "Track.csv", 
                                                                header=True, inferSchema=True)
        
        # assign customerId to each invoice line
        customer_invoice_line = invoice_table.join(invoice_line_table, invoice_table[ \
            'InvoiceId'] == invoice_line_table['InvoiceId']).drop(invoice_line_table['InvoiceId'])
        
        # assign the tracks a customer purchased to their id, and calculate how much they paid for each 
        # invoice line
        customer_track_table = customer_invoice_line.join(track_table, customer_invoice_line['TrackId'] == \
            track_table['TrackId']).drop(customer_invoice_line['TrackId']).drop(track_table['UnitPrice'])\
        .withColumn(
            'invoice_line_total', 
            F.col('UnitPrice') * F.col('Quantity')
        )    
        
        # group the overall payment for each customer in each genre
        final_data = customer_track_table.groupBy("CustomerId", "GenreId").agg(
            F.sum("invoice_line_total").alias("revenue_overall")
        ).withColumn("created_at", F.current_date()) \
            .withColumn("updated_at", F.current_date()) \
            .withColumn("updated_by", F.lit(f"WeeklyCustomerRevenuePerGenre:{current_user}"))

        # load data to database
        final_data = final_data.toPandas()
        
        final_data.to_sql(name=etl_table_name, con = conn, if_exists='replace', index=False)   

        
        
    finally:
        conn.close()
        spark.stop()

# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Initialize Spark session
spark = SparkSession.builder.appName("DeltaReadExample").getOrCreate()

# Read data in Delta format
orders = spark.read.format("delta").load("dbfs:/user/hive/warehouse/orders")
customers = spark.read.format("delta").load("dbfs:/user/hive/warehouse/customers")


# COMMAND ----------

# Spark SQL Query: Get top 10 customers who purchased most orders
top_ordered = spark.sql(
    """
    SELECT customerName, COUNT(O.orderNumber) as orderNum
    FROM customers AS C
    LEFT JOIN orders AS O ON C.customerNumber = O.customerNumber
    GROUP BY customerName
    HAVING customerName IS NOT NULL
    ORDER BY orderNum DESC
    LIMIT 10
"""
)

# COMMAND ----------

# Data Transformation: Calculate total customer for each state
customer_data = (
    customers.filter(customers.country == "USA")
    .groupBy("state")
    .agg(count("customerNumber").alias("total_customer"))
    .orderBy(desc("total_customer"))
)


# COMMAND ----------


from tabulate import tabulate

# Convert top_ordered DataFrame to Pandas DataFrame
top_ordered_pandas = top_ordered.toPandas()

# Convert customer_data DataFrame to Pandas DataFrame
customer_data_pandas = customer_data.toPandas()

# Convert Pandas DataFrame to Markdown format
top_ordered_md = tabulate(top_ordered_pandas, tablefmt="pipe", headers="keys")
customer_data_md = tabulate(customer_data_pandas, tablefmt="pipe", headers="keys")

# Write to result.md
with open("result.md", "w") as f:
    f.write("# Top Ordered Customers\n")
    f.write(top_ordered_md)
    f.write("\n\n# Total Customers for Each State\n")
    f.write(customer_data_md)

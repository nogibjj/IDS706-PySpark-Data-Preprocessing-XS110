from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Initialize Spark session
spark = SparkSession.builder.appName("E-commerce Data Processing").getOrCreate()

# Read the dataset
order_df = spark.read.csv("order.csv", header=True, inferSchema=True)
customer_df = spark.read.csv("customer.csv", header=True, inferSchema=True)


# Clean data: Remove rows with missing values
cleaned_data = data_c.dropna()

# # Spark SQL Query: Get top 10 most viewed products
# cleaned_data.createOrReplaceTempView("ecommerce_data")
# top_viewed = spark.sql("""
#     SELECT product_id, COUNT(*) as views 
#     FROM ecommerce_data 
#     WHERE action = 'view' 
#     GROUP BY product_id 
#     ORDER BY views DESC 
#     LIMIT 10
# """)

# # Data Transformation: Calculate total revenue for each product
# revenue_data = cleaned_data.filter(cleaned_data.action == "purchase") \
#                            .groupBy("product_id") \
#                            .agg(sum("product_price").alias("total_revenue")) \
#                            .orderBy(desc("total_revenue"))

# # Save the results to CSV (or any other desired format)
# top_viewed.write.csv("top_viewed_products.csv")
# revenue_data.write.csv("product_revenues.csv")

# # Stop the Spark session
# spark.stop()

# if __name__ == "__main__":
#     # pylint: disable=no-value-for-parameter
#     descript_stat(df)
#     plot_histogram(df)

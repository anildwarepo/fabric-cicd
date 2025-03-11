# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "ab88234c-5134-4115-8ff3-e780d45740bd",
# META       "default_lakehouse_name": "anildwalakehouse",
# META       "default_lakehouse_workspace_id": "b1a1dad3-61f0-4438-be14-1651717fcaf7"
# META     },
# META     "environment": {
# META       "environmentId": "0088472a-88b0-b63b-48b3-567428c9b318",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# CELL ********************

print("hello")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# Define schema based on CSV columns
event_schema = StructType([
    StructField("cs_sold_date_sk", IntegerType(), True),
    StructField("cs_sold_time_sk", IntegerType(), True),
    StructField("cs_ship_date_sk", IntegerType(), True),
    StructField("cs_bill_customer_sk", IntegerType(), True),
    StructField("cs_bill_cdemo_sk", IntegerType(), True),
    StructField("cs_bill_hdemo_sk", IntegerType(), True),
    StructField("cs_bill_addr_sk", IntegerType(), True),
    StructField("cs_ship_customer_sk", IntegerType(), True),
    StructField("cs_ship_cdemo_sk", IntegerType(), True),
    StructField("cs_ship_hdemo_sk", IntegerType(), True),
    StructField("cs_ship_addr_sk", IntegerType(), True),
    StructField("cs_call_center_sk", IntegerType(), True),
    StructField("cs_catalog_page_sk", IntegerType(), True),
    StructField("cs_ship_mode_sk", IntegerType(), True),
    StructField("cs_warehouse_sk", IntegerType(), True),
    StructField("cs_item_sk", IntegerType(), True),
    StructField("cs_promo_sk", IntegerType(), True),
    StructField("cs_order_number", IntegerType(), True),
    StructField("cs_quantity", IntegerType(), True),
    StructField("cs_wholesale_cost", DoubleType(), True),
    StructField("cs_list_price", DoubleType(), True),
    StructField("cs_sales_price", DoubleType(), True),
    StructField("cs_ext_discount_amt", DoubleType(), True),
    StructField("cs_ext_sales_price", DoubleType(), True),
    StructField("cs_ext_wholesale_cost", DoubleType(), True),
    StructField("cs_ext_list_price", DoubleType(), True),
    StructField("cs_ext_tax", DoubleType(), True),
    StructField("cs_coupon_amt", DoubleType(), True),
    StructField("cs_ext_ship_cost", DoubleType(), True),
    StructField("cs_net_paid", DoubleType(), True),
    StructField("cs_net_paid_inc_tax", DoubleType(), True),
    StructField("cs_net_paid_inc_ship", DoubleType(), True),
    StructField("cs_net_paid_inc_ship_tax", DoubleType(), True),
    StructField("cs_net_profit", DoubleType(), True),
    StructField("session_id", StringType(), True)
])


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#workspace identity is granted Blob Data Storage Contributor permissions and a shortcut is created using workspace identity. 
lakehouse_path = "abfss://fabriccontainer@anildwafabricadlsgen2.dfs.core.windows.net/tables/event_data/"
checkpoint_location = "abfss://fabriccontainer@anildwafabricadlsgen2.dfs.core.windows.net/tables/checkpoint/"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_stream = (spark.readStream
    .format("delta")
    .option("ignoreDeletes", "true")  # Ignore deleted records (if any)
    .option("ignoreChanges", "true")  # Ignore updates, only new rows
    .option("checkpointLocation", "abfss://fabriccontainer@anildwafabricadlsgen2.dfs.core.windows.net/checkpoints/reader/")  # Ensure progress tracking
    .load(lakehouse_path))

# Display streaming data
query_display = (df_stream
    .writeStream
    .outputMode("append")   # Append new records
    .format("console")
    .option("truncate", False)
    .trigger(processingTime="5 seconds")  # Force micro-batch every 5s
    .start())

query_display.awaitTermination()



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(query_display.status)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

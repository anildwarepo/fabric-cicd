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
# META       "environmentId": "3a231dbc-95d2-be92-4ad4-f949f1af768a",
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

from azure.identity import DefaultAzureCredential, ManagedIdentityCredential

from azure.eventhub import EventHubConsumerClient

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%configure -f
# MAGIC {
# MAGIC     "conf": {
# MAGIC         "spark.jars.packages": "com.microsoft.azure:azure-eventhubs-spark_2.12:2.3.18,com.azure:azure-identity:1.4.1"
# MAGIC     }
# MAGIC }


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# Define schema based on CSV columns
data_event_schema  = StructType([
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


# Define the schema for the event data
event_schema = StructType([
    StructField("eventId", StringType(), True), 
    StructField("timestamp", StringType(), True), 
    StructField("data", StringType(), True)  # JSON as String
])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from azure.identity import DefaultAzureCredential, ManagedIdentityCredential
from azure.eventhub import EventHubConsumerClient

# Define your Event Hub namespace and name
event_hub_namespace = "anildwaeventhubns"
event_hub_name = "eh"
client_id = "e13258c5-3535-49ce-b8bb-1d191507b5a9"

# Create a token credential using the Fabric workspace identity
credential = DefaultAzureCredential(managed_identity_client_id=client_id)  


# Create a consumer client for the Event Hub using the Azure AD credential
consumer = EventHubConsumerClient(
    fully_qualified_namespace=f"{event_hub_namespace}.servicebus.windows.net",
    eventhub_name=event_hub_name,
    consumer_group="sparkstreaming",
    credential=credential    # Authenticate with managed identity token
)

# Receive events (simple example: print incoming event messages)
with consumer:
    consumer.receive(
        on_event=lambda partition_context, event: print("Received: ", event.body_as_str()),
        starting_position="@latest"  # start from beginning of stream; use "@latest" for newest
    )


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StringType, StructType, StructField

# Initialize Spark session
spark = SparkSession.builder.appName("EventHubStreaming").getOrCreate()

# Event Hub Connection details
event_hub_connection_string = ""
event_hub_name = "eh"

# Event Hubs Configuration
eh_conf = {
    "eventhubs.connectionString": spark._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(event_hub_connection_string),
    "eventhubs.consumerGroup": "sparkstreaming",
    "eventhubs.startingPosition": """{"offset": "-1", "seqNo": -1, "enqueuedTime": null, "isInclusive": true}"""  # Start from latest
}

eh_conf2 = {
    "eventhubs.eventHubName": "eh",
    "eventhubs.namespace": "anildwaeventhubns",
    "eventhubs.authType": "MSI"  # Use Managed Identity authentication
}

# Read from Event Hub as a structured stream
df = (spark.readStream
    .format("eventhubs")
    .options(**eh_conf2)
    .load())

# Define the schema for the event data
event_schema = StructType([
    StructField("eventId", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("data", StringType(), True)
])

# Deserialize JSON data
df_parsed = (df
    .withColumn("body", col("body").cast("string"))
    .withColumn("parsed", from_json(col("body"), data_event_schema))
    .select("parsed.*"))

#workspace identity is granted Blob Data Storage Contributor permissions and a shortcut is created using workspace identity. 
lakehouse_path = "abfss://fabriccontainer@anildwafabricadlsgen2.dfs.core.windows.net/tables/event_data/"
checkpoint_location = "abfss://fabriccontainer@anildwafabricadlsgen2.dfs.core.windows.net/tables/checkpoint/"

query = (df_parsed.writeStream
    .format("delta")
    .outputMode("append")
    .option("path", lakehouse_path)  # Path to Delta table in Lakehouse
    .option("checkpointLocation", checkpoint_location)  # Checkpoint location
    .start())

query.awaitTermination()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

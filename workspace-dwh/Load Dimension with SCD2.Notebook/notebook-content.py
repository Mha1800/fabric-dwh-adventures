# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# ## Load the SCD Type 2 Class

# CELL ********************

%run "SCDHandler program"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from datetime import datetime

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Set parameters

# MARKDOWN ********************

# param_table_dim : the name of the table without 'dim' extention
# param_surogate_key : the surogate key field name 
# param_business_key : the business key field name
# param_sample_date : the sample date of the snapshot of the source

# PARAMETERS CELL ********************

param_table_dim="customer"
param_surogate_key="customer_rk"
param_business_key="CustomerId"
param_sample_date='20250201'


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Load the source data from staging "stg"

# CELL ********************

source_df = spark.read.format("delta").load("abfss://MhaWorkspace2@onelake.dfs.fabric.microsoft.com/MhaWarehouse2.warehouse/Tables/stg/"+param_table_dim)
#source_df = source_df.withColumnRenamed("CustomerKey", "CustomerId")
#source_df = source_df.withColumn("CustomerId", source_df["CustomerId"].cast('integer'))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Load the target data from datawarehouse "dwh"

# CELL ********************

target_df = spark.read.format("delta").load("abfss://MhaWorkspace2@onelake.dfs.fabric.microsoft.com/MhaWarehouse2.warehouse/Tables/dwh/"+param_table_dim+"_dim")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Apply the scd type 2

# CELL ********************

scd=SCDHandler(with_version=False,surogate_key=param_surogate_key,business_key=param_business_key,processed_date='processed_dttm')


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sample_date_formated=datetime.strptime(param_sample_date, "%Y%m%d").date().strftime("%Y-%m-%d")+" 00:00:00"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

result=scd.scd_2(source_df,target_df,sample_date_formated)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Overwrite the full table in the DWH

# CELL ********************

#display(result['full'])
from com.microsoft.spark.fabric import *
from com.microsoft.spark.fabric.Constants import *
result['full'].write.mode("overwrite").synapsesql("MhaWarehouse2.dwh."+param_table_dim+"_dim")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

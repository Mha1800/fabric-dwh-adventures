# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "f8f54cac-7c7f-4123-9353-3030d3847797",
# META       "default_lakehouse_name": "StagingLakehouseForDataflows_20250812115152",
# META       "default_lakehouse_workspace_id": "6db8acd3-f954-465c-b74e-6c155204510c",
# META       "known_lakehouses": [
# META         {
# META           "id": "f8f54cac-7c7f-4123-9353-3030d3847797"
# META         },
# META         {
# META           "id": "c399e8e0-1f84-4e46-a6c9-25d247e30131"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ## Import data

# MARKDOWN ********************

# ### Load data from ods

# CELL ********************

%pip install utils

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pandas as pd
from pandas import DataFrame
from pyspark.sql.functions import to_timestamp
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import *


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


source_df = spark.read.format("delta").load("abfss://MhaWorkspace2@onelake.dfs.fabric.microsoft.com/MhaWarehouse2.warehouse/Tables/ods/territory")
source_df = source_df.withColumnRenamed("SalesTerritoryKey", "TerritoryId")
source_df = source_df.withColumn("TerritoryId", source_df["TerritoryId"].cast('integer'))
display(source_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

target_df = spark.read.format("delta").load("abfss://MhaWorkspace2@onelake.dfs.fabric.microsoft.com/MhaWarehouse2.warehouse/Tables/dwh/territory_dim")
display(target_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

data_src = """CustomerId,customer_nm,city_nm,postal_cd,status_cd
1,Mohamed,Diegem,1800,1
3,Peter,Vilvoorde,1802,1
4,Lio,Bruxelles,1090,1
2,Thierry,Machelen,1810,0
5,Edmon,Bruxelles,1070,1
6,Joe,Gent,8200,1"""

# Split and prepare data
lines = data_src.split("\n")
header = lines[0].split(",")
data = [line.split(",") for line in lines[1:]]

# Create DataFrame
source_df = spark.createDataFrame(data, schema=header)

source_df = source_df.withColumn("CustomerId", source_df["CustomerId"].cast('integer'))
display(source_df)


data_dim="""customer_rk,CustomerId,customer_nm,city_nm,postal_cd,status_cd,valid_from_dttm,valid_to_dttm,process_dttm,version_no
1001,1,Mohamed,Diegem,1830,1,2020-11-23 00:00:00,2025-01-10 00:00:00,2025-07-10 15:43:25,1
1001,1,Mohamed,Diegem,1830,1,2025-01-10 00:00:00,9999-12-31 23:59:59,2025-07-10 15:43:25,2
1002,2,Thierry,Machelen,1810,0,2025-01-10 00:00:00,9999-12-31 23:59:59,2025-07-10 15:43:25,1
1003,3,Peter,Vilvoorde,1800,1,2025-01-10 00:00:00,9999-12-31 23:59:59,2025-07-10 15:43:25,1"""

# Split and prepare data
lines = data_dim.split("\n")
header = lines[0].split(",")
data = [line.split(",") for line in lines[1:]]

# Create DataFrame
target_df = spark.createDataFrame(data, schema=header)

target_df = target_df.withColumn("valid_to_dttm", target_df["valid_to_dttm"].cast('timestamp'))
target_df = target_df.withColumn("valid_from_dttm", target_df["valid_from_dttm"].cast('timestamp'))
target_df = target_df.withColumn("process_dttm", target_df["process_dttm"].cast('timestamp'))
target_df = target_df.withColumn("version_no", target_df["version_no"].cast('integer'))
target_df = target_df.withColumn("CustomerId", target_df["CustomerId"].cast('integer'))
target_df = target_df.withColumn("customer_rk", target_df["customer_rk"].cast('integer'))
display(target_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Define constants
sample_date = '2025-05-01 00:00:00'
surogate_key='customer_rk'
business_key= ['CustomerId']

version_no='version_no'
start_date = 'valid_from_dttm'
end_date = 'valid_to_dttm'
processed_date = 'process_dttm'
infinite_date = '9999-12-31 23:59:59'
#timestamp_format= '%Y-%m-%d %H:%M:%S'
with_version = True





# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run "SCDHandler program"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************



param_table_dim="customer"
param_surogate_key="customer_rk"
param_business_key="CustomerId"

scd=SCDHandler(with_version=False,surogate_key=param_surogate_key,business_key=param_business_key,processed_date='processed_dttm')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


scd.check_columns_presence(source_df, target_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if type(business_key) is not list:
    business_key = [business_key]

tgt_cols = [x for x in target_df.columns]

#target_df =target_df.filter((target_df[start_date] <= F.lit(sample_date)) & (target_df[end_date] >= F.lit(sample_date)))

# Apply hash calculation and alias
source_df, target_df = scd.apply_hash_and_alias(source_df, target_df)
max_surogate_key=target_df.agg(F.ifnull(F.max(surogate_key),lit(1000))).collect()[0][0]
print('max_surogate_key=',max_surogate_key)
# Identify new records
join_cond = [source_df[join_key] == target_df[join_key] for join_key in business_key]
new_df = source_df.join(target_df, join_cond, 'left_anti')
w=Window.orderBy(business_key)
new_df = new_df.withColumn(version_no,lit(None)) \
        .withColumn(surogate_key,lit(row_number().over(w) +  max_surogate_key)) 
print("New from source:")
display(new_df)

#cur_target_records=target_df.filter((target_df[start_date] <= F.lit(sample_date)) & (target_df[end_date] >= F.lit(sample_date)))

base_df = target_df.join(source_df, join_cond, 'left')
#base_df = base_df.filter(base_df[start_date] <= F.lit(sample_date)) & (base_df[end_date] >= F.lit(sample_date))
print("Base_df:")
display(base_df)
# Filter unchanged records or same records
unchanged_filter_expr = " AND ".join([f"source_df.{key} IS NULL" for key in business_key])
unchanged_df = base_df.filter(f"({unchanged_filter_expr}) OR "
                                f"(source_df.hash_value = target_df.hash_value) OR (target_df.{end_date} < '{sample_date}')") \
    .select("target_df.*")


print("Unchanged in the dimension:")
display(unchanged_df)

# identify updated records

delta_filter_expr = " and ".join([f"source_df.{key} IS NOT NULL" for key in business_key])
updated_df = base_df.filter(f"{delta_filter_expr} AND "
                            f"source_df.hash_value != target_df.hash_value AND (target_df.{start_date} <= '{sample_date}' AND target_df.{end_date} >= '{sample_date}')")

# pick updated records from source_df for new entry
print("To be updated in the dimension:")
updated_new_df = updated_df.select("source_df.*",f"target_df.{version_no}",f"target_df.{surogate_key}")
display(updated_new_df)

# pick updated records from target_df for obsolete entry
print("Closed records:")
obsolete_df = updated_df.select("target_df.*") \
    .withColumn(end_date, F.to_timestamp(F.lit(sample_date),'yyyy-MM-dd HH:mm:ss')) \
    .withColumn(processed_date,lit(F.to_timestamp(current_date())))
    #.withColumn("flag", lit(0))
display(obsolete_df)

# union : new & updated records and add scd2 meta-deta

delta_df = new_df.union(updated_new_df) \
    .withColumn(start_date, F.to_timestamp(lit(sample_date),'yyyy-MM-dd HH:mm:ss')) \
    .withColumn(end_date,  F.to_timestamp(lit(infinite_date))) \
    .withColumn(processed_date,lit(F.to_timestamp(current_date()))) \
    .withColumn(version_no,when(col(version_no).isNotNull(), col(version_no) + 1).otherwise(lit(1)))
print("Records to add in the dimension:")
display(delta_df)

# union all datasets : delta_df + obsolete_df + unchanged_df
result_df = unchanged_df.select(tgt_cols). \
    unionByName(delta_df.select(tgt_cols)). \
    unionByName(obsolete_df.select(tgt_cols))

result_df=result_df.drop('hash_value').orderBy(business_key+[start_date])
display(result_df)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from datetime import datetime
sample_date='20250501'
# Convert string to date object
print("date=",datetime.strptime(sample_date, "%Y%m%d").date().strftime("%Y-%m-%d"))

# Convert to string
#date_string = now.strftime("%Y-%m-%d %H:%M:%S")
#updated_df = base_df.filter(f"{delta_filter_expr} AND "
#                            f"source_df.hash_value != target_df.hash_value AND (target_df.{start_date} <= to_date('{sample_date}', {'yyyyMMdd'}) AND target_df.{end_date} >= to_date('{sample_date}', {'yyyyMMdd'}))")
#display(updated_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import DataFrame
from pyspark.sql.functions import concat_ws, md5, col, current_date, lit,max, ifnull, to_timestamp, row_number, when
from pyspark.sql.window import Window
#from pyspark.sql.functions import F
#from utils.logger import Logger
#from utils.spark_session import SparkSessionManager


class SCDHandler:
    scd_technical_columns=[]
    #timestamp_format= '%Y-%m-%d %H:%M:%S'        
    def __init__(self,with_version=True,
                    version_no='version_no',
                    start_date = 'valid_from_dttm',
                    end_date = 'valid_to_dttm',
                    processed_date = 'process_dttm',
                    infinite_date = '9999-12-31 23:59:59',
                    surogate_key='',
                    business_key=[]):
#        self.spark = SparkSessionManager(self.__class__.__name__).create_session()
#        self.logger = Logger(self.__class__.__name__)
        self.surogate_key=surogate_key
        if type(business_key) is not list:
            self.business_key = [business_key]
        else:
            self.business_key=business_key
        self.scd_technical_columns.extend([surogate_key,start_date,end_date,processed_date])
        
        if(with_version):
            self.scd_technical_columns.append(version_no)
        
        self.with_version = with_version
        self.version_no = version_no
        self.start_date = start_date
        self.end_date = end_date
        self.processed_date = processed_date
        self.infinite_date = infinite_date

    def check_columns_presence(self, source_df, target_df):
        """
        Check if all columns from the target DataFrame are present in the source DataFrame.

        Args:
            source_df (pyspark.sql.DataFrame): Source DataFrame.
            target_df (pyspark.sql.DataFrame): Target DataFrame.

        Raises:
            Exception: If columns are missing in the source DataFrame.

        Returns:
            None
        """
        cols_missing = set([cols for cols in target_df.columns if cols not in source_df.columns]) - set(self.scd_technical_columns)
        if cols_missing:
            raise Exception(f"Cols missing in source DataFrame: {cols_missing}")

    def apply_hash_and_alias(self, source_df, target_df) -> ([DataFrame, DataFrame]):
        """
        Apply hash calculation and alias to source and target DataFrames.

        Args:
            source_df (pyspark.sql.DataFrame): Source DataFrame.
            target_df (pyspark.sql.DataFrame): Target DataFrame.
            

        Returns:
            tuple: Tuple containing aliased source DataFrame and aliased target DataFrame.
        """
        # Extract columns from target DataFrame excluding metadata columns
        tgt_cols = [x for x in target_df.columns if x not in self.scd_technical_columns]
        print("tgt_cols",tgt_cols)
        print("scd_technical_columns",self.scd_technical_columns)
        # Calculate hash expression
        hash_expr = md5(concat_ws("|", *[col(c) for c in tgt_cols]))

        # Apply hash calculation and alias to source and target DataFrames
        source_df = source_df.withColumn("hash_value", hash_expr).alias("source_df")
        target_df = target_df.withColumn("hash_value", hash_expr).alias("target_df")

        return source_df, target_df


    def scd_2(self, source_df, target_df, sample_date) -> ({"full":DataFrame,"insert":DataFrame,"close":DataFrame}):
       

        tgt_cols = [x for x in target_df.columns]

        #target_df =target_df.filter((target_df[start_date] <= F.lit(sample_date)) & (target_df[end_date] >= F.lit(sample_date)))

        # Apply hash calculation and alias
        source_df, target_df = self.apply_hash_and_alias(source_df, target_df)
        max_surogate_key=target_df.agg(ifnull(max(self.surogate_key),lit(1000))).collect()[0][0]
        
        # Identify new records
        join_cond = [source_df[join_key] == target_df[join_key] for join_key in self.business_key]
        new_df = source_df.join(target_df, join_cond, 'left_anti')
        w=Window.orderBy(self.business_key)
        new_df = new_df.withColumn(self.version_no,lit(None)) \
                .withColumn(self.surogate_key,lit(row_number().over(w) +  max_surogate_key)) 
        
        base_df = target_df.join(source_df, join_cond, 'left')
        
        # Filter unchanged records or same records
        unchanged_filter_expr = " AND ".join([f"source_df.{key} IS NULL" for key in self.business_key])
        unchanged_df = base_df.filter(f"({unchanged_filter_expr}) OR "
                                        f"(source_df.hash_value = target_df.hash_value) OR (target_df.{self.end_date} < '{sample_date}')") \
            .select("target_df.*")

        # identify updated records

        delta_filter_expr = " and ".join([f"source_df.{key} IS NOT NULL" for key in self.business_key])
        updated_df = base_df.filter(f"{delta_filter_expr} AND "
                                    f"source_df.hash_value != target_df.hash_value AND (target_df.{self.start_date} <= '{sample_date}' AND target_df.{self.end_date} >= '{sample_date}')")

        # pick updated records from source_df for new entry
        updated_new_df = updated_df.select("source_df.*",f"target_df.{self.version_no}",f"target_df.{self.surogate_key}")

        # pick updated records from target_df for obsolete entry
        obsolete_df = updated_df.select("target_df.*") \
            .withColumn(self.end_date, to_timestamp(lit(sample_date),'yyyy-MM-dd HH:mm:ss')) \
            .withColumn(self.processed_date,lit(to_timestamp(current_date())))
            

        # union : new & updated records and add scd2 meta-deta

        delta_df = new_df.union(updated_new_df) \
            .withColumn(self.start_date, to_timestamp(lit(sample_date),'yyyy-MM-dd HH:mm:ss')) \
            .withColumn(self.end_date,  to_timestamp(lit(self.infinite_date))) \
            .withColumn(self.processed_date,lit(to_timestamp(current_date()))) \
            .withColumn(self.version_no,when(col(self.version_no).isNotNull(), col(self.version_no) + 1).otherwise(lit(1)))

        # union all datasets : delta_df + obsolete_df + unchanged_df
        result_df = unchanged_df.select(tgt_cols). \
            unionByName(delta_df.select(tgt_cols)). \
            unionByName(obsolete_df.select(tgt_cols))

        result_df=result_df.drop('hash_value').orderBy(self.business_key+[self.start_date])
        
        delta_df=delta_df.drop('hash_value').orderBy(self.business_key+[self.start_date])
        obsolete_df=obsolete_df.drop('hash_value').orderBy(self.business_key+[self.start_date])
        return {"full":result_df,"insert":delta_df,"close":obsolete_df}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run "SCDHandler program"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

scdTest=SCDHandler(with_version=True,business_key="CustomerId",surogate_key="customer_rk")
res=scdTest.scd_2(source_df,target_df,"2025-05-01 00:00:00")
display(res['insert'])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

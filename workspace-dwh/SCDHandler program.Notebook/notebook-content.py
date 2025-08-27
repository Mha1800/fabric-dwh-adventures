# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
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
        
        new_df = new_df.withColumn(self.surogate_key,lit(row_number().over(w) +  max_surogate_key)) 
        if (self.with_version):
            new_df = new_df.withColumn(self.version_no,lit(None))
        
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
        if (self.with_version):
            updated_new_df = updated_df.select("source_df.*",f"target_df.{self.version_no}",f"target_df.{self.surogate_key}")
        else:
            updated_new_df = updated_df.select("source_df.*",f"target_df.{self.surogate_key}")
        

        # pick updated records from target_df for obsolete entry
        obsolete_df = updated_df.select("target_df.*") \
            .withColumn(self.end_date, to_timestamp(lit(sample_date),'yyyy-MM-dd HH:mm:ss')) \
            .withColumn(self.processed_date,lit(to_timestamp(current_date())))
            

        # union : new & updated records and add scd2 meta-deta

        delta_df = new_df.union(updated_new_df) \
            .withColumn(self.start_date, to_timestamp(lit(sample_date),'yyyy-MM-dd HH:mm:ss')) \
            .withColumn(self.end_date,  to_timestamp(lit(self.infinite_date))) \
            .withColumn(self.processed_date,lit(to_timestamp(current_date())))
        
        if (self.with_version):
            delta_df = delta_df.withColumn(self.version_no,when(col(self.version_no).isNotNull(), col(self.version_no) + 1).otherwise(lit(1)))

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

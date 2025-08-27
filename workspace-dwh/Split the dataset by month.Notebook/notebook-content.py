# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

from pyspark.sql.functions import to_date,split,make_date,month,year,lit,concat,lpad,date_add

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


lakehouse_path = "abfss://MhaWorkspace2@onelake.dfs.fabric.microsoft.com/MhaLakehouse2.Lakehouse/"

sales_df = spark.read.format("delta").load("abfss://MhaWorkspace2@onelake.dfs.fabric.microsoft.com/MhaWarehouse2.warehouse/Tables/ods/sales")
customer_df = spark.read.format("delta").load("abfss://MhaWorkspace2@onelake.dfs.fabric.microsoft.com/MhaWarehouse2.warehouse/Tables/ods/customer")
product_df = spark.read.format("delta").load("abfss://MhaWorkspace2@onelake.dfs.fabric.microsoft.com/MhaWarehouse2.warehouse/Tables/ods/product")
productCategory_df = spark.read.format("delta").load("abfss://MhaWorkspace2@onelake.dfs.fabric.microsoft.com/MhaWarehouse2.warehouse/Tables/ods/product_category")
productSubcategory_df = spark.read.format("delta").load("abfss://MhaWorkspace2@onelake.dfs.fabric.microsoft.com/MhaWarehouse2.warehouse/Tables/ods/product_subcategory")
territory_df = spark.read.format("delta").load("abfss://MhaWorkspace2@onelake.dfs.fabric.microsoft.com/MhaWarehouse2.warehouse/Tables/ods/territory")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************


sales_df=sales_df.withColumn("order_dt",make_date(split(sales_df['OrderDate'],'/')[2], split(sales_df['OrderDate'],'/')[0], split(sales_df['OrderDate'],'/')[1]))
#(sales_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sample_date='20150401'

all_sample_df=sales_df.withColumn("sample_date",concat(year(sales_df['order_dt']),lpad(month(sales_df['order_dt']),2,'0'),lit('01'))) 
all_sample_df = all_sample_df.select(all_sample_df['sample_date']).distinct().orderBy(all_sample_df['sample_date'])

customer_df = customer_df.alias("customer")
product_df = product_df.alias("product")
productCategory_df = productCategory_df.alias("productCategory")
productSubcategory_df = productSubcategory_df.alias("productSubcategory")
territory_df = territory_df.alias("territory")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def sliceDataToSample(sample_date):
    sample_folder_path=f"{lakehouse_path}Files/Adventures_sample/AdventureWorks_{sample_date}"

    notebookutils.fs.mkdirs(sample_folder_path)

    df=sales_df.where(year(sales_df.order_dt)==year(date_add(to_date(lit(sample_date),'yyyyMMdd'),-1))).where((month(sales_df.order_dt)==month(date_add(to_date(lit(sample_date),'yyyyMMdd'),-1))))
    df=df.drop("order_dt")
    # Perform the join
    df = df.alias("source")
    df.coalesce(1).write.option("header",True).mode("overwrite").csv(f"{sample_folder_path}/AdventureWorks_Sales_{sample_date}.csv")

    territory_sampled = df.join(territory_df, df.TerritoryKey == territory_df.SalesTerritoryKey , "inner").select("territory.*").distinct()
    territory_sampled.coalesce(1).write.option("header",True).mode("overwrite").csv(f"{sample_folder_path}/AdventureWorks_Territories_{sample_date}.csv")

    customer_sampled = df.join(customer_df, df.CustomerKey == customer_df.CustomerKey, "inner").select("customer.*").distinct()
    customer_sampled.coalesce(1).write.option("header",True).mode("overwrite").csv(f"{sample_folder_path}/AdventureWorks_Customers_{sample_date}.csv")

    product_sampled = df.join(product_df, df.ProductKey == product_df.ProductKey, "inner").select("product.*").distinct()
    product_sampled.coalesce(1).write.option("header",True).mode("overwrite").csv(f"{sample_folder_path}/AdventureWorks_Products_{sample_date}.csv")

    productSubcategory_sampled = product_sampled.join(productSubcategory_df, product_sampled.ProductSubcategoryKey == productSubcategory_df.ProductSubcategoryKey, "inner") \
                    .select("productSubcategory.*").distinct()
    productSubcategory_sampled.coalesce(1).write.option("header",True).mode("overwrite").csv(f"{sample_folder_path}/AdventureWorks_Product_Subcategories_{sample_date}.csv")

    productCategory_sampled = productSubcategory_sampled.join(productCategory_df, productSubcategory_sampled.ProductCategoryKey == productCategory_df.ProductCategoryKey, "inner") \
                    .select("productCategory.*").distinct()

    productCategory_sampled.coalesce(1).write.option("header",True).mode("overwrite").csv(f"{sample_folder_path}/AdventureWorks_Product_Categories_{sample_date}.csv")

    list_files_to_move=[f"AdventureWorks_Sales_{sample_date}.csv",f"AdventureWorks_Territories_{sample_date}.csv",\
                        f"AdventureWorks_Customers_{sample_date}.csv",f"AdventureWorks_Products_{sample_date}.csv",\
                        f"AdventureWorks_Product_Subcategories_{sample_date}.csv",f"AdventureWorks_Product_Categories_{sample_date}.csv"]

    for filedir in list_files_to_move:
        files = notebookutils.fs.ls(f"{sample_folder_path}/{filedir}/")    
        csv_file = [file.name for file in files if file.name.startswith("part")]    
        notebookutils.fs.cp(f"{sample_folder_path}/{filedir}/{csv_file[0]}",f"{sample_folder_path}/{csv_file[0]}")
        notebookutils.fs.rm(f"{sample_folder_path}/{filedir}", True)
        notebookutils.fs.mv(f"{sample_folder_path}/{csv_file[0]}",f"{sample_folder_path}/{filedir}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#display(all_sample_df)
rows = all_sample_df.select('sample_date').collect()
for sample in rows:
    print(sample[0])
    sliceDataToSample(sample[0])
    


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

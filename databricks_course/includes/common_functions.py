# Databricks notebook source
from pyspark.sql.functions import current_timestamp
def add_ingestion_date(input_df):
    output_df=input_df.withColumn("ingestion_date",current_timestamp())
    return output_df

# COMMAND ----------

def incremental(df_name,db_name,table_name,partition_column):
    spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
    df_name=change_order_partition_column(df_name,partition_column)
    if spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}"):
        df_name.write.mode('overwrite').insertInto(f"{db_name}.{table_name}")
    else:
        df_name.write.mode('overwrite').partitionBy(partition_column).format("parquet").saveAsTable(f"{db_name}.{table_name}")

# COMMAND ----------

# def change_order_partition_column(df,column):
#     df=df.withColumn("partition_column",df[column])
#     df=df.drop(column)
#     df=df.withColumnRenamed('partition_column',column)
#     return df
def change_order_partition_column(df,column):
    lt=[]
    for each in df.schema.names:
        if each !=column:
            lt.append(each)
    lt.append(column)
    df=df.select(lt)
    return df

# COMMAND ----------

def merge_delta_data(input_df,db_name,table_name,folder_path,merge_condition,partition_column):
    spark.conf.set("spark.databricks.optimizer.dynamicPartitionPruning","true")
    
    from delta.tables import DeltaTable
    if spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}"):
        deltaTable=DeltaTable.forPath(spark,f"/{folder_path}/{table_name}")
        deltaTable.alias("tgt").merge(
        input_df.alias('src'),
        merge_condition)\
        .whenMatchedUpdateAll()\
        .whenNotMatchedInsertAll()\
        .execute()
    else:
        input_df.write.mode('overwrite').partitionBy(partition_column).format("delta").saveAsTable(f"{db_name}.{table_name}")
    

# COMMAND ----------


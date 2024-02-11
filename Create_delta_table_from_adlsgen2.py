# Databricks notebook source
from pyspark.sql.functions import regexp_replace
from pyspark.sql.types import FloatType

# COMMAND ----------

def adls_read_file(adls, container, SASKey, csvpath, separator):
    '''
    Read a flat file stored in an Azure Data Lake Storage. You need the following arguments to make it work:
    - adls : Azure Data Lake Storage name
    - container : Container name of your Data Lake Storage
    - SASKey : Azure Data Lake Storage Shared Acess Signature key
    - csvpath : Path of your file
    - separator : Separator of your flat file
    '''
    spark.conf.set("fs.azure.account.auth.type.{adls}.dfs.core.windows.net".format(adls=adls), "SAS")
    spark.conf.set("fs.azure.sas.token.provider.type.{adls}.dfs.core.windows.net".format(adls=adls), "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
    spark.conf.set("fs.azure.sas.fixed.token.{adls}.dfs.core.windows.net".format(adls=adls), str(SASKey))
    sp_dvf_file=spark.read.csv('abfs://{container}@{adls}.dfs.core.windows.net/{csvpath}'.format(adls=adls, container=container, csvpath=csvpath),sep=separator, header=True)
    return sp_dvf_file
    

# COMMAND ----------

x = adls_read_file('adlsintrosteven','adlsintro', '?sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2024-02-14T01:51:44Z&st=2023-12-28T11:01:44Z&spr=https&sig=xYt%2FGViep0o2%2F4CWdU0xP8eneenl%2BLXjboPSSZ0RPao%3D','list_NASDAQ_ticker.csv',',')

# COMMAND ----------

display(x)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS bronze.dvf USING DELTA LOCATION '/FileStore/tables/dvf_2021'

#%%
from pyspark.sql import SparkSession
# %%
spark = SparkSession.builder.appName("PySparkTest").getOrCreate()
# %%

df = spark.read.csv("/c:/Users/shiga/repos/csv_data_platform/data/sample_events.csv", header=True, inferSchema=True)
# %%
df.show(5,False)
# %%

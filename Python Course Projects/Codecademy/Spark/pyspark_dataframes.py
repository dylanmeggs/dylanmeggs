from pyspark.sql import SparkSession

# Create a new SparkSession
spark = SparkSession\
    .builder\
    .config('spark.app.name', 'learning_spark_sql')\
    .getOrCreate()

# Read in Wikipedia Unique Visitors Dataset
wiki_uniq_df = spark.read\
    .option('header', True) \
    .option('delimiter', ',') \
    .option('inferSchema', True) \
    .csv("wiki_uniq_march_2022.csv")



# select only domain and uniq_human visitors
uniq_human_visitors_df = wiki_uniq_df\
    .select('domain', 'uniq_human_visitors')

# show the new DataFrame
uniq_human_visitors_df.show()

uniq_human_visitors_df.write.csv('./results/csv/uniq_human_visitors/',mode="overwrite")
uniq_human_visitors_df.write.parquet('./results/pq/uniq_human_visitors/',mode="overwrite")
# Analyzing Wikipedia Clickstream Data

# [Project Page Link](https://www.codecademy.com/courses/big-data-pyspark/projects/analyzing-wikipedia-pyspark)



### Import Libraries
from pyspark.sql import SparkSession

## Task Group 1 - Introduction to Clickstream Data

### Task 1
# Create a new `SparkSession` and assign it to a variable named `spark`.
# Create a new SparkSession
spark = SparkSession\
    .builder\
    .config('spark.app.name', 'wikipedia_clickstream_project')\
    .getOrCreate()


### Task 2

# Create an RDD from a list of sample clickstream counts and save it as `clickstream_counts_rdd`.
# Sample clickstream counts
sample_clickstream_counts = [
    ["other-search", "Hanging_Gardens_of_Babylon", "external", 47000],
    ["other-empty", "Hanging_Gardens_of_Babylon", "external", 34600],
    ["Wonders_of_the_World", "Hanging_Gardens_of_Babylon", "link", 14000],
    ["Babylon", "Hanging_Gardens_of_Babylon", "link", 2500]
]

# Create RDD from sample data
clickstream_counts_rdd = spark.sparkContext.parallelize(sample_clickstream_counts)

### Task 3

# Using the RDD from the previous step, create a DataFrame named `clickstream_sample_df`
# Create a DataFrame from the RDD of sample clickstream counts
clickstream_sample_df = clickstream_counts_rdd.toDF(['source_page','target_page','link_category','link_count'])

# Display the DataFrame to the notebook
clickstream_sample_df.show()


## Task Group 2 - Inspecting Clickstream Data

### Task 4

# Read the files in `./cleaned/clickstream/` into a new Spark DataFrame named `clickstream` and display the first few rows of the DataFrame in the notebook
# Read the target directory (`./cleaned/clickstream/`) into a DataFrame (`clickstream`)
clickstream = spark.read\
    .option('header', True) \
    .option('delimiter', '\t') \
    .option('inferSchema', True) \
    .csv("./cleaned/clickstream/")

# Display the DataFrame to the notebook
clickstream.show()
    


### Task 5

# Print the schema of the DataFrame in the notebook.
# Display the schema of the `clickstream` DataFrame to the notebook
clickstream.printSchema()
    


### Task 6

#Drop the `language_code` column from the DataFrame and display the new schema in the notebook.
# Drop target columns
clickstream = clickstream.drop('language_code')

# Display the first few rows of the DataFrame
clickstream.show(3,truncate = False)
# Display the new schema in the notebook
clickstream.printSchema()


### Task 7

# Rename `referrer` and `resource` to `source_page` and `target_page`, respectively,
# Rename `referrer` and `resource` to `source_page` and `target_page`
clickstream = clickstream.withColumnRenamed('referrer','source_page').withColumnRenamed('resource','target_page')
  
# Display the first few rows of the DataFrame
clickstream.show(3,truncate = False)
# Display the new schema in the notebook
clickstream.printSchema()
    


## Task Group 3 - Querying Clickstream Data

### Task 8
# Add the `clickstream` DataFrame as a temporary view named `clickstream` to make the data queryable with `sparkSession.sql()`
# Create a temporary view in the metadata for this `SparkSession` 
clickstream.createOrReplaceTempView("clickstream")


### Task 9
# Filter the dataset to entries with `Hanging_Gardens_of_Babylon` as the `target_page` and order the result by `click_count` using PySpark DataFrame methods.
# Filter and sort the DataFrame using PySpark DataFrame methods
clickstream \
.select('*') \
.filter(clickstream.target_page == 'Hanging_Gardens_of_Babylon') \
.orderBy('click_count') \
.show(truncate=False)
    


### Task 10
# Perform the same analysis as the previous exercise using a SQL query. 
# Filter and sort the DataFrame using SQL
query = """
SELECT
*
FROM clickstream
WHERE target_page = 'Hanging_Gardens_of_Babylon'
ORDER BY click_count
"""
spark.sql(query).show(truncate=False)
    


### Task 11
# Calculate the sum of `click_count` grouped by `link_category` using PySpark DataFrame methods.

# Aggregate the DataFrame using PySpark DataFrame Methods 
clickstream \
.select('link_category', 'click_count') \
.groupBy('link_category') \ #Note that groupBy must come before the sum
.sum() \
.orderBy('sum(click_count)') \
.show(truncate=False)



### Task 12
#Perform the same analysis as the previous exercise using a SQL query.
# Aggregate the DataFrame using SQL
query = """
SELECT
    link_category,
    sum(click_count)
FROM clickstream
GROUP BY link_category
ORDER BY sum(click_count)"""

spark.sql(query).show(truncate=False)

    


## Task Group 4 - Saving Results to Disk

### Task 13
# Let's create a new DataFrame named `internal_clickstream` that only contains article pairs where `link_category` is `link`. 
# Use `filter()` to select rows to a specific condition and `select()` to choose which columns to return from the query.
# Create a new DataFrame named `internal_clickstream`
internal_clickstream = clickstream.filter(clickstream.link_category == 'link')\
    .select('source_page','target_page','click_count')

# Display the first few rows of the DataFrame in the notebook
internal_clickstream.show(3,truncate=False)


### Task 14
# Save the `internal_clickstream` DataFrame to a series of CSV files
internal_clickstream.write.csv('./results/article_to_article_csv/')

### Task 15
# Save the `internal_clickstream` DataFrame to a series of parquet files
internal_clickstream.write.parquet('./results/article_to_article_pq/')

### Task 16
# Stop the notebook's `SparkSession` and `SparkContext`
spark.stop()
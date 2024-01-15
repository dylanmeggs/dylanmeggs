'''After uploading a .json file to DBFS as a table, instead of accessing that table using a SQL command as we did in this lecture, you can create a Spark DataFrame the same way as you tried to do it in the 3rd cell on your screenshot - only instead of specifying the path to where the file is stored on your machine, you should specify the path to its location on DBFS as follows:

df = spark.read.json('dbfs:/FileStore/tables/people.json')'''
# Resilient Distributed Dataset (RDD)
# Step 1: Create RDD
# Step 2: Transformation (RDD Input -> RDD Output)
# Step 3: Action (RDD Input -> Non-RDD Value Output)
# Step 4: Accumulator Variables


# Initiate a Spark session
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
# Create list of tuples for RDD
# Name, SAT Score, Grade, State
student_data = [("Chris",1523,0.72,"CA"),
                ("Jake", 1555,0.83,"NY"),
                ("Cody", 1439,0.92,"CA"),
                ("Lisa",1442,0.81,"FL"),
                ("Daniel",1600,0.88,"TX"),
                ("Kelvin",1382,0.99,"FL"),
                ("Nancy",1442,0.74,"TX"),
                ("Pavel",1599,0.82,"NY"),
                ("Josh",1482,0.78,"CA"),
                ("Cynthia",1582,0.94,"CA")]
# Create an RDD
student_rdd = spark.sparkContext.parallelize(student_data)
# Transform the student's grades to whole numbers
rdd_transformation = student_rdd.map(lambda x: (x[0], x[1], int(x[2]*100), x[3]))

# Create dictionary to map state abbreviations with their names
states = {"NY":"New York", "CA":"California", "TX":"Texas", "FL":"Florida"}

# Broadcast the states dictionary to Spark Cluster. Save this object as broadcastStates.
broadcastStates = spark.sparkContext.broadcast(states)

# confirm type
type(broadcastStates) # Expected Result = pyspark.broadcast.Broadcast

# Reference broadcastStates to map the two-letter abbreviations to their full names. Save transformed rdd as rdd_broadcast.
rdd_broadcast = rdd_transformation.map(lambda x: (x[0], x[1], x[2], broadcastStates.value[x[3]]))

# confirm transformation is correct
rdd_broadcast.collect() # Expected result is that the 2-letter state abbreviations are replaced with state names in the RDD

# Create the accumulator variable that starts at 0 and name it sat_1500.
sat_1500 = spark.sparkContext.accumulator(0)

# confirm type
type(sat_1500) # pyspark.accumulators.Accumulator

# Create a function called count_high_sat_score that increments our accumulator by 1 whenever it encounters a score of over 1500.
def count_high_sat_score(x):
    if x[1] > 1500: sat_1500.add(1)

rdd_broadcast.foreach(lambda x: count_high_sat_score(x))

# confirm accumulator worked
print(sat_1500) # 5
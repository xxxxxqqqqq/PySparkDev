# Import SparkContext from pyspark
from pyspark import SparkContext as s_context

# Import SparkConf from pyspark
from pyspark import SparkConf as s_conf

# Import SparkSession from pyspark.sql
from pyspark.sql import SparkSession as s_session

# Import Pandas and Numpy
import pandas as pd
import numpy as np

conf = s_conf().setAppName('appName').setMaster('local')
sc = s_context(conf=conf)
print(sc)
print(sc.version) 


# Create my_spark
my_spark = s_session.builder.getOrCreate()

# Print my_spark
print(my_spark)

# List all the tables in my cluster (We dont have anything here -> It is empty)
print(my_spark.catalog.listTables())

# an SQL query to extract the first 10 rows from the table called "flights"
query1 = "FROM flights SELECT * LIMIT 10"

# Get the first 10 rows of flights
flights10 = my_spark.sql(query1)

# Show the results
flights10.show()

# an SQL query to count the number of flights to each aiport from SEA and PDX
query2 = "SELECT origin, dest, COUNT(*) as N FROM flights GROUP BY origin, dest"

# Run the query
flight_counts = my_spark.sql(query2)

# Convert the results to a pandas DataFrame
pd_counts = flight_counts.toPandas()

# Print the head of pd_counts
print(pd_counts)


# Creating random pandas dataframe
pd_temp = pd.DataFrame(np.random.random(10))

# creating pyspark dataframe from pandas dataframe
spark_temp = my_spark.createDataFrame(pd_temp)

# At this point it is important to realize that these dataframes cant be found in the cluster yet.
# We just converted them locally. If we want to have them on the cluster, then we need to create a catalog from these.

# Examine the tables in the catalog -> We saw that spark_temp is not available in the cluster.
print(my_spark.catalog.listTables())

# Add spark_temp to the catalog
spark_temp.createOrReplaceTempView("temp")

# Examine the tables in the catalog again -> Now we should be able to see that this data is available in the cluster as catalog
print(my_spark.catalog.listTables())

# We can directly read from spark and read spark dataframes. For example, here we have .csv file available on the cluster
file_path = "/usr/local/share/datasets/airports.csv"

# Read the data into spark dataframes. Second argument is there so that Spark knows to take the column names from the first line of the file
airports = my_spark.read.csv(file_path, header=True)

# Show the data
airports.show()
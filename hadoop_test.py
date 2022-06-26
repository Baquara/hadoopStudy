import hadoop.fs
import hadoop.util

# HDFS is used to store data on the Hadoop Distributed File System
# We first need to connect to the HDFS
fs = hadoop.fs.FileSystem.get(hadoop.conf.Configuration())

# Then we can perform various file system operations
# For example, we can create a new directory
fs.mkdirs("/user/myusername/newdir")

# We can also read data from HDFS
data = fs.open("/user/myusername/input.txt")

# And write data to HDFS
output = fs.create("/user/myusername/output.txt")

# Finally, we can close the HDFS connection
fs.close()

# MapReduce is a programming model for processing large data sets
# with a parallel, distributed algorithm on a cluster
# We can use MapReduce to count the words in a large text file

# First, we need to define a map function
# The map function takes a line of text as input and outputs a list of words
def map(line):
  words = line.split()
  return [(word, 1) for word in words]

# Next, we need to define a reduce function
# The reduce function takes a word and a list of counts as input and outputs a single count
def reduce(word, counts):
  return sum(counts)

# We can now run our MapReduce program
# We need to specify the input and output paths
inputPath = "/user/myusername/input.txt"
outputPath = "/user/myusername/output.txt"

# We also need to specify the map and reduce functions
# Finally, we can specify the number of reducers
# By default, Hadoop will use one reducer
# But we can specify more reducers to parallelize the computation
hadoop.util.run_and_wait(
  hadoop.streaming.StreamingJob(
    mapper=map,
    reducer=reduce,
    input=inputPath,
    output=outputPath,
    num_reducers=4
  )
)

# We can now read the output of our MapReduce program
output = fs.open(outputPath)
for line in output:
  print line

# Finally, we close the HDFS connection
fs.close()

# We've now seen how to use HDFS and MapReduce with Python
# This is just a small taste of what you can do with Hadoop
# There are many more powerful features that we didn't cover here

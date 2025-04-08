import os
from pyspark.sql import SparkSession

# Spark and Hadoop configuration
spark_home = "/Users/marcoo_sg/Spark"
hadoop_home = "/Users/marcoo_sg/Spark/hadoop-3.4.1"

os.environ['SPARK_HOME'] = spark_home
os.environ['SPARK_MASTER_HOST'] = "localhost"
os.environ['HADOOP_HOME'] = hadoop_home
os.environ['HADOOP_CONF_DIR'] = os.path.join(hadoop_home, 'etc', 'hadoop')
os.environ['LD_LIBRARY_PATH'] = os.path.join(hadoop_home, 'lib', 'native') + ':' + os.environ.get('LD_LIBRARY_PATH', '')
os.environ['SPARK_DIST_CLASSPATH'] = os.popen(f"{os.path.join(hadoop_home, 'bin', 'hadoop')} classpath").read()
os.environ['PYSPARK_PYTHON'] = '/Users/marcoo_sg/.pyenv/versions/3.12.0/bin/python' 
os.environ['PYSPARK_DRIVER_PYTHON'] = '/Users/marcoo_sg/.pyenv/versions/3.12.0/bin/python'

# Creating Spark session with MySQL connector
try:
    spark = SparkSession \
        .builder \
        .appName('test') \
        .master('local[*]') \
        .config('spark.jars', '/Users/marcoo_sg/Spark/jars/mysql-connector-j-9.2.0.jar') \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
except Exception as e:
    print(f"Error initializing Spark: {e}")

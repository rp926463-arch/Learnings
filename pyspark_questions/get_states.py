Question:
	You have been given CSV files like karnataka.csv, maharashtra.csv... in an ADLS location, each containing columns like first_name, last_name, age, sex, location. Your task is to add new column called 'state' to dataframe, state column should contain state name extracted from filename.


import os
import sys
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf, input_file_name

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable


conf = SparkConf().setAppName('testApp').setMaster("local[*]")
spark = SparkSession.builder.config(conf=conf).getOrCreate()

print(spark.version)


@udf(StringType())
def getFileName(filepath):
    return os.path.basename(filepath).split('.')[0]


def read_input_file(filepath):
    df = spark.read.option('header', True).csv(filepath)
    df = df.withColumn('state', getFileName(input_file_name()))
    return df


directory = './dataset/'
list_files = []
combined_df = None

for file in os.listdir(directory):
    if os.path.isfile(os.path.join(directory, file)):
        list_files.append(os.path.join(directory, file))

for file in list_files:
    tmp_df = read_input_file(file)
    if combined_df is None:
        combined_df = tmp_df
    else:
        combined_df = combined_df.union(tmp_df)

combined_df.show()

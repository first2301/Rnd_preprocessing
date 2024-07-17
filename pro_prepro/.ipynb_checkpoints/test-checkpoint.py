from pyspark.sql import SparkSession

# spark = SparkSession.builder.getOrCreate()
spark = SparkSession.builder \
    .appName("ImageDataProcessing") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .getOrCreate()

from datetime import datetime, date
import pandas as pd
from pyspark.sql import Row

from pyspark.sql.functions import col, split, udf
from pyspark.sql.types import StringType, LongType, StructType, StructField
import os
import glob


# paths = glob.glob('./work/*')
paths = glob.glob('../data/sand/*')

img_dir_list = [path for path in paths if not path.endswith('.zip')]
img_total_list = [glob.glob(folder+'/*') for folder in img_dir_list]
img_paths = [img_path for sublist in img_total_list for img_path in sublist]

print(img_paths)
# libraries: mathematical computing
import numpy as np
import pandas as pd

# libraries: pyspark SparkContext
from pyspark import SparkContext, SparkConf

# libraries: pyspark sql
from pyspark.sql.types import IntegerType, FloatType, DateType
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from  pyspark.sql.functions import monotonically_increasing_id, desc, row_number
from pyspark.sql import SQLContext

# libraries: visualization
import seaborn as sb
import functools
from collections import Counter

# spark builder
spark = SparkSession.builder.appName("movieAnalysis").getOrCreate()

# read the .parquet file
df_title_akas = spark.read.parquet('title.akas.parquet')

### table: df_title_akas
# drop null values
# columns: "titleId","ordering","title","region"

df_title_akas = df_title_akas.na.drop(subset=["titleId","ordering","title","region"])

# drop duplicated values
# columns: all of them

df_title_akas = df_title_akas.distinct()
df_title_akas.show()
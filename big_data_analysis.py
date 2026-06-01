# big_data_analysis.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when
import matplotlib.pyplot as plt

# Create Spark Session
spark = SparkSession.builder \
    .appName("Netflix Big Data Analysis") \
    .getOrCreate()

print("Spark Session Created Successfully")

# Load Dataset
# Make sure netflix_titles.csv is in the same folder as this script
df = spark.read.csv(
    "netflix_titles.csv",
    header=True,
    inferSchema=True
)

print("\nDataset Loaded Successfully")

# Display First 10 Records
print("\nFirst 10 Records:")
df.show(10)

# Dataset Information
print("\nDataset Information")
print("Total Rows:", df.count())
print("Total Columns:", len(df.columns))

print("\nColumn Names:")
print(df.columns)

# Dataset Schema
print("\nSchema:")
df.printSchema()

# Summary Statistics
print("\nSummary Statistics:")
df.describe().show()

# Missing Values Analysis
print("\nMissing Values:")
missing_values = df.select([
    count(when(col(c).isNull(), c)).alias(c)
    for c in df.columns
])

missing_values.show()

# Movies vs TV Shows Analysis
print("\nMovies vs TV Shows:")
df.groupBy("type").count().show()

# Content Rating Analysis
print("\nContent Ratings:")
df.groupBy("rating") \
    .count() \
    .orderBy("count", ascending=False) \
    .show()

# Release Year Analysis
print("\nRelease Year Analysis:")
df.groupBy("release_year") \
    .count() \
    .orderBy("release_year") \
    .show()

# Top 10 Countries
print("\nTop 10 Countries:")
df.groupBy("country") \
    .count() \
    .orderBy("count", ascending=False) \
    .show(10)

# Visualization
print("\nGenerating Visualization...")

type_counts = df.groupBy("type").count().toPandas()

plt.figure(figsize=(6, 4))
plt.bar(type_counts["type"], type_counts["count"])
plt.title("Movies vs TV Shows on Netflix")
plt.xlabel("Type")
plt.ylabel("Count")
plt.savefig("movies_vs_tvshows.png")
plt.show()

print("\nAnalysis Completed Successfully!")

# Stop Spark Session
spark.stop()

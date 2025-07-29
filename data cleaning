from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Step 1: Initialize Spark session
spark = SparkSession.builder.appName("DataCleaningExample").getOrCreate()

# Step 2: Load Data
df = spark.read.csv("/home/cusat/Desktop/R/Data Analytics/dataCleaning.csv", header=True, inferSchema=True)

# Show the raw data
print("Raw Data:")
df.show()

# Step 3: Handle Missing Values
df_dropna = df.dropna()

# Replace missing values in the 'age' and 'score' columns with defaults
df_fillna = df.fillna({"age": 0, "score": 0})

# Show the data after handling missing values
print("Data after dropping rows with nulls:")
df_dropna.show()

print("Data after replacing missing values with defaults:")
df_fillna.show()

# Step 4: Remove Duplicates
df_no_duplicates = df.dropDuplicates()

# Remove duplicates based on specific columns
df_no_duplicates_name = df.dropDuplicates(["name"])

# Show the data after removing duplicates
print("Data after removing duplicates:")
df_no_duplicates.show()

print("Data after removing duplicates based on 'name' column:")
df_no_duplicates_name.show()

# Step 5: Transform Columns
# Rename 'score' to 'total_score'
df_renamed = df.withColumnRenamed("score", "total_score")

# Change the 'age' column to integer type
df_casted = df.withColumn("age", col("age").cast("int"))

# Add a new column that is the double of 'total_score'
df_new_column = df_renamed.withColumn("double_score", col("total_score") * 2)

# Show the data after column transformations
print("Data after renaming 'score' to 'total_score' and adding new 'double_score' column:")
df_new_column.show()

# Step 6: Filter Data
df_filtered = df.filter(df["age"] >= 25)

# Filter data where 'score' is greater than 90
df_filtered_score = df_renamed.filter(df_renamed["total_score"] > 90)

# Show the data after filtering
print("Data after filtering where 'age' >= 25:")
df_filtered.show()

print("Data after filtering where 'total_score' > 90:")
df_filtered_score.show()

# Step 7: Handle Outliers
# Remove rows where 'total_score' is above 120 (outlier condition)
df_no_outliers = df_renamed.filter(df_renamed["total_score"] <= 120)

# Show the data after removing outliers
print("Data after removing outliers (score > 120):")
df_no_outliers.show()

# Step 8: Save Cleaned Data
# Save the cleaned data to a new CSV file
df_cleaned = df_no_outliers.write.mode("overwrite").csv("/home/cusat/Desktop/R/Data Analytics/cleanedData.csv", header=True)

# Stop the Spark session
spark.stop()

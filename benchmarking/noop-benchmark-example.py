from pyspark.sql import SparkSession
import time

# Initialize Spark session
spark = SparkSession.builder.appName("NoopBenchmarkExample").getOrCreate()

# Your data processing function
def process_data(df):
    # Example transformations
    df = df.filter(df.age > 18)
    df = df.groupBy("country").agg({"salary": "avg"})
    df = df.join(another_df, "country")
    return df

# Benchmarking function
def benchmark_noop(df):
    start_time = time.time()
    
    # Process the data
    result_df = process_data(df)
    
    # Perform noop write
    result_df.write.format("noop").mode("overwrite").save()
    
    end_time = time.time()
    execution_time = end_time - start_time
    
    return execution_time

# Load your data
df = spark.read.parquet("path/to/your/data")

# Run the benchmark
execution_time = benchmark_noop(df)
print(f"Execution time: {execution_time} seconds")

# Don't forget to stop the Spark session when done
spark.stop()
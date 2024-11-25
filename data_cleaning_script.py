from pyspark.sql import SparkSession
from pyspark.sql.functions import unix_timestamp

# Initialize Spark session
spark = SparkSession.builder.appName("DataCleaning").getOrCreate()

# Step 1: Load the dataset
# Replace with the correct file path to your dataset
file_path = "/user/maria_dev/spark/dataset.csv"
df = spark.read.csv(file_path, header=True, inferSchema=True)

# Step 2: Data Cleaning
def clean_data(df):
    # Drop missing values
    df = df.dropna()

    # Remove duplicates
    df = df.dropDuplicates()

    return df

# Apply data cleaning
cleaned_df = clean_data(df)

# Step 3: Write the cleaned data to a new file
output_path = "/user/maria_dev/spark/cleaned_sales_dataset.csv"
cleaned_df.write.csv(output_path, header=True, mode="overwrite")

# Stop Spark session
spark.stop()

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, when
from pyspark.sql.types import StringType, StructType, StructField
import pandas as pd
import ast

# Initialize a Spark session
spark = SparkSession.builder.appName("Classification").getOrCreate()

# Read the CSV file using pandas
keywords_df = pd.read_csv("/path/to/classification_keywords.csv")

# Convert the pandas DataFrame to a dictionary for keyword lookup
keywords_dict = {}
for index, row in keywords_df.iterrows():
    keywords_str = str(row['KEYWORDS']).strip()
    try:
        if keywords_str.startswith("[") and keywords_str.endswith("]"):
            keywords_list = ast.literal_eval(keywords_str)
        else:
            keywords_list = [keyword.strip() for keyword in keywords_str.split(",")]
        for keyword in keywords_list:
            keywords_dict[keyword.lower()] = (row['CATEGORY_LEVEL1'], row['CATEGORY_LEVEL2'])
    except Exception as e:
        print("Skipping row at index {}: {}\n".format(index, e))

# Function to set category levels
def set_category_levels(base_txn_text, benef_name):
    base_txn_text = base_txn_text.lower() if base_txn_text else ''
    benef_name = benef_name.lower() if benef_name else ''
    
    for keyword, (cat_level1, cat_level2) in keywords_dict.items():
        if keyword in base_txn_text or keyword in benef_name:
            return (cat_level1, cat_level2)
    
    return ('OTHER TRANSFER', 'OTHER')

# Define the schema for the UDF return type
schema = StructType([
    StructField("category_level1", StringType(), False),
    StructField("category_level2", StringType(), False)
])

# Register the UDF
category_udf = udf(set_category_levels, schema)

# Define the UDF for classification
def classify_transaction(benef_ifsc, benef_account_no, source, benef_name):
    # ... same function as provided earlier ...

classify_transaction_udf = udf(classify_transaction, StringType())

# Sample data
data = [
    # ... same sample data as provided earlier ...
]
columns = ["remitter_name", "base_txn_text", "benef_name", "benef_ifsc", "benef_account_no", "source"]

# Create the DataFrame
df = spark.createDataFrame(data, columns)

# Perform the classification
df = df.withColumn('cor_ind_benf', classify_transaction_udf(col('benef_ifsc'), col('benef_account_no'), col('source'), col('benef_name')))
df = df.withColumn('category_level1', category_udf(col('base_txn_text'), col('benef_name')).alias('categories')['category_level1'])

# Override category_level2 if remitter_name equals benef_name
df = df.withColumn('category_level2', when(col('remitter_name').lower() == col('benef_name').lower(), 'Personal Transfer')
                                    .otherwise(category_udf(col('base_txn_text'), col('benef_name')).alias('categories')['category_level2']))

# Show the DataFrame
df.show(truncate=False)

# Stop the Spark session
spark.stop()


import pandas as pd
import ast
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Step 1: Read the CSV file using pandas
keywords_df = pd.read_csv("classification_keywords.csv")

# Step 2: Convert the pandas DataFrame to a dictionary for keyword lookup
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
        #print(f"Error in row {index}: {row}")
        #print(f"Error: {e}")
        print("Skipping this row...\n")

# Step 3: Initialize a Spark session
#spark = SparkSession.builder.appName("Classification").getOrCreate()

# Function to set category levels
def set_category_levels(base_txn_text, benef_name):
    base_txn_text = '' if base_txn_text is None else base_txn_text.lower()
    benef_name = '' if benef_name is None else benef_name.lower()
    
    # Check for keyword matches in base_txn_text and benef_name
    for keyword, (cat_level1, cat_level2) in keywords_dict.items():
        if keyword in base_txn_text or keyword in benef_name:
            return (cat_level1, cat_level2)
    
    return ('OTHER TRANSFER', 'OTHER')

# Correcting the returnType to be StructType
schema = StructType([
    StructField("category_level1", StringType(), False),
    StructField("category_level2", StringType(), False)
])

# Register UDF for category levels
category_udf = udf(set_category_levels, schema)

# Define the UDF for classification
def classify_transaction(benef_ifsc, benef_account_no, source, benef_name):
    if benef_ifsc and benef_ifsc.startswith("YESB") and benef_account_no and len(str(benef_account_no)) == 15:
        return 'ybl_cor' if source == 'current' else 'ybl_ind' if source == 'saving' else 'ybl_ind'
    elif benef_ifsc and not benef_ifsc.startswith("YESB"):
        keywords = ["pvt ltd", "innovation", "tata", "steel", "industry", "llp", "corporation", "institutaional", "tech", "automobiles", "services", "telecomunication", "travels"]
        if any(keyword in benef_name.lower() for keyword in keywords):
            return 'non_ybl_cor' if source == 'current' else 'non_ybl_ind' if source == 'saving' else 'non_ybl_ind'
    return 'non_ybl_ind'

classify_transaction_udf = udf(classify_transaction, StringType())


data = [
    ("Kiran", "Credit Card Payment", "HDFC Bank", "YESB0000001", "123456789012345", "current"),
    ("Vishal", "Shopping at Amazon", "Amazon", "HDFC0000001", "1234567890123", "saving"),
    ("Nagesh", "self expense", "nagesh deshmukh", "HDFC0000001", "1234567890123", "saving"),
    ("Laxita", "rtgs transfer", "tata ltd", "HDFC0000001", "1234567890123", "current"),
    ("Ragesh", "taxi rent uber", "manish", "YESB0000001", "123456789012345", "saving"),
    ("Ragesh", "health insuranc", "manish", "YESB0000001", "123456789012345", "saving")
    # ... other rows ...
]

columns = ["remitter_name", "base_txn_text", "benef_name", "benef_ifsc", "benef_account_no", "source"]

df = spark.createDataFrame(data, columns)
df = df.withColumn('cor_ind_benf', classify_transaction_udf(df['benef_ifsc'],df['source'], df['benef_account_no'], df['benef_name']))
df = df.withColumn('categories', category_udf(col('base_txn_text'), col('benef_name')))
df = df.withColumn('category_level1', col('categories').getItem('category_level1'))
df = df.withColumn('category_level2', col('categories').getItem('category_level2'))
df = df.drop('categories')

# Show the DataFrame
df.show()

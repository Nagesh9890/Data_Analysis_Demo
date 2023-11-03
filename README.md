import pandas as pd
import ast
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, when, lower
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.types import StringType, BooleanType

# Step 1: Read the CSV file using pandas
# Ensure that the 'classification_keywords.csv' file is present in the current directory
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
        print("Skipping this row...\n")

# Function to set category levels
def set_category_levels(base_txn_text, benef_name):
    base_txn_text = '' if base_txn_text is None else base_txn_text.lower()
    benef_name = '' if benef_name is None else benef_name.lower()
    
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
    corporate_keywords = [
        "pvt ltd","ltd","LTD","innovation", "tata", "steel", "industry", "llp",
        "corporation", "institutional", "tech", "automobiles", "services",
        "telecommunication", "travels"
    ]
    
    def contains_corporate_keyword(name):
        return any(keyword in name.lower() for keyword in corporate_keywords)
    
    is_corporate = contains_corporate_keyword(benef_name)
    
    if benef_ifsc and benef_ifsc.startswith("YESB"):
        if source == 'current':
            return 'YBL_Corp'
        elif source == 'saving':
            return 'YBL_Ind'
        elif not source:
            return 'YBL_Corp' if is_corporate else 'YBL_Ind'
    else:
        return 'non_ybl_cor' if is_corporate else 'non_ybl_ind'

classify_transaction_udf = udf(classify_transaction, StringType())

# Sample data and creating a DataFrame
data = [
    ("ANUPAM RASAYAN INDIA LTD", "NONE   FUND TRANSFER", "Anupam Rasayan India Ltd SCB 51759", "SCBL0036063", "123456789012345", "current"),
    ("Vishal", "Shopping at Amazon", "Amazon", "HDFC0000001", "1234567890123", "saving"),
    ("Nagesh", "self expense", "nagesh deshmukh", "HDFC0000001", "1234567890123", "saving"),
    ("Laxita", "rtgs transfer", "tata ltd", "HDFC0000001", "1234567890123", "current"),
    ("Ragesh", "taxi rent uber", "manish", "YESB0000001", "123456789012345", "saving"),
    ("Ragesh", "health insuranc", "manish", "YESB0000001", "123456789012345", "saving")
    # ... other rows ...
]
columns = ["remitter_name", "base_txn_text", "benef_name", "benef_ifsc", "benef_account_no", "source"]
df = spark.createDataFrame(data, columns)

# Perform the classification
df = df.withColumn('cor_ind_benf', classify_transaction_udf(
    col('benef_ifsc'),
    col('benef_account_no'),
    col('source'),
    col('benef_name'))
)

# First, create the 'categories' struct column
df = df.withColumn('categories', category_udf(col('base_txn_text'), col('benef_name')))

# Now, you can access the struct fields category_level1 and category_level2 from 'categories'
df = df.withColumn('category_level1', col('categories')['category_level1'])

# Define the UDF to check if all words in remitter_name are in benef_name
def all_words_present(remitter_name, benef_name):
    remitter_words = set(remitter_name.lower().split(' '))  # Split by space explicitly
    benef_words = set(benef_name.lower().split(' '))        # Split by space explicitly
    return remitter_words.issubset(benef_words)

# Register the UDF with Spark
all_words_present_udf = udf(all_words_present, BooleanType())

# Add a new column that uses the UDF to determine if category_level2 should be 'Personal Transfer'
df = df.withColumn(
    'category_level2',
    when(
        all_words_present_udf(col('remitter_name'), col('benef_name')),
        'Personal Transfer'
    ).otherwise(
        col('categories')['category_level2']
    )
)

df = df.drop('categories', 'source')

# Show the DataFrame
df.show(truncate=False)

# Stop the Spark session
spark.stop()

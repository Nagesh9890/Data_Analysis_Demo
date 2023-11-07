{'db_gold.GLD_LIAB_TRANSACTION',
 'db_gold.GLD_MDM_CUST_POOL',
 'db_smith.SMTH_POOL_DC',
 'db_smith.SMTH_POOL_DDISSUE',
 'db_smith.SMTH_POOL_EPI',
 'db_smith.SMTH_POOL_IMPS',
 'db_smith.SMTH_POOL_IWCHQ',
 'db_smith.SMTH_POOL_NACH',
 'db_smith.SMTH_POOL_NEFT',
 'db_smith.SMTH_POOL_RTGS',
 'db_smith.SMTH_POOL_SI',
 'db_smith.SMTH_POOL_UPI',
 'db_smith.smth_pool_base_infra_trf',
 'db_smith.smth_pool_base_owchq',
 'db_smith.smth_pool_esb_ecollect',
 'db_smith.smth_pool_rtgs',
 'db_stage.STG_FCR_FCRLIVE_1_BA_CCY_CODE',
 'db_stage.STG_FCR_FCRLIVE_1_SM_USER_PROFILE',
 'db_stage.stg_fcr_fcrlive_1_ba_cc_brn_mast',
 'db_stage.stg_fcr_fcrlive_1_xf_ol_st_cotxn_mmdd',
 'db_stage.stg_fle_category_master',
 'db_stage.stg_fle_txn_mnemonics_master'}


----------------------
from pyspark.sql import SparkSession
import pandas as pd
import ast
from pyspark.sql.functions import udf, col, when, lower, regexp_replace
from pyspark.sql.types import StructType, StructField, StringType, BooleanType

# Read the CSV file using pandas
keywords_df = pd.read_csv("classification_keywords.csv")

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
        print("Skipping this row...\n")

# Define the UDF to preprocess base_txn_text
def preprocess_text(text):
    if text is not None:
        text = text.lower()
        text = ''.join([i for i in text if not i.isdigit()])  # Remove all digits
        text = text.replace('null', '').replace('none', '')  # Remove 'null' and 'none'
        text = ' '.join(text.split())  # Split by spaces and rejoin to remove extra whitespace
    return text or ''  # Return empty string if text is None

preprocess_text_udf = udf(preprocess_text, StringType())

# Define the UDF to set category levels
def set_category_levels(base_txn_text, benef_name):
    base_txn_text = preprocess_text(base_txn_text)
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

def all_words_present(remitter_name, benef_name):
    remitter_words = set(remitter_name.lower().split(' '))  # Split by space explicitly
    benef_words = set(benef_name.lower().split(' '))        # Split by space explicitly
    return remitter_words.issubset(benef_words)

all_words_present_udf = udf(all_words_present, BooleanType())


# Perform the classification and preprocessing
df = df.withColumn('base_txn_text', preprocess_text_udf(col('base_txn_text')))
df = df.withColumn('cor_ind_benf', classify_transaction_udf(col('benef_ifsc'), col('benef_account_no'), col('source'), col('benef_name')))
df = df.withColumn('categories', category_udf(col('base_txn_text'), col('benef_name')))
df = df.withColumn('category_level1', col('categories')['category_level1'])
df = df.withColumn('category_level2', when(all_words_present_udf(col('remitter_name'), col('benef_name')), 'PERSONAL TRANSFER')
                                        .otherwise(col('categories')['category_level2']))

df = df.drop('categories', 'source')

# Show the DataFrame
df.show(truncate=False)

# Stop the Spark session
spark.stop()

from pyspark.sql.functions import udf, col, when
from pyspark.sql.types import StringType, BooleanType

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

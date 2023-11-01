import re
import pickle
from pyspark.sql import functions as F
from sklearn.feature_extraction.text import TfidfVectorizer

# Define the custom tokenizer
def custom_tokenizer(text):
    pattern = re.compile(r'[a-zA-Z]+\d+')
    return pattern.findall(text)

# Load the saved models and vectorizers
models = ['clf_cat1.pkl', 'clf_cat2.pkl', 'tfidf_payer_name.pkl', 'tfidf_payee_name.pkl',
          'tfidf_payee_account_type.pkl', 'tfidf_payer_account_type.pkl', 'tfidf_payer_vpa.pkl', 'tfidf_payee_vpa.pkl']

clf_cat1, clf_cat2, tfidf_payer_name, tfidf_payee_name, tfidf_payee_account_type, tfidf_payer_account_type, tfidf_payer_vpa, tfidf_payee_vpa = [
    pickle.load(open(model, 'rb')) for model in models]

# Define the start and end dates
start_date = '2022-01-31'
end_date = '2022-01-31'

# Read and filter the data for the specified date range
df2 = sqlContext.table('loyalty_out_test.prospect_phonepe_leads').filter((F.col('data_dt') >= start_date) & (F.col('data_dt') <= end_date)).limit(80000)

# Convert the Spark DataFrame to Pandas DataFrame for processing
df = df2.toPandas()

# Handle NaN values by replacing them with empty strings
df.fillna("", inplace=True)

# Transform the features
features = pd.concat([
    pd.DataFrame(tfidf_vectorizer.transform(df[column]).toarray())
    for tfidf_vectorizer, column in zip([tfidf_payer_name, tfidf_payee_name, tfidf_payee_account_type,
                                         tfidf_payer_account_type, tfidf_payer_vpa, tfidf_payee_vpa],
                                        ['payer_name', 'payee_name', 'payee_account_type',
                                         'payer_account_type', 'payer_vpa', 'payee_vpa'])
], axis=1)

# Predict categories
df['category_level1'] = clf_cat1.predict(features)
df['category_level2'] = clf_cat2.predict(features)

# Convert the Pandas DataFrame back to a Spark DataFrame
result_df = sqlContext.createDataFrame(df)

# Show the resulting DataFrame
result_df.select(['payer_name', 'payee_name', 'category_level1', 'category_level2']).show()

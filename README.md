from pyspark.ml.classification import RandomForestClassificationModel
from pyspark.ml.feature import HashingTF, IDF, Tokenizer, VectorAssembler
from pyspark.sql.functions import size

# Load the saved models
model_cat1 = RandomForestClassificationModel.load("model_cat1")
model_cat2 = RandomForestClassificationModel.load("model_cat2")

# Assuming you've uploaded the CSV to Colab and read it into a DataFrame called df
# Extract the first 4 records
sample_df = df.limit(4)

# Tokenize the columns
tokenizer_inputs = ['payer_name', 'payee_name', 'payee_account_type', 'payer_account_type', 'payer_vpa', 'payee_vpa']

for inputCol in tokenizer_inputs:
    tokenizer = Tokenizer(inputCol=inputCol, outputCol=inputCol + "_tokens")
    sample_df = tokenizer.transform(sample_df)
    # Filter out rows where the tokenized column is empty
    sample_df = sample_df.filter(size(inputCol + "_tokens") > 0)

    # Use HashingTF and IDF to convert tokens to features
    hashingTF = HashingTF(inputCol=inputCol + "_tokens", outputCol=inputCol + "_tf")
    sample_df = hashingTF.transform(sample_df)
    idf = IDF(inputCol=inputCol + "_tf", outputCol=inputCol + "_features").fit(sample_df)
    sample_df = idf.transform(sample_df)

# Assemble features
assembler = VectorAssembler(inputCols=[inputCol + "_features" for inputCol in tokenizer_inputs], outputCol="features")
sample_df = assembler.transform(sample_df)

# Use the loaded model to make predictions
predictions_cat1 = model_cat1.transform(sample_df)
predictions_cat2 = model_cat2.transform(sample_df)

# Show the predictions
predictions_cat1.select('payer_name', 'prediction').show()
predictions_cat2.select('payer_name', 'prediction').show()


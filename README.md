# Select the first 4 records from df and the necessary columns for prediction
sample_df = df.select(['payer_name','payer_vpa','payee_account_type','payee_name','payee_vpa','payer_account_type']).limit(4)

# Use the same preprocessing steps as before
for inputCol in tokenizer_inputs:
    tokenizer = Tokenizer(inputCol=inputCol, outputCol=inputCol+"_tokens")
    sample_df = tokenizer.transform(sample_df)

for inputCol in tokenizer_inputs:
    hashingTF = HashingTF(inputCol="{}_tokens".format(inputCol), outputCol="{}_tf".format(inputCol))
    sample_df = hashingTF.transform(sample_df)
    idf = IDF(inputCol="{}_tf".format(inputCol), outputCol="{}_features".format(inputCol)).fit(sample_df)
    sample_df = idf.transform(sample_df)

assembler_input_cols = ["{}_features".format(inputCol) for inputCol in tokenizer_inputs]
assembler = VectorAssembler(inputCols=assembler_input_cols, outputCol="features")
sample_df = assembler.transform(sample_df)

# Get predictions using the loaded model
predictions_cat1 = loaded_model_cat1.transform(sample_df)
predictions_cat2 = loaded_model_cat2.transform(sample_df)

# Print the predictions
for row in predictions_cat1.select("prediction").collect():
    print "Predicted Category1:", row['prediction']

for row in predictions_cat2.select("prediction").collect():
    print "Predicted Category2:", row['prediction']

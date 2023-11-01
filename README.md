# Demo_4

from IPython.core.display import HTML,display
display(HTML("<style>pre { white-space: pre !important; }</style>"))

------------------------------------------------------------------------

from pyspark.ml.feature import HashingTF, IDF, Tokenizer, VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import StringIndexer

# Assuming you've uploaded the CSV to Colab
df = spark.read.csv("/content/PhonePe_Sherloc_Categories_updated_Smaller_One.csv", header=True, inferSchema=True)
df.show()

# Select the relevant columns
df2 = df.select(['payer_name','payer_vpa','payee_account_type','payee_name','payee_vpa','payer_account_type', 'Category1','Category2'])

# Convert string labels to numeric
label_indexer_cat1 = StringIndexer(inputCol="Category1", outputCol="label_cat1").fit(df2)
label_indexer_cat2 = StringIndexer(inputCol="Category2", outputCol="label_cat2").fit(df2)
df2 = label_indexer_cat1.transform(df2)
df2 = label_indexer_cat2.transform(df2)

# Tokenize the columns
tokenizer_inputs = ['payer_name', 'payee_name', 'payee_account_type', 'payer_account_type', 'payer_vpa', 'payee_vpa']
for inputCol in tokenizer_inputs:
    tokenizer = Tokenizer(inputCol=inputCol, outputCol=inputCol+"_tokens")
    df2 = tokenizer.transform(df2)

# Use HashingTF and IDF to convert tokens to features
for inputCol in tokenizer_inputs:
    hashingTF = HashingTF(inputCol=f"{inputCol}_tokens", outputCol=f"{inputCol}_tf")
    df2 = hashingTF.transform(df2)
    idf = IDF(inputCol=f"{inputCol}_tf", outputCol=f"{inputCol}_features").fit(df2)
    df2 = idf.transform(df2)

# Assemble features
assembler = VectorAssembler(inputCols=[f"{inputCol}_features" for inputCol in tokenizer_inputs], outputCol="features")
df2 = assembler.transform(df2)

# Split data
train, test = df2.randomSplit([0.8, 0.2], seed=42)

# Train Random Forest models
rf_cat1 = RandomForestClassifier(labelCol="label_cat1", featuresCol="features")
rf_cat2 = RandomForestClassifier(labelCol="label_cat2", featuresCol="features")

model_cat1 = rf_cat1.fit(train.select("features", "label_cat1"))
model_cat2 = rf_cat2.fit(train.select("features", "label_cat2"))

# Predictions
predictions_cat1 = model_cat1.transform(test)
predictions_cat2 = model_cat2.transform(test)

# Evaluate
evaluator_cat1 = MulticlassClassificationEvaluator(labelCol="label_cat1", predictionCol="prediction", metricName="accuracy")
accuracy_cat1 = evaluator_cat1.evaluate(predictions_cat1)
print("Accuracy for Category Level 1: %.2f" % accuracy_cat1)

evaluator_cat2 = MulticlassClassificationEvaluator(labelCol="label_cat2", predictionCol="prediction", metricName="accuracy")
accuracy_cat2 = evaluator_cat2.evaluate(predictions_cat2)
print("Accuracy for Category Level 2: %.2f" % accuracy_cat2)

-----------------

from pyspark.ml.feature import HashingTF, IDF, Tokenizer, VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

# Assuming you've uploaded the CSV to your environment
df = spark.read.csv("/content/PhonePe_Sherloc_Categories_updated_Smaller_One.csv", header=True, inferSchema=True)
df.show()

from pyspark.ml.feature import StringIndexer

# Select the relevant columns
df2 = df.select(['payer_name','payer_vpa','payee_account_type','payee_name','payee_vpa','payer_account_type', 'Category1','Category2'])

# Convert string labels to numeric
label_indexer_cat1 = StringIndexer(inputCol="Category1", outputCol="label_cat1").fit(df2)
label_indexer_cat2 = StringIndexer(inputCol="Category2", outputCol="label_cat2").fit(df2)
df2 = label_indexer_cat1.transform(df2)
df2 = label_indexer_cat2.transform(df2)

# Tokenize the columns
tokenizer_inputs = ['payer_name', 'payee_name', 'payee_account_type', 'payer_account_type', 'payer_vpa', 'payee_vpa']
for inputCol in tokenizer_inputs:
    tokenizer = Tokenizer(inputCol=inputCol, outputCol="{}_tokens".format(inputCol))
    df2 = tokenizer.transform(df2)

# Use HashingTF and IDF to convert tokens to features
for inputCol in tokenizer_inputs:
    hashingTF = HashingTF(inputCol="{}_tokens".format(inputCol), outputCol="{}_tf".format(inputCol))
    df2 = hashingTF.transform(df2)
    idf = IDF(inputCol="{}_tf".format(inputCol), outputCol="{}_features".format(inputCol)).fit(df2)
    df2 = idf.transform(df2)

# Assemble features
assembler = VectorAssembler(inputCols=["{}_features".format(inputCol) for inputCol in tokenizer_inputs], outputCol="features")
df2 = assembler.transform(df2)

# Split data
train, test = df2.randomSplit([0.8, 0.2], seed=42)

# Train Random Forest models
rf_cat1 = RandomForestClassifier(labelCol="label_cat1", featuresCol="features")
rf_cat2 = RandomForestClassifier(labelCol="label_cat2", featuresCol="features")

model_cat1 = rf_cat1.fit(train.select("features", "label_cat1"))
model_cat2 = rf_cat2.fit(train.select("features", "label_cat2"))

# Predictions
predictions_cat1 = model_cat1.transform(test)
predictions_cat2 = model_cat2.transform(test)

# Evaluate
evaluator_cat1 = MulticlassClassificationEvaluator(labelCol="label_cat1", predictionCol="prediction", metricName="accuracy")
accuracy_cat1 = evaluator_cat1.evaluate(predictions_cat1)
print("Accuracy for Category Level 1: %.2f" % accuracy_cat1)

evaluator_cat2 = MulticlassClassificationEvaluator(labelCol="label_cat2", predictionCol="prediction", metricName="accuracy")
accuracy_cat2 = evaluator_cat2.evaluate(predictions_cat2)
print("Accuracy for Category Level 2: %.2f" % accuracy_cat2)

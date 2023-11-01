
Py4JJavaErrorTraceback (most recent call last)
<ipython-input-32-76c6fdbbf2ec> in <module>()
     10     hashingTF = HashingTF(inputCol="{}_tokens".format(inputCol), outputCol="{}_tf".format(inputCol))
     11     sample_df = hashingTF.transform(sample_df)
---> 12     idf = IDF(inputCol="{}_tf".format(inputCol), outputCol="{}_features".format(inputCol)).fit(sample_df)
     13     sample_df = idf.transform(sample_df)
     14 

/opt/cloudera/parcels/CDH-7.1.7-1.cdh7.1.7.p1000.24102687/lib/spark/python/pyspark/ml/base.py in fit(self, dataset, params)
    130                 return self.copy(params)._fit(dataset)
    131             else:
--> 132                 return self._fit(dataset)
    133         else:
    134             raise ValueError("Params must be either a param map or a list/tuple of param maps, "

/opt/cloudera/parcels/CDH-7.1.7-1.cdh7.1.7.p1000.24102687/lib/spark/python/pyspark/ml/wrapper.py in _fit(self, dataset)
    293 
    294     def _fit(self, dataset):
--> 295         java_model = self._fit_java(dataset)
    296         model = self._create_model(java_model)
    297         return self._copyValues(model)

/opt/cloudera/parcels/CDH-7.1.7-1.cdh7.1.7.p1000.24102687/lib/spark/python/pyspark/ml/wrapper.py in _fit_java(self, dataset)
    290         """
    291         self._transfer_params_to_java()
--> 292         return self._java_obj.fit(dataset._jdf)
    293 
    294     def _fit(self, dataset):

/opt/cloudera/parcels/CDH-7.1.7-1.cdh7.1.7.p1000.24102687/lib/spark/python/lib/py4j-0.10.7-src.zip/py4j/java_gateway.py in __call__(self, *args)
   1255         answer = self.gateway_client.send_command(command)
   1256         return_value = get_return_value(
-> 1257             answer, self.gateway_client, self.target_id, self.name)
   1258 
   1259         for temp_arg in temp_args:

/opt/cloudera/parcels/CDH-7.1.7-1.cdh7.1.7.p1000.24102687/lib/spark/python/pyspark/sql/utils.pyc in deco(*a, **kw)
     61     def deco(*a, **kw):
     62         try:
---> 63             return f(*a, **kw)
     64         except py4j.protocol.Py4JJavaError as e:
     65             s = e.java_exception.toString()

/opt/cloudera/parcels/CDH-7.1.7-1.cdh7.1.7.p1000.24102687/lib/spark/python/lib/py4j-0.10.7-src.zip/py4j/protocol.py in get_return_value(answer, gateway_client, target_id, name)
    326                 raise Py4JJavaError(
    327                     "An error occurred while calling {0}{1}{2}.\n".
--> 328                     format(target_id, ".", name), value)
    329             else:
    330                 raise Py4JError(














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

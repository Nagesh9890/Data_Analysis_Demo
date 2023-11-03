TypeErrorTraceback (most recent call last)
<ipython-input-17-9885ed69d380> in <module>()
     94 
     95 # Override category_level2 if remitter_name equals benef_name
---> 96 df = df.withColumn('category_level2', when(col('remitter_name').lower() == col('benef_name').lower(), 'Personal Transfer')
     97                                     .otherwise(col('categories')['category_level2']))
     98 

TypeError: 'Column' object is not callable

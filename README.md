# Define the start and end dates
start_date = '2022-01-31'
end_date = '2022-01-31'

# Read and filter the data for the specified date range
df2 = sqlContext.table('loyalty_out_test.prospect_phonepe_leads').filter((F.col('data_dt') >= start_date) & (F.col('data_dt') <= end_date)).limit(80000)

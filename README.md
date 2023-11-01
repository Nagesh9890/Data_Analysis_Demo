SELECT COUNT(*)
FROM loyalty_out_test.prospect_phonepe_leads
WHERE txn_date = '2022-01-26' AND txn_minute = 1;




# Define the start and end dates
start_date = '2022-01-31'
end_date = '2022-01-31'

# Read and filter the data for the specified date range
df2 = sqlContext.table('loyalty_out_test.prospect_phonepe_leads').filter((F.col('data_dt') >= start_date) & (F.col('data_dt') <= end_date)).limit(80000)


# Define the desired date and minute
txn_date = '2022-01-26'
txn_minute = 1

# Read and filter the data for the specified date and minute
df2 = sqlContext.table('loyalty_out_test.prospect_phonepe_leads') \
               .filter((F.col('txn_date') == txn_date) & (F.col('txn_minute') == txn_minute)) \
               .limit(80000)

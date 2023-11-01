# Define the start and end dates
start_date = '2022-01-31'
end_date = '2022-01-31'

# Read and filter the data for the specified date range
df2 = sqlContext.table('loyalty_out_test.prospect_phonepe_leads').filter((F.col('data_dt') >= start_date) & (F.col('data_dt') <= end_date)).limit(80000)




	txn_minute	data_dt	txn_date	txn_hour
 
5
86
30
61
1
33
67
82
11
35
53
76
99
62
98
15
51
68
100
55
88
25
8
70
72
38
39
45
73
13
21
83
93
36
37
10
14
23
24
48
52
2
7
42
50
64
92
20
63
28
41
71
78
9
22
43
4
44
18
81
26
66
84
91
97
29
87
17
19
27
69
77
95
31
40
75
79
85
16
34
58
59
60
12
32
49
54
57
56
89
90
96
6
46
74
80
94
3
47
65
 
 	payer_name	payer_account_number	note	payer_amount	transaction_type	transaction_reference_id	upi_transaction_id	customer_reference_number	payer_account_type	payer_ifsc	payer_vpa	mbanking_enabled	phone_number	payee_name	payee_account_number	payee_amount	payee_account_type	payee_ifsc	payee_vpa	created_date	updated_date	txn_minute	data_dt	txn_date	txn_hour
5	Master TOFIK  IQABAL DHUKAR	00000003557891875	Payment from PhonePe	10000	PAY	T2201260300545805645302	YBL7a39b467ebe44ce388b59cb815e28fb3	202664841658	SAVINGS	CBIN0280582	7990961300@ybl	1	917990961300	SHER KHAN ZUBER KHAN	50200060288750	10000	SAVINGS	HDFC0009553	9226191624ybl@axl	2022-01-26 03:00:57.855000000	2022-01-26 03:00:58.840000000	0	2022-01-31	2022-01-26	3

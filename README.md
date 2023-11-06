------------------------------
Set Category Levels Function
This function (set_category_levels) is used to assign category levels to each transaction based on 
the transaction text (base_txn_text) and the beneficiary's name (benef_name). Here's the detailed 
logic:
• Normalize Text: It converts both base_txn_text and benef_name to lowercase to ensure 
that keyword matching is case-insensitive.
• Keyword Search: The function iterates over each keyword in the keywords_dict. If any 
keyword is found in the base_txn_text or the benef_name, it means the transaction is 
related to that keyword.
• Return Categories: When a keyword is found, the function returns the corresponding 
category_level1 and category_level2 from the dictionary. If no keyword matches, it 
returns the default categories 'OTHER TRANSFER' and 'OTHER'.
Spark UDF for Category Classification
A User-Defined Function (UDF) in Spark is a feature that allows you to extend the capabilities of 
Spark's DataFrame API by running custom Python code. The UDF created from the 
set_category_levels function is intended to be used with Spark DataFrames. When you apply this 
UDF to a DataFrame, it operates row-wise, taking the transaction text and beneficiary name from 
each row, applying the set_category_levels function, and returning the category levels as a 
structured type with two fields: category_level1 and category_level2.
------------------------------------------------------
Transaction Classification Function
The classify_transaction function assigns a classification type to transactions based on several 
criteria:
• IFSC Code: Checks if the benef_ifsc starts with "YESB", which might indicate a transaction 
related to the "Yes Bank" or similar.
• Transaction Source: It looks at the source column to see if the transaction came from a 
'current' or 'saving' account.
• Beneficiary Name Analysis: The function checks if the benef_name contains any of a list of 
corporate-related keywords. If it does, it suggests that the beneficiary is a corporate entity.
• Determine Classification:
• If the IFSC code indicates "Yes Bank", the function classifies the transaction as 'YBL_Corp' if 
the source is 'current', or 'YBL_Ind' if the source is 'saving'. If the source is not provided, it 
defaults to 'YBL_Corp' if corporate keywords are found, otherwise 'YBL_Ind'.
• If the IFSC does not indicate "Yes Bank", it uses the presence of corporate keywords to 
classify the transaction as 'non_ybl_cor' for corporate or 'non_ybl_ind' for individual.
Update DataFrame with Classifications
This part of the code integrates the classification logic into the DataFrame. After applying the 
classify_transaction_udf, the DataFrame gets a new column (cor_ind_benf) with the transaction 
types as determined by the classify_transaction function.
Additionally, the script uses another UDF (all_words_present_udf) to potentially reclassify 
category_level2 based on the comparison of remitter_name and benef_name:
• If all words from remitter_name are found within benef_name, this suggests a personal 
transfer, possibly indicating that the remitter and the beneficiary are the same person or 
closely related. In this case, category_level2 is set to 'Personal Transfer'.
• If not all words are found, it keeps the category_level2 as determined by the category_udf.
This logic is implemented using Spark's when function, which works similarly to an IF-THEN-ELSE
statement, allowing conditional logic to be embedded within the DataFrame transformations.
By applying these functions and UDFs, the script enriches the DataFrame with new columns that 
classify the transactions both in terms of category levels and the nature of the beneficiary (individual 
or corporate). These classifications are derived from analyzing the transaction text, beneficiary 
details, and remitter information, providing a structured approach to understanding the 
transactions in the dataset.
------------------------------------------
Self-Transfer Logic :
The user-defined function, named all_words_present_udf, is created to determine whether all the 
words in the remitter_name are present in the benef_name. The logic behind this is to help identify 
personal transfers, under the assumption that a personal transfer is likely to have the remitter's 
name included in the beneficiary's name.
Logic of the UDF
Here is the step-by-step logic that the UDF follows:
• Tokenize Names: It splits the remitter_name and benef_name into sets of words, converting 
them to lowercase to ensure case-insensitive matching.
• Check Subset: It checks if the set of words from remitter_name is a subset of the set of 
words from benef_name. Being a subset means that all elements (words) of the 
remitter_name are contained within the benef_name.
• Return Boolean: The UDF returns a boolean value: True if all words in the remitter_name
are found in the benef_name, and False otherwise.
Integration into DataFrame
This UDF is used within the DataFrame transformation to conditionally set the category_level2
field. The when function is used here, which operates like an if statement:
• Condition: The script checks the condition provided by all_words_present_udf for each 
row.
• Action: If the condition is True (meaning all words in remitter_name are also in 
benef_name), the category_level2 is set to 'Personal Transfer'.
• Otherwise: If the condition is False, it retains the category_level2 as assigned by the 
previous classification logic (category_udf).
Purpose of the UDF
The main purpose of this UDF is to add another layer of classification that can identify personal 
transactions as distinct from business or unrelated transactions. This is based on the heuristic that 
personal transactions will often include similar or identical names for the remitter and beneficiary.
Example
Here's an example of how this logic would work with a sample row from the DataFrame:
• Suppose we have a remitter_name "John Doe" and a benef_name "John Doe Savings 
Account".
• The all_words_present_udf would tokenize both to {"john", "doe"} and {"john", 
"doe", "savings", "account"} respectively.
• Since every word in the remitter_name is also in the benef_name, the UDF would return 
True.
• As a result, category_level2 for this row would be set to 'Personal Transfer'.

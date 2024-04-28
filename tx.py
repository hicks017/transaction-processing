import os
import sqlite3
import pandas as pd
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
SOURCE_ONE = os.environ.get("SOURCE_ONE")
SOURCE_TWO = os.environ.get("SOURCE_TWO")
SOURCE_THREE = os.environ.get("SOURCE_THREE")
SOURCE_FOUR = os.environ.get("SOURCE_FOUR")
SOURCE_FIVE = os.environ.get("SOURCE_FIVE")
SOURCE_SIX = os.environ.get("SOURCE_SIX")
SOURCE_SEVEN = os.environ.get("SOURCE_SEVEN")
DB_NAME = os.environ.get("DB_NAME")

# Define standardize column order
col_order = ['source', 'date', 'description', 'category', 'amount']

# Define source_1 function
def source_1(csv_path):

    # Load data
    df = pd.read_csv(csv_path)

    ## 1. Change category for credit card payments to transfers
    condition = df['Type'] == "Payment"
    replacement = "Transfer"
    df.loc[condition, 'Category'] = replacement

    ## 2. Change category of redemption creidt to rewards
    condition = df['Description'] == "REDEMPTION CREDIT"
    replacement = "Rewards"
    df.loc[condition, 'Category'] = replacement

    ## 3. Modify column names
    df = df.rename(
        {
            'Transaction Date': 'date',
            'Description': 'description',
            'Category': 'category',
            'Amount': 'amount'
        },
        axis='columns'
    )

    ## 4. Remove uninterested columns
    df = df.drop(['Post Date', 'Type', 'Memo'], axis='columns')

    ## 5. Insert source of data to first column
    df.insert(0, 'source', SOURCE_ONE)

    ## 6. Change date format from mm/dd/yyyy to yyyy-mm-dd
    df['date'] = pd.to_datetime(df['date'])

    # Finish
    return df

# Define source_2 function
def source_2(csv_path):

    # Load data and fix missing comma on header row from Source 2 files
    cols = [
        'Details',
        'Posting Date',
        'Description',
        'Amount',
        'Type',
        'Balance',
        'Check or Slip #'
    ]
    cols.append('blank_col_fix')
    df = pd.read_csv(csv_path, skiprows=1, names=cols)

    ## 1. Modify column names
    df = df.rename(
        {
        'Posting Date': 'date',
        'Description': 'description',
        'Type': 'category',
        'Amount': 'amount'
        },
        axis='columns'
    )

    ## 2. Remove uninterested columns
    df = df.drop(
        ['Details', 'Balance', 'Check or Slip #', 'blank_col_fix'],
        axis='columns'
    )

    ## 3. Insert source of data to first column
    df.insert(0, 'source', SOURCE_TWO)

    ## 4. Change date format from mm/dd/yyyy to yyyy-mm-dd
    df['date'] = pd.to_datetime(df['date'])

    ## 5. Reorder columns
    df = df[col_order]

    # Finish
    return df

def source_3(csv_path):

    # Load data
    df = pd.read_csv(csv_path)

    ## 1. Modify column names
    df = df.rename(
        {
            'Transaction Date': 'date',
            'Description': 'description',
            'Category': 'category'
        },
        axis='columns'
    )

    ## 2. Insert source of data to first column
    df.insert(0, 'source', SOURCE_THREE)

    ## 3. Convert debits to negative number and combine with credits
    df['Debit'] *= -1
    df['amount'] = df['Debit'].fillna(df['Credit'])

    ## 4. Remove uninterested columns
    df = df.drop(['Posted Date', 'Card No.', 'Debit', 'Credit'], axis='columns')

    ## 5. Change category of credit card payments to transfer
    condition = df['category'] == "Payment/Credit"
    replacement = "Transfer"
    df.loc[condition, 'category'] = replacement

    # Finish
    return df

def source_4(csv_path):

    # Load data
    df = pd.read_csv(csv_path)

    ## 1. Modify column names
    df = df.rename(
        {
            'Transaction Date': 'date',
            'Description': 'description',
            'Category': 'category'
        },
        axis='columns'
    )

    ## 2. Insert source of data to first column
    df.insert(0, 'source', SOURCE_FOUR)

    ## 3. Convert debits to negative number and combine with credits
    df['Debit'] *= -1
    df['amount'] = df['Debit'].fillna(df['Credit'])

    ## 4. Remove uninterested columns
    df = df.drop(['Posted Date', 'Card No.', 'Debit', 'Credit'], axis='columns')

    # Finish
    return df

def source_5(csv_path):

    # Load data
    df = pd.read_csv(csv_path)

    ## 1. Change category of credit card payments to transfer
    condition = df['Type'] == "Credit"
    replacement = "Transfer"
    df.loc[condition, 'Category'] = replacement

    ## 2. Modify column names
    df = df.rename(
        {
            'Trans Date': 'date',
            'Description': 'description',
            'Category': 'category',
            'Amount': 'amount'
        },
        axis='columns'
    )

    ## 3. Insert source of data to first column
    df.insert(0, 'source', SOURCE_FIVE)

    ## 4. Edit debit/credit format from x/(x) to x/-x and remove dollar sign
    def process_target_amount(amount):
        amount = amount.replace('$', '')
        if '(' in amount:
            return amount.strip('()')
        else:
            return '-' + amount
    
    df['amount'] = df['amount'].apply(process_target_amount)

    ## 5. Change date format from mm/dd/yyyy to yyyy-mm-dd
    df['date'] = pd.to_datetime(df['date'])

    ## 6. Remove uninterested columns
    df = df.drop(
        [
            'Originating Account Last 4',
            'Posting Date',
            'Type',
            'Merchant Name',
            'Merchant City',
            'Merchant State',
            'Transaction Type',
            'Reference Number'
        ],
        axis='columns'
    )

    ## 7. Reorder columns
    df = df[col_order]

    # Finish
    return df

def source_6(csv_path):

    # Load data
    colnames = ['date', 'amount', 'x', 'category', 'description']
    df = pd.read_csv(csv_path)

    ## 1. Change category of credit card payments to transfer
    df = df.astype({'category': str})

    condition = df['description'].isin(['ONLINE PAYMENT THANK YOU', 'BILL PAY PAYMENT'])
    replacement = "Transfer"
    df.loc[condition, 'category'] = replacement

    df['category'] = df['category'].replace('nan', '')

    ## 2. Change date format from mm/dd/yyyy to yyyy-mm-dd
    df['date'] = pd.to_datetime(df['date'])

    ## 3. Insert source of data to first column
    df.insert(0, 'source', SOURCE_SIX)

    ## 5. Remove uninterested columns
    df = df.drop(['x'], axis='columns')

    ## 6. Reorder columns
    df = df[col_order]

    # Finish
    return df

def source_7(csv_path):

    # Load data
    df = pd.read_csv(csv_path)

    ## 1. Change date format from mm/dd/yyyy to yyyy-mm-dd
    df['date'] = pd.to_datetime(df['date'])

    ## 2. Insert source of data to first column
    df.insert(0, 'source', SOURCE_SEVEN)

    ## 3. Remove uninterested columns
    df = df.drop(['x'], axis='columns')

    ## 4. Reorder columns
    df = df[col_order]

    ## 5. Change category to string and remove NaN values
    df = df.astype({'category': str})
    df['category'] = df['category'].replace('nan', '')

    # Finish
    return df

# Define function to insert data into SQLite3 database
def insert_into_db(df, table_name):
    conn = sqlite3.connect(DB_NAME)
    df.to_sql(table_name, conn, if_exists='append', index=False)
    conn.close()

# Prompt for CSV path and function choice
csv_path = input("Enter the full path to the CSV file:\nPath: ")
function_choice = input("Choose a number to select a function:\n1) {SOURCE_ONE}\n2) {SOURCE_TWO}\n3) {SOURCE_THREE}\n4) {SOURCE_FOUR}\n5) {SOURCE_FIVE}\n6) {SOURCE_SIX}\n7) {SOURCE_SEVEN}\nSelection: ")

# Call the selected function based on received input
if function_choice == '1':
    df = source_1(csv_path)
elif function_choice == '2':
    df = source_2(csv_path)
elif function_choice == '3':
    df = source_3(csv_path)
elif function_choice == '4':
    df = source_4(csv_path)
elif function_choice == '5':
    df = source_5(csv_path)
elif function_choice == '6':
    df = source_6(csv_path)
elif function_choice == '7':
    df = source_7(csv_path)
else:
    print("Invalid choice. Rerun program and try again.")

# Insert data into SQLite3 database
table_name = input("Enter the database's table name to insert into:\nTable: ")
insert_into_db(df, table_name)

import pandas as pd
from db import AzureDB

# initialise db utility
db = AzureDB()
db.access_container("retail-sales-raw")

def transform_data(df, table_name):
    if df is None:
        return None
    print(f"Transforming {table_name}...")

    df.columns = [c.strip() for c in df.columns]

    if table_name == 'Supplier':
        df = df[['Supplier']].copy()
        df = df.dropna(subset=['Supplier']).drop_duplicates() # handle missing val
        df = df.rename(columns={'Supplier': 'Supplier_Name'}) # rename to db schema

        # supplier id (eg SUP001)
        df.insert(0, 'Supplier_ID',
                  ['SUP' + str(i).zfill(3) for i in range(1, 1 + len(df))])

    elif table_name == 'Customer':
        df['Customer_Name'] = df['First Name'] + ' ' + df['Last Name'] # combine full name
        
        # rename
        df = df.rename(columns={
            'Customer ID':'Customer_ID',
            'Home Store Location':'Customer_Address',
            'Age':'Customer_Age',
            'Gender': 'Customer_Gender'
        })

        df = df[['Customer_ID', 'Customer_Name', 'Customer_Address', 'Customer_Age', 'Customer_Gender']].copy()
        df = df.dropna(subset=['Customer_ID']).drop_duplicates() # remobe duplicate/invalid val

        df['Customer_Age'] = df['Customer_Age'].astype(int)
        df['Customer_ID']  = df['Customer_ID'].astype(str)

    elif table_name == 'Store_Manager':
        df = df[['Store Manager ID', 'Full Name', 'Email', 'Phone', 'Hire Date', 'Leadership Level']].copy()
        df = df.rename(columns={
            'Store Manager ID': 'Store_Manager_ID',
            'Full Name': 'Manager_Name',
            'Email': 'Manager_Email',
            'Phone':'Manager_Phone_Number',
            'Hire Date': 'Manager_Hire_Date',
            'Leadership Level': 'Manager_Leadership_Level'
        })

        # stored as "0400 111 201"
        df['Manager_Phone_Number'] = (
            df['Manager_Phone_Number']
            .astype(str)
            .str.replace(r'\s+', '', regex=True)
            .pipe(pd.to_numeric, errors='coerce')
            .astype(int)
        )

        df['Manager_Hire_Date'] = pd.to_datetime(df['Manager_Hire_Date'], errors='coerce')

    elif table_name == 'Store':
        df = df[['Store ID', 'Store Name', 'Store Location',
                 'Store Manager ID', 'Open Date']].copy()
        
        df = df.rename(columns={
            'Store ID':         'Store_ID',
            'Store Name':       'Store_Name',
            'Store Location':   'Store_Location',
            'Store Manager ID': 'Store_Manager_ID',
            'Open Date':        'Store_Open_Date'
        })

        df['Store_Open_Date'] = pd.to_datetime(df['Store_Open_Date'], errors='coerce')

    elif table_name == 'Product':
        df = df[['Category_Per_Price', 'Specific Product (Item)', 'Supplier']].drop_duplicates().copy()

        # generate Supplier_ID map 
        unique_suppliers = df['Supplier'].dropna().unique()
        supplier_map = {
            name: 'SUP' + str(i).zfill(3) 
            for i, name in enumerate(unique_suppliers, start=1)
        }
        df['Supplier_ID'] = df['Supplier'].map(supplier_map)

        # split category and price (original: category|100)
        split_df = df['Category_Per_Price'].str.split('|', expand=True)
        df['Product_Category'] = split_df[0].str.strip()
        df['Product_Name'] = df['Specific Product (Item)'].str.strip().str[:10]
        df['Product_Price'] = pd.to_numeric(
            split_df[1].astype(str).str.replace(r'[\$,]', '', regex=True), 
            errors='coerce'
        )

        df.insert(0, 'Product_ID', range(1, 1 + len(df)))
        df = df[['Product_ID', 'Product_Name', 'Product_Category', 'Product_Price', 'Supplier_ID']]

    elif table_name == 'Transaction_Table':
        df = df.rename(columns={
            'Transaction ID':   'Transaction_ID',
            'Customer ID':      'Customer_ID',
            'Store ID':         'Store_ID',
            'Store Manager ID': 'Store_Manager_ID',
            'Date':             'Date',
            'Quantity':         'Product_Quantity',
            'Total Amount':     'Total_Price'
        })

        # pull the mapping file from azure blob
        fm_df = db.access_blob_csv('Feature_Mappings.csv')
        fm_df.columns = [c.strip() for c in fm_df.columns]
        
        # assign pid (ID) to Category_Price_Key
        mapping = fm_df[['Category_Price_Key']].drop_duplicates().copy()
        mapping.insert(0, 'pid', range(1, 1 + len(mapping)))
        pid_map = mapping.set_index('Category_Price_Key')['pid'].to_dict()

        df['_price_key'] = (
            df['Product Category'] + '|' + 
            df['Price per Unit'].astype(str).str.replace(r'[\$,]', '', regex=True).str.split('.').str[0]
        )
        df['Product_ID'] = df['_price_key'].map(pid_map)
        df['Transaction_ID'] = df['Transaction_ID'].astype(str).str.zfill(10)
        df['Total_Price'] = pd.to_numeric(df['Total_Price'].astype(str).str.replace(r'[\$,]', '', regex=True), errors='coerce')
        df['Date'] = pd.to_datetime(df['Date'], errors='coerce')

        df = df[['Transaction_ID', 'Customer_ID', 'Product_ID', 'Store_ID', 'Store_Manager_ID', 'Date', 'Product_Quantity', 'Total_Price']]
    return df

def run_etl():
    tables = [
        {'name': 'Supplier',          'file': 'Feature_Mappings.csv'},
        {'name': 'Customer',          'file': 'Customer.csv'},
        {'name': 'Store_Manager',     'file': 'Store_Manager.csv'},
        {'name': 'Store',             'file': 'Store.csv'},
        {'name': 'Product',           'file': 'Feature_Mappings.csv'},
        {'name': 'Transaction_Table', 'file': 'retail_sales_dataset_expanded.csv'},
    ]

    for table in tables:
        try:
            print(f"--- Starting ETL for {table['name']} ---")
            df = db.access_blob_csv(table['file'])
            df = transform_data(df, table['name'])

            if df is not None:
                db.append_dataframe_sqldatabase(table['name'], df)
                print(f"Successfully loaded {len(df)} rows into {table['name']}\n")

        except Exception as e:
            if "Violation of PRIMARY KEY constraint" in str(e):
                print(f"Skipping {table['name']}: Data already exists in Database.\n")
            else:
                print(f"Error loading {table['name']}: {e}\n")
            continue


if __name__ == "__main__":
    run_etl()
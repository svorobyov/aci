import os
import pprint
import json
import sys
import pandas as pd
from sqlalchemy import create_engine, text, types
import matplotlib.pyplot as plt


_PP = pprint.PrettyPrinter(indent=4)


engine = create_engine("mysql+mysqlconnector://root:root@localhost/testdb1")  # connect to the local DB


def create_table(dataframe, table_name=None, fields=None, print_results=False):
   """From dataframe."""
   with engine.connect() as connection:
      connection.execute(text(f"DROP TABLE IF EXISTS {table_name}"))  # using textual SQL
      connection.execute(text(f"CREATE TABLE {table_name}({fields})"))
      connection.execute(text(f"DESCRIBE TABLE {table_name}"))
      n_records = dataframe.to_sql(
         name=table_name,  # table name, table is empty (DROP), just created, for types
         if_exists='append',
         index=False,  # crucial!
         method=None,  # standard SQL INSERT clause (one per row)
         con=connection)  # or engine
      if print_results:
         print(f"n_records: {n_records}")
         _PP.pprint(connection.execute(text(f"SELECT * FROM {table_name} LIMIT 10")).fetchall())
         _PP.pprint(connection.execute(text(f"SELECT count(*) FROM {table_name}")).fetchall())
      connection.commit()  # commit the transaction, I missed this!


def sql_txt_exec(command):
   with engine.connect() as connection:
      resultset = connection.execute(text(command))
      results_as_dict = resultset.mappings().all()
      print(f"*** SQL command: {command}")
      _PP.pprint(results_as_dict)


def xlxs_to_frames_to_sql(load=False):
   """Load test Excel data."""
   if not load:
      return
   xl_file = pd.ExcelFile(f"{os.environ['DATASETS_DIR']}/ica/DE case - test data.xlsx")

   # Data frames
   data_frames = {sheet_name: xl_file.parse(sheet_name) for sheet_name in xl_file.sheet_names}
   print(f"data frames: {list(data_frames.keys())}")

   # Frames for tables
   df_customers = data_frames['Customers']
   df_transactions = data_frames['Transactions']
   df_offers = data_frames['Offers']

   if False:
      print("Basic info about initial frames:")
      print(df_customers.describe())
      print(df_transactions.describe())
      print(df_offers.describe())

   # Remove duplicated *_ids
   df_customers_d = df_customers.drop_duplicates(subset=['customer_id'], keep='first')
   df_transactions_d = df_transactions.drop_duplicates(subset=['transaction_id'], keep='first')
   df_offers_d = df_offers.drop_duplicates(subset=['offer_id'], keep='first')

   if False:
      print("Basic info about DEDUPLICATED frames:")
      print(df_customers_d.describe())
      print(df_transactions_d.describe())
      print(df_offers_d.describe())

   print(f"initial      'transactions' frame shape: {df_transactions.shape}")
   print(f"deduplicated 'transactions' frame shape: {df_transactions_d.shape} (transaction_id)")
   print(f"initial      'customers'    frame shape: {df_customers.shape}")
   print(f"deduplicated 'customers'    frame shape: {df_customers_d.shape} (customer_id)")
   print(f"initial      'offers'       frame shape: {df_offers.shape}")
   print(f"deduplicated 'offers'       frame shape: {df_offers_d.shape} (offer_id)")

   # sys.exit(0)  # Demo step 1

   # TODO: create tables, fill them up from deduplicated data frames
   # offers_fields = "customer_id INT, offer_id BIGINT, start_date DATE, end_date DATE"  # all fields inside parentheses in CTREATE table (...)
   offers_fields = "customer_id INT, offer_id BIGINT PRIMARY KEY, start_date DATE, end_date DATE"  # TODO: error, duplicate primary keys! Duplicate entry '1184093' for key 'offers.PRIMARY'
   create_table(df_offers_d, table_name="offers", fields=offers_fields)

   # customers_fields = "customer_name VARCHAR(32), customer_id INT, customer_dob DATE, gender VARCHAR(2)"  # all fields inside parentheses in CTREATE table (...)
   customers_fields = "customer_name VARCHAR(32), customer_id INT PRIMARY KEY, customer_dob DATE, gender VARCHAR(2)"  # requires deduplication on PRIMARY KEY
   create_table(df_customers_d, table_name="customers", fields=customers_fields)

   transactions_fields0 = "transaction_id INT, customer_id INT, product_id INT, store_id INT, offer_id BIGINT, sales_amount DOUBLE(53, 2), transaction_date DATE"
   transactions_fields = "transaction_id INT PRIMARY KEY, customer_id INT, product_id INT, store_id INT, offer_id BIGINT, sales_amount DOUBLE(53, 2), transaction_date DATE"
   create_table(df_transactions, table_name="transactions0", fields=transactions_fields0)
   create_table(df_transactions_d, table_name="transactions", fields=transactions_fields)


# Load/refresh data if needed
# xlxs_to_frames_to_sql(load=True)
xlxs_to_frames_to_sql(load=False)

# Show resulting SQL data
sql_txt_exec("SELECT COUNT(*) FROM offers")
sql_txt_exec("SELECT * FROM offers LIMIT 6")
sql_txt_exec("SELECT COUNT(*) FROM customers")
sql_txt_exec("SELECT * FROM customers LIMIT 6")
sql_txt_exec("SELECT COUNT(*) FROM transactions0")
sql_txt_exec("SELECT * FROM transactions0 LIMIT 6")
sql_txt_exec("SELECT COUNT(*) FROM transactions")
sql_txt_exec("SELECT * FROM transactions LIMIT 6")

# sys.exit(0)

sql_txt_exec("SELECT * FROM per_age_per_dow")

with engine.connect() as connection:
   df1 = pd.read_sql_table(
      'per_age_per_dow',
      con=connection
   )
   # df1.reset_index(drop=True)
   # df1.drop(index=False, inplace=True)
   df1.reset_index(drop=True, inplace=True)
   print(df1)

df1.plot(x='age')
plt.show()

sys.exit(0)
df2 = pd.DataFrame({
   'Name': ['John', 'Sammy', 'Joe'],
   'Age': [45, 38, 90]
})

# plotting a bar graph
df2.plot(x="Name", y="Age", kind="bar")
plt.show()

# QUERY1 = """
# """

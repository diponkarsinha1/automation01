import pandas as pd
from sqlalchemy import create_engine
import schedule
import time
import os

# === 1. MySQL Configuration ===
user = ''
password = ''
host = ''
port = ''

raw_db = 'JnJSourcing'
clean_db = 'JSourcingCleanData'

# Engines
raw_engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}:{port}/{raw_db}")
clean_engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}:{port}/{clean_db}")

# === 2. Helper Functions ===
def get_max_id(table, id_col, engine):
    try:
        query = f"SELECT MAX({id_col}) as max_id FROM {table}"
        return pd.read_sql(query, engine)['max_id'][0]
    except Exception as e:
        print(f"❌ Error fetching max ID from {table}: {e}")
        return 0

def read_last_id(filename):
    try:
        if os.path.exists(filename):
            with open(filename, 'r') as f:
                return int(f.read().strip())
    except:
        pass
    return 0

def write_last_id(filename, max_id):
    try:
        with open(filename, 'w') as f:
            f.write(str(max_id))
    except Exception as e:
        print(f"❌ Error writing to {filename}: {e}")

# === 3. ETL Logic ===
def etl_if_new_data():
    print("\n🔍 Checking for new data...")

    table_configs = {
        'Supplier_Information_Raw': ('SupplierID', 'last_supplierid.txt'),
        'Supplier_Performance_Raw': ('PerformanceID', 'last_performanceid.txt'),
        'Sourcing_Contracts_Raw': ('ContractID', 'last_contractid.txt'),
        'Purchase_Orders_Raw': ('PO_ID', 'last_poid.txt'),
        'Purchase_Order_Details_Raw': ('PODetailID', 'last_podetailid.txt'),
        'Spend_Analysis_Raw': ('SpendID', 'last_spendid.txt')
    }

    any_new_data = False

    for t, (id_col, id_file) in table_configs.items():
        try:
            last_id = read_last_id(id_file)
            cur_id = get_max_id(t, id_col, raw_engine)
            print(f"📊 Table: {t} | Last ID: {last_id} | Current Max ID: {cur_id}")

            if cur_id and cur_id > last_id:
                any_new_data = True
                df = pd.read_sql(f"SELECT * FROM {t}", raw_engine)

                # === Cleaning & Aggregation ===
                if t == 'Supplier_Information_Raw':
                    df['Country'] = df['Country'].str.strip().str.title().replace('', pd.NA)
                    agg = df.groupby('Country')['SupplierID'].nunique().reset_index(name='SupplierCount')
                    clean_name, agg_name = 'Supplier_Information', 'Agg_SupplierCountByCountry'

                elif t == 'Supplier_Performance_Raw':
                    df['QualityScore'] = pd.to_numeric(df['QualityScore'], errors='coerce')
                    agg = df.groupby('SupplierID')['QualityScore'].mean().reset_index(name='AvgQualityScore')
                    clean_name, agg_name = 'Supplier_Performance', 'Agg_AvgQualityScoreBySupplier'

                elif t == 'Sourcing_Contracts_Raw':
                    df['ContractValue'] = pd.to_numeric(df['ContractValue'], errors='coerce')
                    agg = df.groupby('SupplierID')['ContractValue'].sum().reset_index(name='TotalContractValue')
                    clean_name, agg_name = 'Sourcing_Contracts', 'Agg_TotalContractValueBySupplier'

                elif t == 'Purchase_Orders_Raw':
                    df['Currency'] = df['Currency'].str.upper().str.strip().replace('', pd.NA)
                    df['Total_Amount'] = pd.to_numeric(df['Total_Amount'], errors='coerce')
                    agg = df.groupby('Currency')['Total_Amount'].sum().reset_index(name='TotalSpend')
                    clean_name, agg_name = 'Purchase_Orders', 'Agg_TotalSpendByCurrency'

                elif t == 'Purchase_Order_Details_Raw':
                    df['Quantity'] = pd.to_numeric(df['Quantity'], errors='coerce')
                    agg = df.groupby('ItemID')['Quantity'].sum().reset_index(name='TotalQuantity')
                    clean_name, agg_name = 'Purchase_Order_Details', 'Agg_TotalQuantityByItem'

                elif t == 'Spend_Analysis_Raw':
                    df['Category'] = df['Category'].str.strip().str.title().replace('', pd.NA)
                    df['Amount_Spend'] = pd.to_numeric(df['Amount_Spend'], errors='coerce')
                    agg = df.groupby('Category')['Amount_Spend'].sum().reset_index(name='TotalSpend')
                    clean_name, agg_name = 'Spend_Analysis', 'Agg_TotalSpendByCategory'

                # === Write to Clean DB ===
                try:
                    df.to_sql(clean_name, clean_engine, if_exists='replace', index=False)
                    print(f"✅ Cleaned data written to `{clean_name}`")
                except Exception as e:
                    print(f"❌ Failed writing {clean_name}: {e}")

                try:
                    agg.to_sql(agg_name, clean_engine, if_exists='replace', index=False)
                    print(f"✅ Aggregated data written to `{agg_name}`")
                except Exception as e:
                    print(f"❌ Failed writing {agg_name}: {e}")

                write_last_id(id_file, cur_id)
            else:
                print(f"⏸ No new data in `{t}`")

        except Exception as e:
            print(f"❌ Error processing table {t}: {e}")

    if any_new_data:
        print("🎉 ETL completed and clean database updated.")
    else:
        print("👍 All tables are up to date.")

# === 4. Schedule ETL Every 5 Minutes/// depend we can change the time ===
schedule.every(1).seconds.do(etl_if_new_data)

print("🔁 Automated ETL started. It checks every 5 minutes... (Press Ctrl+C to stop)")
while True:
    schedule.run_pending()
    time.sleep(1)

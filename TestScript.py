import pandas as pd
from sqlalchemy import create_engine
import schedule
import time

# === 1. MySQL Configuration ===
user = 'r'
password = ''
host = ''
port = ''

raw_db = 'JnJSourcing'
clean_db = 'JSourcingCleanData'

# Create SQLAlchemy engines for raw and clean databases
raw_engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}:{port}/{raw_db}")
clean_engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}:{port}/{clean_db}")

# === 2. Universal Cleaner ===
def clean_dataframe(df):
    df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)  # strip whitespace
    df.replace('', pd.NA, inplace=True)  # replace blank strings with NaN
    df.dropna(inplace=True)  # drop rows with any null
    return df

# === 3. ETL Task ===
def etl_clean_all():
    print("\n🔄 Starting full ETL clean of all raw tables...")

    table_configs = {
        'Supplier_Information_Raw': ('SupplierID', 'Supplier_Information', 'Agg_SupplierCountByCountry'),
        'Supplier_Performance_Raw': ('PerformanceID', 'Supplier_Performance', 'Agg_AvgQualityScoreBySupplier'),
        'Sourcing_Contracts_Raw': ('ContractID', 'Sourcing_Contracts', 'Agg_TotalContractValueBySupplier'),
        'Purchase_Orders_Raw': ('PO_ID', 'Purchase_Orders', 'Agg_TotalSpendByCurrency'),
        'Purchase_Order_Details_Raw': ('PODetailID', 'Purchase_Order_Details', 'Agg_TotalQuantityByItem'),
        'Spend_Analysis_Raw': ('SpendID', 'Spend_Analysis', 'Agg_TotalSpendByCategory')
    }

    for raw_table, (id_col, clean_table, agg_table) in table_configs.items():
        try:
            print(f"\n📥 Processing `{raw_table}`...")
            df = pd.read_sql(f"SELECT * FROM {raw_table}", raw_engine)
            df = clean_dataframe(df)

            # === Table-specific cleaning & aggregation ===
            if raw_table == 'Supplier_Information_Raw':
                df['Country'] = df['Country'].str.title()
                agg = df.groupby('Country')['SupplierID'].nunique().reset_index(name='SupplierCount')

            elif raw_table == 'Supplier_Performance_Raw':
                df['QualityScore'] = pd.to_numeric(df['QualityScore'], errors='coerce')
                agg = df.groupby('SupplierID')['QualityScore'].mean().reset_index(name='AvgQualityScore')

            elif raw_table == 'Sourcing_Contracts_Raw':
                df['ContractValue'] = pd.to_numeric(df['ContractValue'], errors='coerce')
                agg = df.groupby('SupplierID')['ContractValue'].sum().reset_index(name='TotalContractValue')

            elif raw_table == 'Purchase_Orders_Raw':
                df['Currency'] = df['Currency'].str.upper()
                df['Total_Amount'] = pd.to_numeric(df['Total_Amount'], errors='coerce')
                agg = df.groupby('Currency')['Total_Amount'].sum().reset_index(name='TotalSpend')

            elif raw_table == 'Purchase_Order_Details_Raw':
                df['Quantity'] = pd.to_numeric(df['Quantity'], errors='coerce')
                agg = df.groupby('ItemID')['Quantity'].sum().reset_index(name='TotalQuantity')

            elif raw_table == 'Spend_Analysis_Raw':
                df['Category'] = df['Category'].str.title()
                df['Amount_Spend'] = pd.to_numeric(df['Amount_Spend'], errors='coerce')
                agg = df.groupby('Category')['Amount_Spend'].sum().reset_index(name='TotalSpend')

            # === Push cleaned and aggregated tables to clean DB ===
            df.to_sql(clean_table, clean_engine, if_exists='replace', index=False)
            print(f"✅ Cleaned data written to `{clean_table}` ({len(df)} rows)")

            agg.to_sql(agg_table, clean_engine, if_exists='replace', index=False)
            print(f"📊 Aggregated data written to `{agg_table}`")

        except Exception as e:
            print(f"❌ Error processing `{raw_table}`: {e}")

    print("\n✅ ETL completed for all tables.")

# === 4. Schedule ETL to run every hour ===
schedule.every(1).seconds.do(etl_clean_all)

# === 5. Immediate Run on Startup ===
etl_clean_all()

print("🔁 ETL is running. It will repeat every 1 hour. (Press Ctrl+C to stop)")
while True:
    schedule.run_pending()
    time.sleep(1)

# import relevant libraries
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import date, datetime
import os

def main():
    try:
        # --- Configuration ---
        CSV_FILE_PATH = 'F:/Khalid/ITI/00-Freelance and projects/Projects/Python/Sample - Superstore.csv'
        DATABASE_URL = 'postgresql+psycopg2://postgres:XILSxtaZsFJHqysaWsiaqrtHKzwPIXDT@trolley.proxy.rlwy.net:19456/railway'
        engine = create_engine(DATABASE_URL)

        PROCESS_NAME = 'daily_etl_job'

        # ✅ Check if ETL already ran today
        query = f"""
        SELECT COUNT(*) FROM etl_log
        WHERE process_name = '{PROCESS_NAME}' AND "run_date" = '{date.today()}'
        """
        already_ran = pd.read_sql(query, con=engine).iloc[0, 0]
        if already_ran:
            print("🚫 ETL already ran today. Skipping.")
            return

        # ✅ Read & clean CSV
        df = pd.read_csv(CSV_FILE_PATH, encoding='ISO-8859-1')
        df.columns = [col.strip().lower().replace(" ", "_") for col in df.columns]

        # ✅ Date key transformation
        df['order_date'] = pd.to_datetime(df['order_date'], errors='coerce')
        df['order_date_key'] = df['order_date'].dt.strftime('%Y%m%d').astype(int)
        df['ship_date'] = pd.to_datetime(df['ship_date'], errors='coerce')
        df['ship_date_key'] = df['ship_date'].dt.strftime('%Y%m%d').astype(int)

        # ✅ Date dimension
        date_range = pd.date_range(start='2000-01-01', end='2030-12-31')
        date_dim = pd.DataFrame({'date': date_range})
        date_dim['date_key'] = date_dim['date'].dt.strftime('%Y%m%d').astype(int)
        date_dim['day'] = date_dim['date'].dt.day
        date_dim['month'] = date_dim['date'].dt.month
        date_dim['month_name'] = date_dim['date'].dt.strftime('%B')
        date_dim['quarter'] = date_dim['date'].dt.quarter
        date_dim['year'] = date_dim['date'].dt.year
        date_dim['weekday'] = date_dim['date'].dt.weekday + 1
        date_dim['weekday_name'] = date_dim['date'].dt.strftime('%A')
        date_dim['is_weekend'] = date_dim['weekday'] >= 6
        date_dim['week_of_year'] = date_dim['date'].dt.isocalendar().week
        date_dim['is_holiday'] = False

        # ✅ Split dimension/fact
        customers_df = df[['customer_id', 'customer_name', 'segment', 'country','city', 'state','postal_code', 'region']].drop_duplicates()
        product_df = df[['product_id', 'product_name', 'sub-category', 'category']].drop_duplicates()
        orders_df = df[['row_id', 'order_id', 'order_date_key', 'ship_date_key', 'ship_mode', 'sales', 'quantity', 'discount', 'customer_id', 'product_id']].drop_duplicates()

        customers_df['surrogate_key'] = range(1, len(customers_df) + 1)
        customers_df = customers_df.drop_duplicates(subset=['customer_id'])

        product_df['product_sk'] = range(1, len(product_df) + 1)
        product_df = product_df.drop_duplicates(subset=['product_id'])

        orders_df = orders_df.merge(customers_df[['customer_id', 'surrogate_key']], on='customer_id', how='left')
        orders_df = orders_df.merge(product_df[['product_id', 'product_sk']], on='product_id', how='left')
        orders_df = orders_df.drop(columns=['customer_id', 'product_id'])

        # ✅ Export to PostgreSQL
        customers_df.to_sql("dim_customer", con=engine, if_exists='replace', index=False)
        product_df.to_sql("dim_product", con=engine, if_exists='replace', index=False)
        date_dim.to_sql("dim_date", engine, if_exists='replace', index=False)
        orders_df.to_sql("fact_orders", con=engine, if_exists='replace', index=False)

        print("✅ Data exported to PostgreSQL successfully.")

        # ✅ Log ETL success
        with engine.connect() as conn:
            conn.execute(
                text("""
                    INSERT INTO etl_log (process_name, run_date, status)
                    VALUES (:process_name, :run_date, :status)
                """),
                {"process_name": PROCESS_NAME, "run_date": date.today(), "status": "success"}
            )
            conn.commit()

    except Exception as e:
        print(f"❌ ETL failed: {e}")

if __name__ == "__main__":
    main()
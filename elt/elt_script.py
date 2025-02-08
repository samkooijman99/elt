import psycopg2
import time
from stocks import get_stock_data
from dates import get_dates
from exchange_rates import get_exchange_rates
from datetime import datetime, timedelta


END_DATE = datetime.now()
START_DATE = END_DATE - timedelta(365.25 * 20)

stock_query = """
    ("Date" DATE,
    "Open" FLOAT,
    "High" FLOAT,
    "Low" FLOAT,
    "Close" FLOAT,
    "Volume" BIGINT,
    "Dividends" FLOAT,
    "Stock_Splits" FLOAT,
    "Capital_Gains" Float,
    "Symbol" VARCHAR(20)
    ) """

date_query = """
    ("date" DATE
    )"""

exchange_rate_query = """

    ("Date" DATE,
    "usd_per_eur" NUMERIC(10,4),  -- Allows values up to 999999.9999
    "eur_per_usd" NUMERIC(10,4)   -- 10 total digits, 4 after decimal
    )"""

def wait_for_postgres(host, dbname, user, password, max_retries=30, delay_seconds=1):
    print(f"Waiting for {host} to be ready...")
    retries = 0
    while retries < max_retries:
        try:
            conn = psycopg2.connect(
                dbname=dbname,
                user=user,
                password=password,
                host=host,
                port=5432
            )
            conn.close()
            print(f"Successfully connected to {host}")
            return True
        except psycopg2.OperationalError as e:
            print(f"Failed to connect to {host}: {e}")
            retries += 1
            if retries < max_retries:
                print(f"Retrying in {delay_seconds} seconds... (Attempt {retries}/{max_retries})")
                time.sleep(delay_seconds)
    
    print(f"Failed to connect to {host} after {max_retries} attempts")
    return False

def create_schema_and_table(conn, cur, schema_name, table_name, table_query):
    try:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
        conn.commit()
        
        create_table_sql = f"""
        DROP TABLE IF EXISTS {schema_name}.{table_name};
        CREATE TABLE {schema_name}.{table_name} 
        {table_query}
        """
        cur.execute(create_table_sql)
        conn.commit()
        print(f"Schema and table {schema_name}.{table_name} created successfully")
        
    except Exception as e:
        print(f"Error creating schema and table: {str(e)}")
        raise

def write_data_to_db(df, conn, cur, schema_name, table_name):
    try:
        print("Preparing data for database insertion...")
        
        data_tuples = [tuple(x) for x in df.values]
        print(f"Prepared {len(data_tuples)} rows for insertion")
        
        columns = df.columns
        print(f"Using columns: {columns}")
        
        columns_str = ','.join([f'"{col}"' for col in columns])
        placeholders = ','.join(['%s'] * len(columns))
        insert_sql = f"""
        INSERT INTO {schema_name}.{table_name} 
        ({columns_str}) 
        VALUES ({placeholders})
        """
        print(f"Insert SQL: {insert_sql}")
        
        print("Starting batch insert...")
        cur.executemany(insert_sql, data_tuples)
        conn.commit()
        print(f"Successfully wrote {len(df)} rows to {schema_name}.{table_name}")
        
    except Exception as e:
        print(f"Error writing data to database: {str(e)}")
        print("Rolling back transaction...")
        conn.rollback()
        raise

def main():
    print("Starting ELT script...")

    destination_config = {
        'dbname': 'destination_db',
        'user': 'postgres',
        'password': 'secret',
        'host': 'destination_postgres'
    }

    # Wait for database to be ready
    print("Checking database connections...")
    if not wait_for_postgres(**destination_config):
        print("Could not connect to destination database")
        exit(1)

    try:
        # Connect to destination database
        print("Connecting to destination database...")
        dest_conn = psycopg2.connect(**destination_config)
        dest_cur = dest_conn.cursor()

        print("\n--- Starting stock data retrieval ---")
        stock_df = get_stock_data()
        date_df = get_dates(START_DATE, END_DATE)
        exchange_df = get_exchange_rates(START_DATE, END_DATE)
        print("\n--- Stock data retrieval completed ---")
        
        if stock_df is not None:
            print(f"Successfully retrieved stock data with shape: {stock_df.shape}")
            print("Sample of data columns:", stock_df.columns.tolist())
            
            schema_name = 'yfinance_data'
            table_name = 'stock_data_hist'
            create_schema_and_table(dest_conn, dest_cur, schema_name, table_name, stock_query)
            write_data_to_db(stock_df, dest_conn, dest_cur, schema_name, table_name)

        if date_df is not None:
            print(f"Successfully retrieved stock data with shape: {date_df.shape}")
            print("Sample of data columns:", date_df.columns.tolist())
            
            schema_name = 'general_dimensions'
            table_name = 'dim_date'
            create_schema_and_table(dest_conn, dest_cur, schema_name, table_name, date_query)
            write_data_to_db(date_df, dest_conn, dest_cur, schema_name, table_name)
                
        if exchange_df is not None:
            print(f"Successfully retrieved stock data with shape: {exchange_df.shape}")
            print("Sample of data columns:", exchange_df.columns.tolist())
            
            schema_name = 'general_dimensions'
            table_name = 'exchange_rates'
            create_schema_and_table(dest_conn, dest_cur, schema_name, table_name, exchange_rate_query)
            write_data_to_db(exchange_df, dest_conn, dest_cur, schema_name, table_name)
        
        print("Closing database connections...")
        dest_cur.close()
        dest_conn.close()
        
        print("Data copy completed successfully!")

    except Exception as e:
        print(f"Error during data copy: {str(e)}")
        exit(1)

if __name__ == "__main__":
    main()
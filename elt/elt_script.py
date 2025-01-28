import psycopg2
import time

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

def main():
    print("Starting ELT script...")

    source_config = {
        'dbname': 'source_db',
        'user': 'postgres',
        'password': 'secret',
        'host': 'source_postgres'
    }

    destination_config = {
        'dbname': 'destination_db',
        'user': 'postgres',
        'password': 'secret',
        'host': 'destination_postgres'
    }

    # Wait for databases to be ready
    print("Checking database connections...")
    if not wait_for_postgres(**source_config):
        print("Could not connect to source database")
        exit(1)
    if not wait_for_postgres(**destination_config):
        print("Could not connect to destination database")
        exit(1)

    try:
        # Connect to source database
        print("Connecting to source database...")
        source_conn = psycopg2.connect(
            dbname=source_config['dbname'],
            user=source_config['user'],
            password=source_config['password'],
            host=source_config['host']
        )
        source_cur = source_conn.cursor()

        # Connect to destination database
        print("Connecting to destination database...")
        dest_conn = psycopg2.connect(
            dbname=destination_config['dbname'],
            user=destination_config['user'],
            password=destination_config['password'],
            host=destination_config['host']
        )
        dest_cur = dest_conn.cursor()

        # Get list of tables from source database
        print("Getting list of tables...")
        source_cur.execute("""
            SELECT tablename 
            FROM pg_tables 
            WHERE schemaname = 'public'
        """)
        tables = source_cur.fetchall()

        for (table_name,) in tables:
            print(f"Processing table: {table_name}")
            
            # Get table structure
            source_cur.execute(f"""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = %s
                AND table_schema = 'public'
                ORDER BY ordinal_position
            """, (table_name,))
            columns = source_cur.fetchall()

            # Create table in destination
            create_table_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ("
            create_table_sql += ", ".join([f"{col[0]} {col[1]}" for col in columns])
            create_table_sql += ")"
            
            print(f"Creating table {table_name} in destination...")
            dest_cur.execute(create_table_sql)
            dest_conn.commit()

            # Copy data
            print(f"Copying data for table {table_name}...")
            source_cur.execute(f"SELECT * FROM {table_name}")
            rows = source_cur.fetchall()

            if rows:
                placeholders = ", ".join(["%s"] * len(columns))
                insert_sql = f"INSERT INTO {table_name} VALUES ({placeholders})"
                dest_cur.executemany(insert_sql, rows)
                dest_conn.commit()

            print(f"Copied {len(rows)} rows from table {table_name}")

        print("Closing database connections...")
        source_cur.close()
        dest_cur.close()
        source_conn.close()
        dest_conn.close()
        
        print("Data copy completed successfully!")

    except Exception as e:
        print(f"Error during data copy: {str(e)}")
        exit(1)

if __name__ == "__main__":
    main()
import pyodbc
import time
import datetime
def truncate_tables(database_path, tables_to_keep):
    # Connect to the Access database
    conn_str = (
        r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};'
        f'DBQ={database_path};'
    )
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    # Get list of tables in the database
    tables = cursor.tables(tableType='TABLE')
    table_names = [table.table_name for table in tables]

    # Truncate each table except the ones specified to keep
    for table_name in table_names:
        if table_name not in tables_to_keep:
            try:
                cursor.execute(f"DELETE * FROM [{table_name}] WHERE DATEVALUE([Date & Time]) < Date()-1")
                print(f"Truncated table: {table_name}")
                conn.commit()
            except Exception as e:
                print(f"Error truncating table {table_name}: {e}")
        else:
            print(f"Skipped truncating table: {table_name}")

    # Commit changes and close connection
    conn.close()



def insert_column_in_meter_data_tables(database_path): #Just used to add a new Field to a load of tables.
    # Connect to the Access database
    conn_str = (
        r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};'
        f'DBQ={database_path};'
    )
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    # Get list of tables in the database
    tables = cursor.tables(tableType='TABLE')
    table_names = [table.table_name for table in tables]

    # Iterate over each table
    for table_name in table_names:
        if "Meter Data" in table_name:
            try:
                # Construct the SQL to add a new column
                new_column_name = "kW"
                alter_table_sql = f"ALTER TABLE [{table_name}] ADD COLUMN {new_column_name} NUMBER"

                # Execute the SQL statement
                cursor.execute(alter_table_sql)
                print(f"Inserted column '{new_column_name}' in table: {table_name}")
                conn.commit()
            except Exception as e:
                print(f"Error modifying table {table_name}: {e}")

    # Close connection
    conn.close()



database_path = r"C:\Users\OMOPS\OneDrive - Narenco\Documents\AE API DB.accdb"

tables_to_keep = ["1)Sites", "2)Breakers", "3)Meters", "4)Inverters", "5)POA"]
truncate_tables(database_path, tables_to_keep)
#insert_column_in_meter_data_tables(database_path)

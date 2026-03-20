import pyodbc 

connection_string = (
    r'DRIVER={ODBC Driver 18 for SQL Server};'
    r'SERVER=localhost\SQLEXPRESS;'
    r'DATABASE=NARENCO_O&M_AE;'
    r'Trusted_Connection=yes;'
    r'Encrypt=no;'
)

conn = pyodbc.connect(connection_string)
cursor = conn.cursor()

# Define the columns and their data types
meter_columns = [
    ('id', 'INT IDENTITY(1,1) PRIMARY KEY'),
    ('Timestamp', 'DATETIME'),
    ('[Last Upload]', 'DATETIME'),
    ('[Volts A]', 'INT'),
    ('[Volts B]', 'INT'),
    ('[Volts C]', 'INT'),
    ('[Amps A]', 'INT'),
    ('[Amps B]', 'INT'),
    ('[Amps C]', 'INT'),
    ('Watts', 'INT'),
    ('HardwareID', 'INT')
]
breaker_columns = [
    ('id', 'INT IDENTITY(1,1) PRIMARY KEY'),
    ('Timestamp', 'DATETIME'),
    ('[Last Upload]', 'DATETIME'),
    ('Status', 'BIT'),
    ('HardwareID', 'INT')
]
inv_columns = [
    ('id', 'INT IDENTITY(1,1) PRIMARY KEY'),
    ('Timestamp', 'DATETIME'),
    ('[Last Upload]', 'DATETIME'),
    ('Watts', 'INT'),
    ('[dc V]', 'INT'),
    ('HardwareID', 'INT')
]
poa_columns = [
    ('id', 'INT IDENTITY(1,1) PRIMARY KEY'),
    ('Timestamp', 'DATETIME'),
    ('[Last Upload]', 'DATETIME'),
    ('[W/M²]', 'INT'),
    ('HardwareID', 'INT')
]

# Add the New sites here in the below format
sites = [('Williams', 40), ('Longleaf Pine', 40)]

try:
    # Loop through each table name and create the table
    for table_name, inv_num in sites:
        # Meter - Construct the SQL CREATE TABLE statement
        m_columns_sql = ', '.join([f"{name} {data_type}" for name, data_type in meter_columns])
        meter_table_sql = f"CREATE TABLE [{table_name} Meter Data] ({m_columns_sql});"
        cursor.execute(meter_table_sql)
        #POA
        p_columns_sql = ', '.join([f"{name} {data_type}" for name, data_type in poa_columns])
        poa_table_sql = f"CREATE TABLE [{table_name} POA Data] ({p_columns_sql});"
        cursor.execute(poa_table_sql)
        #Breaker, Only some get this one
        b_columns_sql = ', '.join([f"{name} {data_type}" for name, data_type in breaker_columns])
        breaker_table_sql = f"CREATE TABLE [{table_name} Breaker Data] ({b_columns_sql});"
        cursor.execute(breaker_table_sql)
        #INV NUM
        for inv in range(1, inv_num+1):
            inv_columns_sql = ', '.join([f"{name} {data_type}" for name, data_type in inv_columns])
            inv_table_sql = f"CREATE TABLE [{table_name} INV {inv} Data] ({inv_columns_sql});"
            cursor.execute(inv_table_sql)

    conn.commit()
except Exception as e:
    print(f"Error: {e}")
finally:
    cursor.close()
    conn.close()
    for site_data in sites:
        print(f"Tables created successfully for {site_data[0]}")
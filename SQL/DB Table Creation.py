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

master_List_Sites = {('Bishopville II', 36, 'bishopvilleII'), ('Bluebird', 24, 'bluebird'), ('Bulloch 1A', 24, 'bulloch1a'), ('Bulloch 1B', 24, 'bulloch1b'), ('Cardinal', 59, 'cardinal'), ('CDIA', 1, 'cdia'),
                     ('Cherry Blossom', 4, 'cherryblossom'), ('Cougar', 31, 'cougar'), ('Conetoe', 16, 'conetoe'), ('Duplin', 21, 'duplin'), ('Elk', 43, 'elk'), ('Freightliner', 18, 'freightliner'), ('Gray Fox', 40, 'grayfox'),
                      ('Harding', 24, 'harding'), ('Harrison', 43, 'harrison'), ('Hayes', 26, 'hayes'), ('Hickory', 2, 'hickory'), ('Hickson', 16, 'hickson'), ('Holly Swamp', 16, 'hollyswamp'),
                       ('Jefferson', 64, 'jefferson'), ("Longleaf Pine", 40, 'longleaf'),('Marshall', 16, 'marshall'), ('McLean', 40, 'mcLean'), ('Ogburn', 16, 'ogburn'), ('PG', 18, 'pg'), ('Richmond', 24, 'richmond'),
                        ('Shorthorn', 72, 'shorthorn'), ('Sunflower', 80, 'sunflower'), ('Tedder', 16, 'tedder'), ('Thunderhead', 16, 'thunderhead'), ('Upson', 24, 'upson'), 
                        ('Van Buren', 17, 'vanburen'), ('Warbler', 32, 'warbler'), ('Washington', 40, 'washington'), ('Wayne 1', 4, 'wayne1'), ('Wayne 2', 4, 'wayne2'), 
                        ('Wayne 3', 4, 'wayne3'), ('Wellons', 6, 'wellons'), ('Whitehall', 16, 'whitehall'), ('Whitetail', 80, 'whitetail'), ('Williams', 40, 'williams'), ('Violet', 2, 'violet')}

has_breaker = {'Bishopville II', 'Cardinal', 'Cherry Blossom', 'Elk', 'Gray Fox', 'Harding', 'Harrison', 'Hayes', 'Hickory', 'Hickson', 'Jefferson', 'Longleaf Pine', 'Marshall', 'McLean', 'Ogburn', 
               'Shorthorn', 'Sunflower', 'Tedder', 'Thunderhead', 'Warbler', 'Washington', 'Whitehall', 'Whitetail', 'Williams', 'Violet'}

 


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


try:
    
    # Loop through each table name and create the table
    for table_name, inv_num, var in master_List_Sites:
        # Meter - Construct the SQL CREATE TABLE statement
        m_columns_sql = ', '.join([f"{name} {data_type}" for name, data_type in meter_columns])
        meter_table_sql = f"CREATE TABLE [{table_name} Meter Data] ({m_columns_sql});"
        cursor.execute(meter_table_sql)
        #POA
        p_columns_sql = ', '.join([f"{name} {data_type}" for name, data_type in poa_columns])
        poa_table_sql = f"CREATE TABLE [{table_name} POA Data] ({p_columns_sql});"
        cursor.execute(poa_table_sql)
        #Breaker, Only some get this one
        if table_name in has_breaker and table_name != 'Violet':
            b_columns_sql = ', '.join([f"{name} {data_type}" for name, data_type in breaker_columns])
            breaker_table_sql = f"CREATE TABLE [{table_name} Breaker Data] ({b_columns_sql});"
            cursor.execute(breaker_table_sql)
        elif table_name == 'Violet':
            for r in range(1,3):
                b_columns_sql = ', '.join([f"{name} {data_type}" for name, data_type in breaker_columns])
                breaker_table_sql = f"CREATE TABLE [{table_name} Breaker Data {r}] ({b_columns_sql});"
                cursor.execute(breaker_table_sql)
        
        if table_name != "Duplin":
            #INV NUM
            for inv in range(1, inv_num+1):
                inv_columns_sql = ', '.join([f"{name} {data_type}" for name, data_type in inv_columns])
                inv_table_sql = f"CREATE TABLE [{table_name} INV {inv} Data] ({inv_columns_sql});"
                cursor.execute(inv_table_sql)
        else:
            #Duplin
            for inv in range(1, inv_num+1):
                if inv < 4:
                    inv_type = 'Central'
                    num = inv
                else:
                    inv_type = 'String'
                    num = inv - 3
                inv_columns_sql = ', '.join([f"{name} {data_type}" for name, data_type in inv_columns])
                inv_table_sql = f"CREATE TABLE [{table_name} {inv_type} INV {num} Data] ({inv_columns_sql});"
                cursor.execute(inv_table_sql)
        
    conn.commit()
except Exception as e:
    print(f"Error: {e}")
finally:
    cursor.close()
    conn.close()
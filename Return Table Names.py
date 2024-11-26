import pyodbc

# Your database connection string
dbconn_str = r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=C:\Users\JosephLang\OneDrive - Narenco\Documents\AE API DB.accdb;'

# Establish a connection to the database
dbconnection = pyodbc.connect(dbconn_str)

# Create a cursor
cursor = dbconnection.cursor()

# Get the table names
table_names = []
for row in cursor.tables():
    if row.table_type == 'TABLE' and 'Meter' in row.table_name:
        table_names.append(row.table_name)

# Close the cursor and connection
cursor.close()
dbconnection.close()

# Print the list of table names
print("Meter Table names:")
for table_name in table_names:
    print(table_name)

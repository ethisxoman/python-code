import pandas as pd
from connection import cursor

#executing SQL query
cursor.execute("SELECT * FROM ethisx_accounts.users")

output = cursor.fetchall()
for row in output:
    print(row)
import os
from dotenv import load_dotenv
import 
# Load environment variables from .env file
load_dotenv()

# Build SOURCE DB config
source_db = {
    "host": os.getenv("SOURCE_HOST"),
    "user": os.getenv("SOURCE_USER"),
    "password": os.getenv("SOURCE_PASSWORD"),
    "database": os.getenv("SOURCE_DATABASE"),  
    "port": int(os.getenv("SOURCE_PORT", 3306)),  # default 3306
}

# Build REPORTING DB config
reporting_db = {
    "host": os.getenv("REPORTING_HOST"),
    "user": os.getenv("REPORTING_USER"),
    "password": os.getenv("REPORTING_PASSWORD"),
    "database": os.getenv("REPORTING_DATABASE"),
    "port": int(os.getenv("REPORTING_PORT", 3306)),
}




# #connecting to database
# conn = mysql.connector.connect(
#     host=os.getenv("host"),         
#     user=os.getenv("user"),              
#     password=os.getenv("password"),              
# )

# #creating cursor object to execute queries
# cursor = conn.cursor()

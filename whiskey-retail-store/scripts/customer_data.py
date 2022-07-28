from connection import sql_connection
import datetime
import pandas as pd
from tqdm import tqdm as tq


with sql_connection() as conn:
    cursor = conn.cursor()
    
    
    print("Creating data warehouse ...")
    
    print("Checking for existing table")
    #creating the warehouse table
    query = '''
    
    DROP TABLE IF EXISTS dwh_customers;
    
    
    '''
    
    cursor.execute(query)
    
    query= '''
    
    CREATE TABLE dwh_customers AS
        
        SELECT 
        c1.customer_id,
        c1.first_name,
        c1.last_name,
        c1.full_name,
        c2.country_code
        
        FROM whiskey_retail_shop.customers c1
        JOIN whiskey_retail_shop.countries c2
        USING (country_id)
        ORDER BY customer_id; 
        
        
        
        
        
   
    
    '''
    
    cursor.execute(query)
    print("Created table dwh_customers")
    
    query = '''
    
    ALTER TABLE dwh_customers
    MODIFY COLUMN customer_id INTEGER NOT NULL PRIMARY KEY;
    
    '''
    
    cursor.execute(query)
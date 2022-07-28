from connection import sql_connection



with sql_connection() as conn:
    cursor = conn.cursor()
    
    
    print("Creating data warehouse ...")
    
    print("Checking for existing table")
    #creating the warehouse table
    query = '''
    
    DROP TABLE IF EXISTS dwh_products;
    
    
    '''
    
    cursor.execute(query)
    
    query= '''
    
    CREATE TABLE dwh_products AS
        
        SELECT *
        
        
        FROM whiskey_retail_shop.products
        
        ORDER BY product_id; 
        
        
        
        
        
   
    
    '''
    
    cursor.execute(query)
    print("Created table dwh_products")
    
    query = '''
    
    ALTER TABLE dwh_products
    MODIFY COLUMN product_id INTEGER NOT NULL PRIMARY KEY;
    
    '''
    
    cursor.execute(query)
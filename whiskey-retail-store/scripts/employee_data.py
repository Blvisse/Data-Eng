from connection import sql_connection



#we create a data warehouse
with sql_connection() as conn:
    cursor = conn.cursor()
    
    print("Creating data warehouse ...")
    
    print("Checking for existing table")
    #creating the warehouse table
    query = '''
    
    DROP TABLE IF EXISTS dwh_employees;
    
    
    '''
    
    cursor.execute(query)
    
    query= '''
    
    CREATE TABLE dwh_employees AS
        
    SELECT 
        c1.employee_id,
        c1.first_name,
        c1.last_name,
        c1.full_name,
        c2.department
        
        FROM whiskey_retail_shop.employees AS c1
        JOIN whiskey_retail_shop.departments AS c2
        USING (department_id)
        ORDER BY employee_id; 
        
        
        
        
        
   
    
    '''
    
    cursor.execute(query)
    print("Created table dwh_employees")
    
    query = '''
    
    ALTER TABLE dwh_employees
    MODIFY COLUMN employee_id INTEGER NOT NULL PRIMARY KEY;
    
    '''
    
    cursor.execute(query)
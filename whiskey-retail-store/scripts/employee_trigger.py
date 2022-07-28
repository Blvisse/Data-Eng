#trigger to add new customer to the database and the data warehouse
from connection import sql_connection


with sql_connection() as conn:
    cursor = conn.cursor()
    
    print("Creating employee trigger...")
    
    print("Changing schemas...")
    query= '''
    
    USE whiskey_retail_shop;
    
    '''
    
    cursor.execute(query)
    print("Successfully changed schema")
    
    
    query= '''
    
    DROP TRIGGER IF EXISTS insert_employee;
    
    
    '''
    cursor.execute(query)
    query = '''
    
    CREATE TRIGGER insert_employee
    AFTER INSERT ON whiskey_retail_shop.employees
    FOR EACH ROW
    
    INSERT INTO dwh_whiskey.dwh_employees
    SELECT 
    
    e.employee_id,
    e.first_name,
    e.last_name,
    e.full_name,
    d.department
    
    FROM employees AS e
    JOIN departments AS d
    USING (department_id)
    WHERE e.employee_id = new.employee_id;
    
    
    
    '''
    cursor.execute(query)
    print("Successfully created trigger")
    
     
    
   


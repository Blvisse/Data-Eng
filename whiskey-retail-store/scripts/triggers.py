#trigger to add new customer to the database and the data warehouse
from connection import sql_connection


with sql_connection() as conn:
    cursor = conn.cursor()
    
    print("Creating customer trigger...")
    
    print("Changing schemas...")
    query= '''
    
    USE whiskey_retail_shop;
    
    '''
    
    cursor.execute(query)
    print("Successfully changed schema")
    
    
    query= '''
    
    DROP TRIGGER IF EXISTS insert_customer;
    
    
    '''
    cursor.execute(query)
    query = '''
    
    CREATE TRIGGER insert_customer
    AFTER INSERT ON whiskey_retail_shop.customers
    FOR EACH ROW
    
    INSERT INTO dwh_whiskey.dwh_customers
    SELECT 
    
    c1.customer_id,
    c1.first_name,
    c1.last_name,
    c1.full_name,
    c2.country_code
    
    FROM customers AS c1
    JOIN countries AS c2
    USING (country_id)
    WHERE c1.customer_id = new.customer_id;
    
    
    
    '''
    cursor.execute(query)
    print("Successfully created trigger")
    
    print("Testing query...")    
    
    query= '''
    insert into customers(customer_id, first_name, last_name  ,full_name  ,email  ,street  ,four_digits  ,country_id  ,credit_provider_id)
values(99887, 'Legolas', 'Greenleaf', 'Legolas Greenleaf', 'Legolas@gmail.com', 'Mirkwood', 9999, 5, 6);
    
    
    
    '''
    
    cursor.execute(query)
    print("Test done ...") 


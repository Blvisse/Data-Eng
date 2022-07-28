from connection import sql_connection



with sql_connection() as conn:
    cursor = conn.cursor()
    
    
    print("Accessing data warehouse ...")
    
    print("Checking for existing table")
    #creating the warehouse table
    query = '''
    
    DROP TABLE IF EXISTS dwh_fact;
    
    
    '''
    
    cursor.execute(query)
    
    query= '''
    
    CREATE TABLE dwh_fact AS
        
        SELECT 
        c1.customer_id,
        e1.employee_id,
        p2.product_id,
        p2.Alcohol_Percentage,
        p2.Alcohol_Unit,
        p2.Alcohol_Price,
        p2.Product_Name,
        c1.four_digits,
        c2.Country,
        c3.credit_provider,
        d.Date_key,
        p1.date

        FROM whiskey_retail_shop.payments AS p1
        JOIN
        whiskey_retail_shop.customers AS c1 
        USING (customer_id)
        JOIN
        whiskey_retail_shop.employees AS e1 
        USING (employee_id)
        JOIN
        whiskey_retail_shop.products AS p2 
        USING (product_id)
        JOIN
        whiskey_retail_shop.countries AS c2
        USING (country_id)
        JOIN
        whiskey_retail_shop.customer_cc AS c3
        USING (credit_provider_id)
        JOIN
        dwh_date AS d ON p1.date=d.Dates
        
        ORDER BY d.Dates;
        
        
        
        
        
        
        
   
    
    '''
    
    cursor.execute(query)
    print("Created table dwh_facts")
    
    print("Enhancing Tables ...")
    query = '''
    
    ALTER TABLE dwh_fact
    
    ADD foreign key (customer_id) REFERENCES dwh_customers(customer_id);
    
    
    
    '''
    
    cursor.execute(query)
    
    query = '''
    
    ALTER TABLE dwh_fact
    ADD foreign key (employee_id) REFERENCES dwh_employees(employee_id);
    
    '''
    
    cursor.execute(query)
    
    query = '''
    
    ALTER TABLE dwh_fact
    ADD foreign key (product_id) REFERENCES dwh_products(product_id);
    
    '''
    
    cursor.execute(query)
    
    query = '''
    
    ALTER TABLE dwh_fact
    ADD foreign key (Date_key) REFERENCES dwh_date(Date_key);
    
    '''
    
    cursor.execute(query)
    
    
    
    
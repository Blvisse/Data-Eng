#trigger to add new customer to the database and the data warehouse
from connection import sql_connection


with sql_connection() as conn:
    cursor = conn.cursor()
    
    print("Creating payment trigger...")
    
    print("Changing schemas...")
    query= '''
    
    USE whiskey_retail_shop;
    
    '''
    
    cursor.execute(query)
    print("Successfully changed schema")
    
    
    query= '''
    
    DROP TRIGGER IF EXISTS insert_payment;
    
    
    '''
    cursor.execute(query)
    query = '''
    
    CREATE TRIGGER insert_payment
    AFTER INSERT ON whiskey_retail_shop.payments
    FOR EACH ROW
    
    INSERT INTO dwh_whiskey.dwh_fact
    SELECT 
        c.customer_id,
        e.employee_id,
        pr.product_id,
        pr.Alcohol_Unit,
        pr.Alcohol_Percentage,
        pr.Alcohol_Price,
        pr.Product_Name,
        c.four_digits,
        co.Country,
        cc.credit_provider,
        d.Date_key,
        d.Dates
    
    FROM payments as p
    JOIN customers as c
    ON p.customer_id = c.customer_id
    JOIN countries as co
    ON c.country_id = co.country_id
    JOIN customer_cc AS cc
    ON c.credit_provider_id = cc.credit_provider_id
    JOIN employees AS e
    ON p.employee_id = e.employee_id
    JOIN products AS pr
    ON p.product_id = pr.product_id
    JOIN dwh_whiskey.dwh_date AS d
    ON d.Dates = p.date
    WHERE p.payment_id = new.payment_id;
    
    
    
    '''
    cursor.execute(query)
    print("Successfully created trigger")
    
     
    
   


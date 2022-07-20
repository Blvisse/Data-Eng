try:

    import pymysql
    import pandas as pd
    import contextlib
    print("Successfully imported libraries")
    
except ImportError as ie:
    
    print("Error importing module: {}".format(ie))
    exit(1)
    


#create a context manager to handle our sql connection
@contextlib.contextmanager
def sql_connection():
    '''
    Context manager to handle sql connection
    Args: 
        db: database name
    Yield:
        conn: sql connection
    '''
    try:
        conn = pymysql.connect(host='localhost',
                               user='root',
                               password='root',
                               port=3306,
                               db='whiskey_retail_shop',
                               
                               charset='utf8mb4'
                               )
        print("Successfully connected to database")
        # cursor = conn.cursor()
        yield conn
    except pymysql.MySQLError as e:
        print("Error connecting to database: {}".format(e))
        exit(1)
    finally:
        conn.close()
        print("Connection closed")
        

#use the context library to execute sql queries
with sql_connection() as conn:
    cursor =conn.cursor()
    
    
    print("Cursor object {}".format(cursor))

    print("Checking if database exists")
    cursor.execute('''
                DROP SCHEMA IF EXISTS whiskey_retail_shop;
                
                ''')


    cursor.execute('''
                    
                    CREATE SCHEMA whiskey_retail_shop;
                    
                    ''')

    cursor.execute('''
                
                USE whiskey_retail_shop;
                
                ''')

    print("Created and using schema")

    #Create the table in the database
    print("Creating countries table")
    cursor.execute('''
                
                DROP TABLE IF EXISTS countries;
                
                ''')

    print("Creating countries table")
    cursor.execute('''
                
                CREATE TABLE countries (
                    Country VARCHAR(100) NOT NULL,
                    Country_Code VARCHAR(100) NOT NULL,
                    Country_id INT PRIMARY KEY
                    
                    
                );
                
                
                ''')
    print("Created countries table")
    
    print("Creating customers table")
    cursor.execute('''
                
                DROP TABLE IF EXISTS customer_cc;
                
                
                ''')
    cursor.execute('''
                
                CREATE TABLE customer_cc (
                    
                    credit_provider VARCHAR(100) NOT NULL,
                    credit_provider_id INT PRIMARY KEY
                    
                    );
                
                ''')
    print("Created customers table")
    
    print("Creating Products table")

    cursor.execute('''
                
                DROP TABLE IF EXISTS products;
                
                
                ''')

    cursor.execute('''
                
                CREATE TABLE products (
                    
                    Product_Name VarChar(100) NOT NULL,
                    Alcohol_Price FLOAT NOT NULL,
                    Alcohol_Unit FLOAT NOT NULL,
                    Alcohol_Percentage FLOAT NOT NULL,
                    Alcohol_Percentage_cl FLOAT NOT NULL,
                    Product_id INT NOT NULL PRIMARY KEY
                    
                )
                
                
                ''')
    
    print("Created Products table")
    cursor.execute('''
                
        DROP TABLE IF EXISTS departments;

                ''')

    cursor.execute('''
                
            CREATE TABLE departments (
            department_id INT PRIMARY KEY,
            department VARCHAR(100) NOT NULL
                );
            
        ''')


    cursor.execute('''
    DROP TABLE IF EXISTS customers;
    ''')

    cursor.execute('''
        CREATE TABLE customers (
        customer_id INT PRIMARY KEY NOT NULL,
        first_name VARCHAR(100) NOT NULL,
        last_name VARCHAR(100) NOT NULL,
        full_name VARCHAR(100) NOT NULL,
        email VARCHAR(100) NOT NULL,
        street VARCHAR(100) NOT NULL,
        four_digits INT NOT NULL,
        country_id INT NOT NULL,
        credit_provider_id INT NOT NULL,
        
        FOREIGN KEY (country_id) REFERENCES countries (country_id),
        FOREIGN KEY (credit_provider_id) REFERENCES customer_cc (credit_provider_id)
    );
    ''')

    cursor.execute('''
                
                DROP TABLE IF EXISTS employees;
                
                ''')

    cursor.execute('''
                
                CREATE TABLE employees (
                employee_id INT PRIMARY KEY NOT NULL,
                first_name VARCHAR(100) NOT NULL,
                last_name VARCHAR(100) NOT NULL,
                full_name VARCHAR(100) NOT NULL,
                email VARCHAR(100) NOT NULL,
                city VARCHAR(100) NOT NULL,
                department_id INT NOT NULL,
                
                FOREIGN KEY (department_id) REFERENCES departments(department_id)
                    
                );
                
                ''')

    cursor.execute('''
                
                    DROP TABLE IF EXISTS payments;
                
                ''')

    cursor.execute('''
                
                CREATE TABLE payments (
                payment_id INT NOT NULL PRIMARY KEY,
                date DATE NOT NULL,
                customer_id INT NOT NULL,
                employee_id INT NOT NULL,
                product_id INT NOT NULL,
                price FLOAT NOT NULL
                );
    ''')



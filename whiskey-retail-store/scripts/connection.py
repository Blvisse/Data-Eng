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
                               db='dwh_whiskey',
                               
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
        
        

from connection import sql_connection
import datetime
import pandas as pd
from tqdm import tqdm as tq


#we create a data warehouse
with sql_connection() as conn:
    cursor = conn.cursor()
    
    print("Creating data warehouse ...")
    
    try:
    
        print("Checking for existing schema ...")
        query= '''
        
        DROP SCHEMA IF EXISTS dwh_whiskey;
        
        
        
        '''
        
        cursor.execute(query)
        
        print("Creating new schema ... ")
        query = '''
        
        CREATE SCHEMA dwh_whiskey;
        
        '''
        
        cursor.execute(query)
    
        print("Created data warehouse")
        
        query = '''
        
        USE whiskey_retail_shop;
        
        '''
        conn.commit()
    
    except Exception as e:
        print("Error creating data warehouse: {}".format(e))
        exit(1)
        
    
#we generate date dat to aid in dimensionality
with sql_connection() as conn:
    cursor = conn.cursor()
    
    try:
        print("Opened new connection ...")
        
        #get the latest date from our database
        
        query = '''
        
        SELECT min(date) AS first_transaction
        FROM whiskey_retail_shop. payments       
        
        '''
        cursor.execute(query)
        result=cursor.fetchall()
        print(result)
    
    
    except Exception as e:
        print("Error creating data warehouse: {}".format(e))
        exit(1)
    
    #create dates
    
    start_date=pd.to_datetime('1991-01-01').date()
    end_date=pd.to_datetime('2100-12-31').date()
    
    #Generate a list of all dates from the above range
    dates=pd.date_range(start_date, end_date)
    dates_df=pd.DataFrame(dates, columns=['Date'])
    
    #generate our date key column
    # Date Key Column
    dates_df['Date_key'] = 10000 * dates_df.Date.dt.year + 100 * dates_df.Date.dt.month + dates_df.Date.dt.day

    # Day_name Column
    dates_df['Day_name'] = dates.day_name()

    # Month Column
    month_dict = {1:'Jan',2:'Feb',3:'Mar',4:'Apr',
                5:'May',6:'Jun',7:'Jul',8:'Aug',9:'Sep',10:'Oct',
                11:'Nov',12:'Dec'}
    dates_df['Month'] = dates_df.Date.dt.month
    dates_df.Month.replace(month_dict,inplace=True)

    # Year Column
    dates_df['Year'] = dates_df.Date.dt.year
    
    print(len(dates_df.Date_key.unique()))
    print(len(dates_df))

    # Confirm that the Date_key column is unique
    assert len(dates_df.Date_key.unique()) == len(dates_df.Date)
    
    print(dates_df)
    
    #create an empty table
    try:
        
        print("Checking for existing tables...")
        query = '''
        
        DROP TABLE IF EXISTS dwh_date;
        
        
        '''
        
        cursor.execute(query)
        
        
        print("Creating new tables ...")
        query = '''
        
        CREATE TABLE dwh_date (
            Dates DATE NOT NULL,
            Date_key INT NOT NULL PRIMARY KEY,
           
            Day_name VARCHAR(100) NOT NULL,
            Month_name VARCHAR(100) NOT NULL,
            Year_name VARCHAR(100) NOT NULL
            
            );
            
        '''
        
        cursor.execute(query)
        print("Created dwh_date")
    
    except Exception as e:
        print("Error creating data warehouse: {}".format(e))
        exit(1)
    

# Convert into a list of arrays
records=dates_df.to_records(index=False)
result = tuple(records)
    
with sql_connection() as conn:
    cursor = conn.cursor()
    try:
        
        print("Inserting data into dwh_date ...")
        for index in tq(range(0,len(result)),desc="Inserting data"):
            query = '''
            
            INSERT INTO dwh_date (Dates,Date_key, Day_name, Month_name, Year_name)
            VALUES {}
            
            '''.format(result[index])
            cursor.execute(query)
            conn.commit()
        print("Inserted data into dwh_date")
    except Exception as e:
        print("Error creating data warehouse: {}".format(e))
        exit(1)
        

    
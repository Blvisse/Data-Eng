try:

    # from scrape_data import scrape_whisky
    import pandas as pd
    import numpy as np
    import names
    from faker import Faker
    import pandasql as ps
    import random
    import time
    from datetime import datetime
    print("Successfully imported modules")
    
except ImportError as e:
    
    print("Error importing module: {}".format(e))
    exit(1)


#create a faker object
faker = Faker()

# product_data=scrape_whisky()

# #export to csv file
# product_data.to_csv('../data/whisky_data.csv',index=False)

#sql function query
def sql(query):
    return ps.sqldf(query)

data=pd.read_csv('../data/whisky_data.csv')

def clean_cols(data: pd.DataFrame):
    
    '''
    This function takes  in the whiskey dataframe and cleans the columns by striping off strings.
    
    Args:
        data: dataframe of whiskey data
    
    Returns:
        data: cleaned dataframe
    
    '''
    
    data['Product_price']=data['Product_price'].apply(lambda x: x.replace('£','') )
    data['Product_price']=data['Product_price'].apply(lambda x: x.replace(',','') ).astype(float)
    data['Product_unit']=data['Product_unit'].apply(lambda x : x.strip("()").strip(" per litre").strip(" per 10c").replace('£',''))
    data['Product_unit']=data['Product_unit'].apply(lambda x: x.replace(',','') ).astype(float)
    data['product_percentage']=data['product_percentage'].apply(lambda x : x.strip("%")).astype(float)
    data['product_percentage_cl']=data['product_percentage_cl'].apply(lambda x : x.strip("cl")).astype(float)
    
    
    return data
    
    

#view data
data=clean_cols(data)

try:
    #we generate a random id to act as primary key

    print("Generating primary key/ Unique Ids")
    product_id=np.random.default_rng().choice(len(data),len(data), replace=False)

    #confirm that the len of product id and product data
    print("Sanity check ongoing")
    assert len(set(product_id)) == len(data['Product_name'])

    #ensure that the ids are unique
    assert len(pd.Series(product_id).unique()) == len(data['Product_name'])

    print("Sanity check passed")

    #if that works now we merge
    data['Product_id']=product_id
    
    print(data)
    
except AssertionError as e:
    print("Error with sanity check: " + str(e))
    exit(1)
    
except Exception as e:
    print("Error with id generation " + str(e))
    exit(1)

#we generate random data for Employees

employee_id=np.random.default_rng().choice(4000,100, replace=False)
try:
#ensure the ids are unique
    print("Carrying out sanity checks")
    assert len(set(employee_id))==100
    assert len(pd.Series(employee_id).unique())==len(employee_id)
    print("Done..")
except AssertionError as e:
    print("Error with sanity check: " + str(e))
    exit(1)
    
# Generating 100 Employee Data
employee_first_name = []
employee_last_name = []
employee_full_name = []
employee_email = []
employee_city = []
departments = ['Sales', 'Finance', 'Marketing', 'BI']
employee_department = []

# iterate through the employees and generate random data
print("Generating employee data ...")
for i in range(len(employee_id)):
    employee_first_name.append(names.get_first_name())
    employee_last_name.append(names.get_last_name())
    employee_full_name.append(employee_first_name[i] + ' ' + employee_last_name[i])
    employee_email.append(employee_first_name[i] + employee_last_name[i][0].lower() + '@gmail.com')
    employee_city.append(faker.city())
    employee_department.append(np.random.choice(departments, 1)[0])
    
print("Done..")
    
# Generating a dataframe of employee data
print("Compiling dataframe ...")
employee_df = pd.DataFrame(employee_id, columns = ['employee_id'])
employee_df['first_name'] = employee_first_name
employee_df['last_name'] = employee_last_name
employee_df['full_name'] = employee_full_name
employee_df['email'] = employee_email
employee_df['city'] = employee_city
employee_df['department'] = employee_department

print(employee_df.head())



#generate Customer data

customer_id=np.random.default_rng().choice(99999,1000, replace=False)

#ensure the ids are unique
#carry out sanity check
try:  
    
    print("Carrying out sanity checks")  
    assert len(set(customer_id))==1000
    assert len(pd.Series(customer_id).unique())==len(customer_id)
    
    print("Done..")
except AssertionError as e:
    print("Error with sanity check: " + str(e))
    exit(1)


# Generating 1000 Customers Data
customer_first_name = []
customer_last_name = []
customer_full_name = []
customer_email = []
customer_last_four_digits = []
customer_country = []
customer_country_code = []
customer_street = []
customer_credit_card_company = []


# iterate through the customers and generate random data
print("Generating data ...")
for i in range(len(customer_id)): 
    customer_first_name.append(names.get_first_name())
    customer_last_name.append(names.get_last_name())
    customer_full_name.append(customer_first_name[i] + ' ' + customer_last_name[i])
    customer_email.append(customer_first_name[i] + customer_last_name[i][0].lower() + '@gmail.com')
    customer_last_four_digits.append(np.random.randint(low = 1000, high = 9999, size = 1)[0])
    customer_country.append(faker.country())
    customer_country_code.append(customer_country[i][0:3].upper())
    customer_street.append(faker.street_address())
    customer_credit_card_company.append(faker.credit_card_provider())
    
print("Done ...")
# Create a customer dataframe

print("Compiling dataframe ...")
customer_df = pd.DataFrame(customer_id, columns = ['customer_id'])
customer_df['first_name'] = customer_first_name
customer_df['last_name'] = customer_last_name
customer_df['full_name'] = customer_full_name
customer_df['email'] = customer_email
customer_df['country'] = customer_country
customer_df['country_code'] = customer_country_code
customer_df['street'] = customer_street
customer_df['credit_provider'] = customer_credit_card_company
customer_df['four_digits'] = customer_last_four_digits    



print(customer_df.head())

#Generating payments data
print("Generating payments data ...")

date_range=pd.date_range(start= "1991-01-01", end= "2021-12-31", freq="D",)

print("Generating ids ... ")
#Generating Unique payment id's
payment_id=np.random.default_rng().choice(999999,len(date_range),replace=False)

#Carry out sanity checks to ensure length and unique

try:
    
    print("Carrying out sanity check ...")
    assert len(set(payment_id))==len(date_range)
    assert len(pd.Series(payment_id).unique())==len(payment_id)
    
    print("Done..")
except AssertionError as e:
    print("Error with sanity check: " + str(e))
    exit(1)
    
#Generating payment data
customer_id_payments = []
employee_id_payments = []
product_id_payments = []
dates = []


# iterate through the payments and generate random data
print("Generating payment data ...")
for i in range(len(payment_id)):
    dates.append(datetime.strftime(random.choice(date_range), format='%Y-%m-%d'))
    customer_id_payments.append(random.choice(customer_id))
    employee_id_payments.append(random.choice(employee_id))
    product_id_payments.append(random.choice(product_id))


# Create a payments dataframe
print("Compiling dataframe ...")
payment_df = pd.DataFrame(payment_id, columns = ['payment_id'])
payment_df['date'] = sorted(dates)
payment_df['customer_id'] = customer_id_payments
payment_df['employee_id'] = employee_id_payments
payment_df['product_id'] = product_id_payments


# Create query ti add alcohol_price column to the table

query= '''

SELECT p1.*,d1.Product_price
FROM payment_df p1
inner join data d1
USING (product_id)
 
'''


payment_data=sql(query)

print(payment_data.head())


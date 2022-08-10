'''
This script carries out unit testing on data fetched from the datawarehouse.

'''
import pandas as pd
import unittest


class DataTest(unittest.TestCase):
    data=r'C:\Users\blais\Desktop\Masters\ML Track\Data-Eng\whiskey-retail-store\data\dwh_fact.csv'
    data=pd.read_csv(data)
    col='Alcohol_Price'
    
    

    
    def test_prices(self):
        
        
        
        
        # for col in date_cols:
            
        self.assertEqual(data[col].dtype, 'float64', "Failed date test for column: {}".format(col))
            
    # def test_dates(self):
    #         '''
    #     This function tests the dates in the data warehouse.
    #     '''
    #     for col in datecols:
    #         assert data[col].dtype == 'datetime64[ns]', "Failed date test for column: {}".format(col)    
            
        # try:
        #     test_dates()
        #     print("Tests Completed Successfully ...")
        # except Exception as e:
        #     print ("Error occurred while running tests: {}".format(e))


# def test_dates(data: pd.DataFrame,datecols: list):
#     '''
#     This function tests the dates in the data warehouse.
#     '''
#     for col in datecols:
#         assert data[col].dtype == 'datetime64[ns]', "Failed date test for column: {}".format(col)
        
        
        

if __name__ == '__main__':
    data='../data/whisky_data.csv'
    date_cols=['Product_name']    
    
    unittest.main()
    # try:
    #     test_dates()
    #     print("Tests Completed Successfully ...")
    # except Exception as e:
    #     print ("Error occurred while running tests: {}".format(e))
    
    
'''
This script carries out unit testing on data fetched from the datawarehouse.

'''
import pandas as pd
import unittest


class DataTest(unittest.TestCase):
    
    
    

    
    def test_dates(self):
        data='../data/whisky_data.csv'
        data=pd.read_csv(data)
        col='Product_name'
        
        
        
        # for col in date_cols:
            
        self.assertEqual(data[col].dtype, 'datetime64[ns]', "Failed date test for column:")
            
        
        
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
    
    
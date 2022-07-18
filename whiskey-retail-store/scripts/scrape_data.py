'''
This function scrapes the data from the website and stores it in a csv file.


'''
from re import A
import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup


#define method for scraping the data

def scrape_html(base_url,page):
    ''''
    This function takes in a base url and the page number and scrapes the website for the html source code 
    and returns it in form of a soup object
    
    Args:
        base_url: string: Base url of the website to be scraped
        page: int: page number of the website to be scraped
        
    Returns:
        soup: BeautifulSoup object
    
    '''
    url = base_url + str(page)
    
    
    req=requests.get(url)
    
    # print(req)
    soup = BeautifulSoup(req.content,'lxml')
    
    # print(soup)
    
    return soup
    
# we define a function that takes the BeautifulSoup object and get specific data
# we want to gather the name,alcohol amount, alcohol percent, and price of the drinks

def get_product_content(soup):
    ''' 
    Extract the card content from the soup object
    
    Args:
        soup: BeautifulSoup object
        
    Returns:
        product_content: list of html tags containing the name, alcohol amount, alcohol percent
    
    '''
    product_content = soup.find_all('div',class_='product-card__content')
    
    return product_content

def get_product_data(soup):
    
    ''' 
    Extract the product data from the soup object
    
    Args:
        soup: BeautifulSoup object
        
    Returns:
        product_data: list of html tags containing the price and amount per liter
    
    '''
    product_data = soup.find_all('div',class_='product-card__data')
    # print(product_data)
    
    return product_data


# soup=scrape_html('https://www.thewhiskyexchange.com/c/40/single-malt-scotch-whisky',1)
# get_product_data(soup)
#we create a function to extract the data from the lists


def clean_product_data(product_html_tags):
    '''
    This function takes in a list of html tags and returns a list of strings containing the data
    
    Args:
        product_html_tags: list of html tags
        
    Returns:
        product_data: list of strings containing the data
    
    '''
    product_price = []
    product_unit=[]
    for index,tag in enumerate(product_html_tags):
        beverage_price=tag.find_all('p')[0]
        beverage_unit=tag.find_all('p')[1]
        # print(beverage_name)
        # print(beverage_name.get_text())
        beverage_price=beverage_price.get_text().strip()
        beverage_unit=beverage_unit.get_text().strip()
        
        
        product_price.append(beverage_price)
        product_unit.append(beverage_unit)
        
    return product_price,product_unit

def clean_product_name(product_html_tags):
    '''
    Extract product name
    
    Args: 
        product_html_tags: list of html tags
        
    Returns:
        product_name: list of strings containing the product name
    
    '''
    product_name = []
    product_percentage=[]
    product_percentage_cl=[]
    for index,tag in enumerate(product_html_tags):
        beverage_name=tag.find_all('p')[0]
        beverage_percentage=tag.find_all('p')[1]
        # print(beverage_name)
        # print(beverage_percentage)
        # print(beverage_name.get_text())
        beverage_name=beverage_name.get_text().strip()
        beverage_percentage_cl=beverage_percentage.get_text().strip().split('/')[0].strip()
        beverage_percentage=beverage_percentage.get_text().strip().split('/')[1].strip()
        
        
        
        
        
        product_name.append(beverage_name)
        product_percentage.append(beverage_percentage)
        product_percentage_cl.append(beverage_percentage_cl)
        
    return product_name,product_percentage,product_percentage_cl
    
def create_df(product_data: list,product_name_list: list):
    product_price=product_data[0]
    product_unit=product_data[1]
    product_name=product_name_list[0]
    product_percentage=product_name_list[1] 
    product_percentage_cl=product_name_list[2]
    
    data=pd.DataFrame(product_name,columns=['Product_name'])
    data['Product_price']=product_price
    data['Product_unit']=product_unit
    data['product_percentage']=product_percentage
    data['product_percentage_cl']=product_percentage_cl
    
    return data   
def update_dataset(data,new_data):
    data=data.append(new_data,ignore_index=True,verify_integrity = True)
    return data 

#scrap all the webpages

def get_links(url='https://www.thewhiskyexchange.com'):
    url=url
    req=requests.get(url)
    soup=BeautifulSoup(req.content,'lxml')
    # print(soup)
    
    a_tags=soup.find_all('a',class_='subnav__link')
    # print(a_tags)
    
    link_list=[]
    for link in a_tags:
        link_list.append(link.get('href'))
        
    relevant_links=[]
    
    for link in link_list:
        if link is not None and '/c/' in link and 'whisky' in link and '?' not in link:
            relevant_links.append(link)
            
    # print(link_list)
    return relevant_links
def scrape_whisky(url='https://www.thewhiskyexchange.com', number_of_pages=5):
    df=pd.DataFrame()
    #create scrapper object
    generated_links=get_links()
    
    for link in generated_links:
        try:
            for page in range (0,number_of_pages):
                soup=scrape_html(base_url=url+link+'?pg=',page=page+1)
                product_content=get_product_content(soup)
                product_data=get_product_data(soup)
                
                name=clean_product_name(product_content)[0]
                percentage=clean_product_name(product_content)[1]
                percentage_cl=clean_product_name(product_content)[2]
                
                price=clean_product_data(product_data)[0]
                unit_price=clean_product_data(product_data)[1]
                
                if page == 0:
                    data=create_df(clean_product_data(product_data),clean_product_name(product_content))
                
                data=update_dataset(data,create_df(clean_product_data(product_data),clean_product_name(product_content)))
                    
        except Exception as e:
            print("Error with link {}:".format(e))
            # continue
                
        finally:
            start_location=link.rfind('/')+1
            end_location=len(link)
            data.to_csv('../data/'+link[start_location:end_location]+'.csv')
            df=df.append(data,ignore_index=True)
            
    return df


print(scrape_whisky())
    
# soup=scrape_html('https://www.thewhiskyexchange.com/c/40/single-malt-scotch-whisky',1)
# # get_product_data(soup)
# product_data = get_product_data(soup)
# product_content = get_product_content(soup)
# print(create_df(clean_product_data(product_data),clean_product_name(product_content)))
# print(get_links())
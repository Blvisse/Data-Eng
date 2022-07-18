from scrape_data import scrape_whisky

product_data=scrape_whisky()

#export to csv file
product_data.to_csv('../data/whisky_data.csv',index=False)
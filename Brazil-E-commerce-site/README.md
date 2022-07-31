# Introduction

This project was adapted from @ajupton [github] big data engineering

Retailers in the current landscape are adapting to the digital age. Digital retail behemoths have carved out substantial market shares in the online space at the same time that traditional retail stores are broadly in decline. In this time of digital flux, an omni-channel retail approach is necessary to keep pace. This is especially true for retailers that have invested in an extensive brick-and-mortar store portfolio or have strong relationships with brick-and-mortar partners.

This data engineering project uses a real world retail dataset to explore delivery performance at scale. The primary concern of data engineering efforts in this project is to create a strong foundation onto which data analytics and modeling may be applied as well as provide summary reports for daily ingestion by decision makers.

A series of ETL jobs are programmed as part of this project using python, SQL, Airflow, and Spark to build pipelines that download data from an AWS S3 bucket, apply some manipulations, and then load the cleaned-up data set into another location on the same AWS S3 bucket for higher level analytics.



# Data
Data in this project is from the Brazilian Olist Company fetched from Kaggle [datalink].
We version the data using dvc and hence our data folder will rather be a dvc repo link reference 



# Working with the Project

## Prerequisites
1. Amazon Web Services Account 
2. Amazon S3 bucket
3. DVC
4. Jupyter
5. SQLALchemy
6. Spark


## Steps
###   1. Download the data from Kaggle

Create a new data directory and Using the [datalink] access and download the data into the directory

### 2. Create an S3 bucket
Log into your amazon web service account and create an s3 bucket where we shall store our data.














[github]: https://github.com/ajupton/big-data-engineering-project

[datalink]: https://github.com/ajupton/big-data-engineering-project
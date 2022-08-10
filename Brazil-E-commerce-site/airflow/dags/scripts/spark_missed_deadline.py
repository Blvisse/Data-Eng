from zipfile import ZipFile

print("Unziping file ...")
with ZipFile('/opt/airflow/data') as zipObj:
    zipObj.extractall()
    
print("File unzipped successfully")
    
try:
    import findspark
    findspark.init()
    from pyspark import SparkConf, SparkContext
    from pyspark.sql import SparkSession
    from pyspark.sql import *
    from pyspark.sql.functions import *
    print("Successfully loaded spark ...")
    
except ImportError as IE:
    print("Error loading Spark: {}".format(IE))
    exit(1)
    
#configure spark session
conf=SparkConf().setMaster("local").setAppName("SparkSQL_NLP")
spark=SparkSession.builder.getOrCreate()

print(spark)

#set sqlContext from Spark context
from pyspark.sql import SQLContext
sqlContext = SQLContext(spark)

#setup the context and read into it the padnas dataframe
spark.conf.set("spark.sql.execution.arrow.enabled", "true")

spark = SparkSession.builder.getOrCreate()

# Load in csv file into spark dataframe

print ("Loading data into spark dataframe ...")
df_items = spark.read.format("csv") \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .load("/opt/airflow/data/olist_order_items_dataset.csv")

df_orders = spark.read.format("csv") \
             .option("header", "true") \
             .option("inferSchema", "true") \
             .load("/opt/airflow/data/olist_orders_dataset.csv")

df_products = spark.read.format("csv") \
             .option("header", "true") \
             .option("inferSchema", "true") \
             .load("/opt/airflow/data/olist_products_dataset.csv")
             
print("Creating RDDs ...")           
# Create SQL Tables from dfs
df_items.createOrReplaceTempView('items')
df_orders.createOrReplaceTempView('orders')
df_products.createOrReplaceTempView('products')

#we replicate the late carrier deliveries query
late_deliveries=spark.sql('''
                          
                          SELECT i.order_id,i.seller_id,i.shipping_limit_date,i.price,i.freight_value,
                          p.product_id,p.product_category_name,
                          o.customer_id,o.order_status,o.order_purchase_timestamp,o.order_estimated_delivery_date,
                          o.order_delivered_customer_date,o.order_delivered_carrier_date
                          
                          FROM items AS i
                          JOIN orders AS o
                          ON i.order_id = o.order_id
                          JOIN products AS p
                          ON i.product_id = p.product_id
                          WHERE i.shipping_limit_date < o.order_delivered_carrier_date
                          
                          
                          ''')

#write results into csv file
print("Writing results into csv file ...")
late_deliveries.coalesce(1).write.option("header","true").csv("/opt/airflow/data/late_deliveries")
print("Results written successfully ...")
print("Closing spark session ...")
spark.stop()
print("Done!")
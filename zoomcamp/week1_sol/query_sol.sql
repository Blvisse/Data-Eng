
'''

Question 1

How many taxi trips were there on January 15?

Consider only trips that started on January 15.

'''

-- #How many trips were there on January 15
SELECT EXTRACT(DAY FROM tpep_pickup_datetime) AS datecol,COUNT(*) AS number_of_trips
FROM jan_trips
WHERE EXTRACT(DAY FROM tpep_pickup_datetime)=15
GROUP BY datecol;

-- SELECT COUNT(*)
-- FROM jan_trips
-- WHERE tpep_pickup_datetime::date = '2021-01-15';


'''
Question 2
Find the largest tip for each day. On which day it was the largest tip in January?

Use the pick up time for your calculations.


'''

SELECT EXTRACT(DAY FROM tpep_dropoff_datetime) AS datecol, MAX (tip_amount) AS total_tips 
FROM jan_trips
GROUP BY datecol
ORDER BY total_tips DESC;

'''
Question 3

What was the most popular destination for passengers picked up in central park on January 14?

Use the pick up time for your calculations.

Enter the zone name (not id). If the zone name is unknown (missing), write "Unknown"

'''

SELECT ja."DOLocationID" AS dropoff,COALESCE(lo.zone,'Unknown') AS dropf, EXTRACT(DAY FROM tpep_dropoff_datetime) AS pick_up_date,COUNT(*) AS drop_off_counts 
FROM jan_trips AS ja
LEFT JOIN lookup_data AS lo
ON ja."DOLocationID"=lo."LocationID"
WHERE ja."PULocationID"=43 AND EXTRACT(DAY FROM tpep_dropoff_datetime)=14
GROUP BY pick_up_date,dropoff,dropf
ORDER BY drop_off_counts DESC;
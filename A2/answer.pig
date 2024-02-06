-- Name: Mubin Qureshi
-- Student ID: 180181900

data = LOAD 'AB_NYC_2019.csv' USING PigStorage(',')
     AS (id:int, name:chararray, host_id:int, host_name:chararray,
         neighbourhood_group:chararray, neighbourhood:chararray,
         latitude:float, longitude:float, room_type:chararray,
         price:int, minimum_nights:int, number_of_reviews:int,
         last_review:chararray, reviews_per_month:double,
         calculated_host_listings_count:int, availability_365:int);
 
 
--  Step 1: Analyze only the records where minimum_nights>10, number_of_reviews > 10 and the last_review falls within 2018 or 2019.

filtered = FILTER data BY (minimum_nights > 10) AND (number_of_reviews > 10) AND (last_review MATCHES '.*201[89].*');


-- Step 2: For the records from Step 1, get the general neighborhood_group information and store it with the neighbourhood_group as the 1st column, the average price of the neighbourhood_group as the 2nd column, and the average number of available days for the neighbourhood_group as the 3rd column. Order the results by price, from high to low, and save them to the folder "AirBnB_neighbourhood" in the hdfs home directory.

group_neighbourhood = GROUP filtered BY neighbourhood_group;

stats = FOREACH group_neighbourhood GENERATE
                      group AS neighbourhood_group,
                      AVG(filtered.price) AS avg_price,
                      AVG(filtered.availability_365) AS avg_availability;

ordered_stats = ORDER stats BY avg_price DESC;
DUMP ordered_stats;
STORE ordered_stats INTO 'AirBnB_neighbourhood' USING PigStorage(',');


--Step 3:  For the records from Step 1, display on the console the room_type, the lowest price for each room_type, and the name of the property with the lowest price for the room_type.

grouped_by_room_type = GROUP filtered BY room_type;
results = FOREACH grouped_by_room_type {
    ordered_rooms = ORDER filtered BY price ASC;
    lowest = LIMIT ordered_rooms 1;
    GENERATE group AS room_type, 
             flatten(lowest.price) AS lowest_price, 
             flatten(lowest.name) AS property_name;
}
DUMP results;

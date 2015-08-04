buys_count.py: count a user's comsumer on each category from over 18 million lines of data;

buys_count_with_date.py: count a user's comsumer over the latest 30,60,90 days;

buys_count_without_spark.py: The same function with buys_count.py,without using spark;

get_data.py: read data from mysql database;

last_5_package: count the last 5 used package;

region_count: count the region of a user.

interest_calculate: calculate the interest of a user on each category (including first class and second class), the input file is the latest opened packages of a user.
and the interest calculate :  interest(i)=0.9*interest(i-1)+interest,
the normalized interest: normal_interest=1.0/(1+exp(-interest)).

active_calculate: calculate the activeness of a user on ecah category (including first class and second class), the activeness of each category is the latest time the user open.

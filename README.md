# IPL-Database-Analysis-using-PySpark-and-SQLite3-on-python

[![Python 3.9](https://img.shields.io/badge/python-3.9-blue.svg)](https://www.python.org/downloads/release/python-390/)

## About the Project

I have created a database and imported three tables into the database. Then, I ran several SQL queries to find out several facts related to IPL matches. I have done the work using both pyspark and sqlite3 module. To implement sqlite3 I have written a class then, created an object and worked with the object to fetch the query results.

## Libraries Used

- pyspark
- pandas
- sqlite3
- os
- findspark

## Database Schema

![databse schema](https://github.com/Souryadipstan/IPL-Database-Analysis-using-PySpark-and-SQLite3-on-python/blob/ipl_dataset_2/ipl_dataset_schema.jpg?raw=true)

## Queries

1. Find the top 3 venues which hosted the most number of eliminator matches?
2. Return most number of catches taken by a player in IPL history?
3. Write a query to return a report for highest wicket taker in matches which were affected by Duckworth-Lewis’s method (D/L method).
4. Write a query to return a report for highest strike rate by a batsman in non powerplay overs(7-20 overs)
Note: strike rate = (Total Runs scored/Total balls faced by player) *100, Make sure that balls faced by players should be legal delivery (not wide balls or no balls).
5. Write a query to return a report for highest extra runs in a venue (stadium, city).
6. Write a query to return a report for the cricketers with the most number of players of the match award in neutral venues.
7. Write a query to get a list of top 10 players with the highest batting average Note: Batting average is the total number of runs scored divided by the number of times they have been out (Make sure to include run outs (on non-striker end) as valid out while calculating average).
8. Write a query to find out who has officiated (as an umpire) the most number of matches in IPL.
9. Find venue details of the match where V Kohli scored his highest individual runs in IPL.
10. Creative Case study:
Please analyze how winning/losing tosses can impact a match and it's result? (Marks for Visualization also)

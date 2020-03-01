
# PURPOSE 

This purpose of setting up this database is to allow the users of **Sparkify(music streaming startup)** to be able to easily query data to achieve their analytic goals. As the data currently resides as a bundle of json files, it is not well organized for fetching information for analytical purposes. Hence using this data in the json files and organizing it as star schema would make it a lot easier for querying purposes. ***Hence this project involves designing a star schema, defining fact and dimension tables, building an ETL pipeling to automate the table loading process from source to target. This database is built in postgres and uses python for ETL pipeline.*** This database will be tested against some sql queries provided by the analytics team. 


# WHERE IS THE RAW DATA 

The raw data is in the form of json files which contains song and log information. You can check this in the data folder. These are accessed using python and transformed before they are loaded in our fact and dimension tables. 


# STAR SCHEMA

![Alt text](data/ss.png?raw=true "STAR SCHEMA")



# FILES 


data - Folder that conatains raw data in the form of json files. 
create_tables.py - Program to Create the schema structure that calls SQL queries in sql_queries.py and creates the tables. 
etl.py - ETL code to process data and load the tables
etl.ipynb - Trail loading test with one row before full loading 
Readme.md - Project Description 
test.ipynb - File used to test if data is loaded or not

First run the create_tables.py followed by etl.py to load the tables. We can then test if the etl was asucessful ot not using test.ipynb

# Example Queries

1. Which artist from California has the highest users listening to their music post 10pm? 
2. Which county in Michigan generates maximum revenue due to artist 'X'?



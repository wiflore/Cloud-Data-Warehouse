#### Project: Data Warehouse

### Discuss the purpose of this database in context of the startup, Sparkify, and their analytical goals.

A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

I built an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to. I tested the database and ETL pipeline by running queries given to me by the analytics team from Sparkify and compared my results with their expected results.


### State and justify your database schema design and ETL pipeline.

#### Schema for Song Play Analysis
Using the song and event datasets, i created a star schema optimized for queries on song play analysis. This includes the following tables.

##### Fact Table  
* songplays - records in event data associated with song plays i.e. records with page NextSong
** songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

##### Dimension Tables
* users - users in the app
* user_id, first_name, last_name, gender, level
* songs - songs in music database
* song_id, title, artist_id, year, duration
* artists - artists in music database
* artist_id, name, location, lattitude, longitude
* time - timestamps of records in songplays broken down into specific units
* start_time, hour, day, week, month, year, weekday

##### Project Template
The project template includes four files:  

* create_table.py is where I creatde the fact and dimension tables for the star schema in Redshift.
* etl.py is where I loaded data from S3 into staging tables on Redshift and then process that data into your analytics tables on Redshift.
* sql_queries.py is where I defined wje SQL statements, which will be imported into the two other files above.

#### Project Steps
Below are steps you can follow to complete each component of this project.

##### Create Table Schemas
1. Design schemas for your fact and dimension tables
2. Write a SQL CREATE statement for each of these tables in sql_queries.py
3. Complete the logic in create_tables.py to connect to the database and create these tables
4. Write SQL DROP statements to drop tables in the beginning of create_tables.py if the tables already exist. This way, you can run create_tables.py whenever you want to reset your database and test your ETL pipeline.
5. Launch a redshift cluster and create an IAM role that has read access to S3.
6. Add redshift database and IAM role info to dwh.cfg.
7. Test by running create_tables.py and checking the table schemas in your redshift database. You can use Query Editor in the AWS Redshift console for this.
8. Build ETL Pipeline
9. Implement the logic in etl.py to load data from S3 to staging tables on Redshift.
10. Implement the logic in etl.py to load data from staging tables to analytics tables on Redshift.
11. Test by running etl.py after running create_tables.py and running the analytic queries on Redshift database to compare your results with the expected results.
12. Delete  redshift cluster when finished.

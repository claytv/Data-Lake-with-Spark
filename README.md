## Data Lake with Spark 
An introduction to using Apache Spark 

### The goal of this project is to build an ETL pipeline that extracts song and event data from S3 for sparkify and transforms it into a data lake written to another location so that the data is usable for analytics. 

### File Descriptions 

* README.md - Overview of the project with instructions
* data - Folder containing all of the data
* dl.cfg - Configuration files which allows the user to hide information but still give the other files access 
         - Change information in this file to match your S3 Bucket
         
* etl.py - Reads in song and log data from the desired input and writes parquet files for songs_table, artists_table, users_table, time_table, and songplays_table

### Config
* Create a S3 bucket and put the name in dl.cfg file ( Or perform locally ) 
* Update AWS keys to your own in dl.cfg

### Update etl.py
* Update input_data and output_data variables in main() from 'etl.py' to reflect local or S3 usage

### Run etl.py 
* In the terminal execute the command 'python etl.py'

### Verify 
* Examine the parquet files in the output_data to ensure they are properly written

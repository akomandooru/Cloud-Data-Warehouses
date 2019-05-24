# Summary of the project
 The goal of this project is to develop an ETL pipeline tool for Sparkify that extracts song data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for an analytics team to continue finding insights in what songs their users are listening to.

[Fact and Dimension table schema](https://drive.google.com/file/d/1DeIz6PCJRfsi_hqoOLTC3QLRJRrxVfFd/view?usp=sharing)

### Instruction to run the tool from python console

```sh
$ python create_tables.py
$ python etl.py
```

Code explaination

```sh
dwh.cfg - this file has all of the configuration settings in it (redshift, and S3 storage information)
sql_queries.py - this file has all of the sql scripts needed for dropping tables, creating tables, copying data from S3 to staging tables, and inserting data from staging tables to the fact and dimension tables
create_tables.py - this script file has python code to drop and create tables in redshift
etl.py - this script file will copy data from S3 to staging tables, and insert data from staging tables to the fact/dimension tables
```
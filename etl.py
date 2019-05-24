import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    This function will load the staging tables using the copy table queries
    Inputs: 
        cur: database cursor
        conn: database connection
    """ 
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    This function will select data from staging tables and insert them into the fact and dimension tables
    Inputs: 
        cur: database cursor
        conn: database connection
    """ 
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    This is the main function in the etl script.
    This script will connect to a redshift cluster, load the staging tables with log and song data, and select/insert to fact
    and dimension tables.
    """    
    
    #Configure the parser to read the redshift cluster settings
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    #connect to the redshift cluster
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    #load the staging tables from the log/song files in S3 storage
    load_staging_tables(cur, conn)
    #select data from the staging tables and insert them into the fact and dimension tables
    insert_tables(cur, conn)

    #close the connection
    conn.close()


if __name__ == "__main__":
    main()
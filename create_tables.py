import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    This function will drop the tables in the database if they exist
    Inputs: 
        cur: database cursor
        conn: database connection
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    This function will create the tables in the database
    Inputs: 
        cur: database cursor
        conn: database connection
    """    
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    This is the main function in the create_tables script.
    This script will connect to a redshift cluster, drop tables, and create table schema to prepare for ETL later on.
    """
    
    #Populate the configuration parser so we can read the settings to connect to the redshift cluster
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    #Connect to the redshift cluster
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    #Drop the tables if they exist
    drop_tables(cur, conn)
    #Create the table structures for ETL later on
    create_tables(cur, conn)

    #Close connection to the cluster
    conn.close()


if __name__ == "__main__":
    main()
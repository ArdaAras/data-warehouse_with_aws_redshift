import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Fills staging_events and staging_songs tables by copying data residing in S3
    Parameters:
            cur       (cursor) : Database connection cursor object
            conn  (connection) : Database connection object
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Fills songplays, users, songs, artists and time analytical tables using staging tables
    Parameters:
            cur       (cursor) : Database connection cursor object
            conn  (connection) : Database connection object
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Connects to redshift 'sparkify' database using values in dwh.cfg file.
    Fills staging, dimension and fact tables. Closes connection upon table creation ends.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
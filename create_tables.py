import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Drops staging, dimension and fact tables using 'drop_table_queries' list
    Parameters:
            cur       (cursor) : Database connection cursor object
            conn  (connection) : Database connection object
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Creates staging, dimension and fact tables using 'create_table_queries' list
    Parameters:
            cur       (cursor) : Database connection cursor object
            conn  (connection) : Database connection object
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Connects to redshift 'sparkify' database using values in dwh.cfg file.
    Creates staging, dimension and fact tables. Closes connection upon table creation ends.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
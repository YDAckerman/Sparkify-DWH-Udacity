import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    - execute each query in the copy_table_queries list
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    - execute each query in the insert_table_queries list
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    - get configuration information
    - connect to redshift cluster
    - copy staging tables from s3
    - create star schema from staging tables
    - close the connection
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn_str = "host={} dbname={} user={} password={} port={}"
    conn = psycopg2.connect(conn_str.format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()

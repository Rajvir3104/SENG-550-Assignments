import psycopg2
db_connect = psycopg2.connect(dbname='postgres@localhost',
                                 user='postgres',
                                 password='postgres',
                                 host='localhost',
                                 port=5432)


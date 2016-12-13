# coding:utf-8
import psycopg2


def test_insert():
    con = psycopg2.connect('host=localhost port=5432 user=postgres')
    cur = con.cursor()
    cur.execute('DROP FOREIGN TABLE IF EXISTS ncos_example;')
    cur.execute('DROP SERVER IF EXISTS multicorn_ncos;')
    cur.execute('DROP EXTENSION IF EXISTS multicorn;')
    cur.execute('CREATE EXTENSION multicorn;')
    cur.execute("CREATE SERVER multicorn_ncos FOREIGN DATA WRAPPER multicorn\
                 options (wrapper 'ncos_fdw.NCOSForeignDataWrapper');")
    cur.execute("CREATE FOREIGN TABLE ncos_example (\
                   id integer,\
                   key varchar\
                 ) SERVER multicorn_ncos\
                 options (\
                   bucket 'test',\
                   prefix 'insert/',\
                   endpoint 'http://localhost:4569',\
                   access_key 'access_key',\
                   secret_key 'secret_key',\
                   store_as 'text',\
                   format 'json'\
                 );")
    cur.execute("INSERT INTO ncos_example (id, key) VALUES (1, 'v1'), (2, 'v2')")
    con.commit()

    cur.execute("SELECT * FROM ncos_example")
    assert cur.fetchall() == [
        (1, "v1"),
        (2, "v2"),
    ]

import psycopg2 as psql
import os

file = os.path.join("secrets", ".psql.pass")
with open(file, "r") as file:
        password=file.read().rstrip()

conn_string="host=hadoop-04.uni.innopolis.ru port=5432 user=team26 dbname=team26_projectdb password={}".format(password)

with psql.connect(conn_string) as conn:
        
        cur = conn.cursor()
        with open(os.path.join("sql","create_table.sql")) as file:
                content = file.read()
                cur.execute(content)
        conn.commit()

        with open(os.path.join("sql", "import_data.sql")) as file:
                commands= file.readlines()
                with open(os.path.join("data", "accidents_1.csv"), "r") as depts:
                        cur.copy_expert(commands[0], depts)

        conn.commit()
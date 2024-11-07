import pandas as pd
from sqlalchemy import create_engine, text

mysql_engine = create_engine("mysql+pymysql://joe:JoeThuku1948@localhost/school_db")
postgres_engine = create_engine("postgresql+psycopg2://postgres:joe@localhost/teachers_db")

# Query both databases and combine results
def federated_query():
    with mysql_engine.connect() as mysql_conn, postgres_engine.connect() as postgres_conn:
        students = pd.read_sql("SELECT * FROM students", mysql_conn)
        teachers = pd.read_sql("SELECT * FROM teachers", postgres_conn)

        print("MySQL - Students:")
        print(students)
        print("\nPostgreSQL - Teachers:")
        print(teachers)

        # Example joining on arbitrary conditions; here we just show both tables side by side
        combined = pd.concat([students, teachers], axis=1)
        print("\nFederated Query Result:")
        print(combined)

if __name__ == "__main__":
    federated_query()

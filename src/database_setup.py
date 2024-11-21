from sqlalchemy import create_engine, text

# Database connections
mysql_engine = create_engine("mysql+pymysql://joe:JoeThuku1948@localhost/school_db")
postgres_engine = create_engine("postgresql+psycopg2://postgres:joe@localhost/teachers_db")


def setup_mysql():
    with mysql_engine.connect() as conn:
        try:
            with conn.begin():  # Explicitly manage the transaction
                conn.execute(text("DROP TABLE IF EXISTS students"))
                conn.execute(text("""
                    CREATE TABLE students (
                        student_id INT PRIMARY KEY,
                        first_name VARCHAR(50),
                        last_name VARCHAR(50),
                        email VARCHAR(100)
                    )
                """))
                conn.execute(text("""
                    INSERT INTO students (student_id, first_name, last_name, email) VALUES
                    (1, 'John', 'Doe', 'john.doe@example.com'),
                    (2, 'Jane', 'Smith', 'jane.smith@example.com')
                """))
            print("Data inserted successfully.")
        except Exception as e:
            print(f"An error occurred while setting up MySQL: {e}")


def setup_postgres():
    with postgres_engine.connect() as conn:
        conn.execute(text("DROP TABLE IF EXISTS teachers"))
        conn.execute(text("""
            CREATE TABLE teachers (
                teacher_id INT PRIMARY KEY,
                first_name VARCHAR(50),
                last_name VARCHAR(50),
                email VARCHAR(100)
            )
        """))
        conn.execute(text("""
            INSERT INTO teachers (teacher_id, first_name, last_name, email) VALUES
            (1, 'Alice', 'Brown', 'alice.brown@example.com'),
            (2, 'Bob', 'White', 'bob.white@example.com')
        """))

if __name__ == "__main__":
    setup_mysql()
    setup_postgres()
    print("Databases and tables set up successfully.")

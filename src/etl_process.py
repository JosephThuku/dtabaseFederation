import pandas as pd
from sqlalchemy import create_engine
import matplotlib.pyplot as plt
import seaborn as sns

# Database engines
mysql_engine = create_engine("mysql+pymysql://joe:JoeThuku1948@localhost/school_db")
postgres_engine = create_engine("postgresql+psycopg2://postgres:joe@localhost/teachers_db")
# warehouse_engine = create_engine("postgresql+psycopg2://postgres:joe@localhost/warehouse_db")

# Extract, Transform, Load
def etl_process():
    # Step 1: Extract
    students_df = pd.read_sql("SELECT * FROM students", mysql_engine)
    teachers_df = pd.read_sql("SELECT * FROM teachers", postgres_engine)
    
    print("Extracted Students Data:")
    print(students_df)
    print("\nExtracted Teachers Data:")
    print(teachers_df)

    # Step 2: Transform - Concatenate data as an example transformation
    combined_df = pd.concat([students_df, teachers_df], axis=1)
    print("\nTransformed Combined Data:")
    print(combined_df)

    # # Step 3: Load - Load into PostgreSQL warehouse
    # combined_df.to_sql("combined_data", warehouse_engine, if_exists="replace", index=False)
    # print("\nData loaded into data warehouse.")

    # Visualization
    sns.pairplot(combined_df)
    plt.show()

if __name__ == "__main__":
    etl_process()

import pandas as pd
from sqlalchemy import create_engine
import matplotlib.pyplot as plt
import seaborn as sns

mysql_engine = create_engine("mysql+pymysql://joe:JoeThuku1948@localhost/school_db")
postgres_engine = create_engine("postgresql+psycopg2://postgres:joe@localhost/teachers_db")
warehouse_engine = create_engine("postgresql+psycopg2://postgres:joe@localhost/warehouse_db")

def etl_process():
    try:
        # Step 1: Extract
        students_df = pd.read_sql("SELECT * FROM students", mysql_engine)
        teachers_df = pd.read_sql("SELECT * FROM teachers", postgres_engine)

        teachers_df = teachers_df.rename(columns={
            'first_name': 'teacher_first_name',
            'last_name': 'teacher_last_name',
            'email': 'teacher_email'
        })

        print("Extracted Students Data:")
        print(students_df)
        print("\nExtracted Teachers Data:")
        print(teachers_df)

        # Step 2: Transform
        combined_df = pd.concat([students_df, teachers_df], axis=1)
        combined_df = combined_df.dropna()  # Optional: Handle missing data
        print("\nTransformed Combined Data:")
        print(combined_df)

        # Step 3: Load
        combined_df.to_sql("combined_data", warehouse_engine, if_exists="replace", index=False)
        print("\nData loaded into data warehouse.")

        # Visualization
        sns.pairplot(combined_df)
        plt.savefig("pairplot.png")  # Save plot to file
        print("\nVisualization saved as 'pairplot.png'.")

    except Exception as e:
        print(f"An error occurred during ETL process: {e}")

if __name__ == "__main__":
    etl_process()

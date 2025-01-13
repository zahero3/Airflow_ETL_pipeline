
#@Author ZaheerAhamad

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import psycopg2
import pandas as pd

# Define the ETL process function
def etl_process():
    # Step 1. Defining the Data Source
    conn = psycopg2.connect(
        host="localhost",
        database="postgres",
        user="home",
        password="233"
    )
    cursor = conn.cursor()

    ##################################################### Extraction #####################################################################
    # Step 2. Data Extraction
    query = "SELECT * FROM practice.superstore_orders"
    data = pd.read_sql(query, conn)

    ##################################################### Transformation ################################################################
    # Step 3. Data Transformation
    data['Order_Date'] = pd.to_datetime(data['Order_Date'], format='%d/%m/%y')    # Changing Data Type to Datetime
    data['Ship_Date'] = pd.to_datetime(data['Ship_Date'], format='%d/%m/%y')      # Changing Data Type to Datetime
    data['Order_ID'] = data['Order_ID'].replace('CA-2020', 'MX-2020', regex=True) # Replacing substring from a col
    data['Postal_Code'] = data['Postal_Code'].fillna(0).astype(int)               # Filling missing values with 0 and then changing type to int

    # Generate a unique table name (e.g., timestamp-based)
    import datetime
    table_name = f"Store_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}"

    # Create Table Dynamically
    create_table_query = f"""
    CREATE TABLE practice.{table_name} AS
    SELECT * FROM practice.superstore_orders WHERE 1=0;
    """
    cursor.execute(create_table_query)
    print(f"Table 'practice.{table_name}' created successfully.")

    # Save transformed data to CSV
    data.to_csv("Transformed_data.csv", index=False, header=False)

    ##################################################### LOADING #####################################################################
    # Step 4. Loading/Inserting data into target table
    with open("Transformed_data.csv", "r") as f:
        cursor.copy_expert(f"COPY practice.{table_name} FROM STDIN WITH CSV", f)
    print(f"Data loaded into target table")

    conn.commit() # Committing the changes
    cursor.close()
    conn.close()

# Define the DAG
dag = DAG(
    'etl_pipeline',
    schedule_interval=timedelta(seconds=10),  # Runs every 10 seconds
    start_date=datetime(2023, 1, 1),
    catchup=False
)

# Define the task
task = PythonOperator(
    task_id='run_etl',
    python_callable=etl_process,
    dag=dag
)
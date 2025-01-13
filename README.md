# Airflow_ETL_pipeline # ETLpipeline # airflow # apacheairflow
ETL Pipeline with Apache Airflow

**Overview**
This project demonstrates an ETL (Extract, Transform, Load) pipeline built using Apache Airflow. The pipeline extracts data from a PostgreSQL database, applies transformations, and loads the transformed data back into a new table in the same database. The ETL process is automated using an Airflow DAG that runs at regular intervals.

**Features**
  1.	Data Extraction: Connects to a PostgreSQL database and fetches data from a source table.
	2.	Data Transformation:
	    •	Converts date columns to a standard datetime format.
	    •	Replaces substrings in specific columns.
	    •	Handles missing values for integer columns.
	3.	Data Loading: Dynamically creates a new table in the database and loads the transformed data into it.
	4.	Automation: Scheduled to run every 10 seconds using Apache Airflow.
	5.	Logs and Debugging: All process logs can be accessed via the Airflow webserver.

 **How It Works**
	1.	Schedule: The pipeline is set to run every 10 seconds using Airflow’s scheduler.
	2.	Process:
	      •	Extraction: Fetches data from the source table in the PostgreSQL database.
	      •	Transformation: Applies the following:
	      •	Standardizes date formats.
	      •	Replaces substrings in the Order_ID column.
	      •	Fills missing values in the Postal_Code column with 0.
	      •	Loading: Creates a new table dynamically and inserts the transformed data.
	3.	Monitoring: Logs and task statuses can be monitored via the Airflow web interface.

 **Logs and Debugging**
	•	Logs: The ETL pipeline’s logs are available on the Airflow webserver.
	•	Print Statements: Output from print() commands in the code will appear in the Airflow logs for the respective     task.

**Important Notes**
	•	The transformed data is saved as a CSV file (Transformed_data.csv) in the working directory during the             process.
	•	Ensure the PostgreSQL database is accessible and the credentials in the script are correct.

 **Future Enhancements**
	•	Add error handling and retries for database operations.
	•	Enable email alerts for task failures.
	•	Extend the pipeline to support multiple data sources.

 

from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator
import requests
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
from datetime import datetime, timedelta
from airflow import DAG

import mysql.connector

# Load the .env file
load_dotenv()

# Define default arguments for the DAG
default_args = {
    'owner': 'Data Engineer Department: Lucky Ramasila',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Initialize the DAG
with DAG(
    'planet42_etl',
    default_args=default_args,
    description='ETL pipeline to collect data from API and Load into database',
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:


    # Task 1: Extract data from API
    def extract_data(**kwargs):
        """Extract data from API"""
        # API details
        api_url = os.getenv('api_url')
        headers = {"Content-Type": "application/json", "x-api-key": os.getenv('api_key')}
        payload = {
            "start_date": "2023-01-01",
            "end_date": "2023-01-31"
        }

        try:
            response = requests.post(api_url, headers=headers, json=payload)
            data = response.json()
            pd.DataFrame(data).to_csv('/tmp/raw_transactions.csv', index=False)
            print("Data extracted successfully!")
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 500:
                raise AirflowException("Internal Server Error: Please try again later.")
            else:
                raise AirflowException(f"Error: {e}")


    # Task 2: Transform data
    def transform_data(**kwargs):
        """Transform the data"""

        # create dataframe from csv
        df = pd.read_csv('/tmp/raw_transactions.csv')

        # Drop rows with missing values
        df = df.replace({None: "uncategorized"})

        # Convert transaction_date to datetime
        df['transaction_date'] = pd.to_datetime(df['transaction_date'])

        # Stop duplicate rows
        df.drop_duplicates(inplace=True)

        # Filter transations that are negative
        df = df[df['transaction_amount'] >= 0]

        # Create column that labels transactions based on amount
        df['amount_category'] = pd.cut(
            df['transaction_amount'],
            bins=[0, 50, 200, float('inf')],
            labels=['Low', 'Medium', 'High']
        )

        # Window function to calculate total per customer
        df['total_per_customer'] = df.groupby('customer_id')['transaction_amount'].transform('sum')

        # Save transformed data
        df.to_csv('/tmp/transformed_transactions.csv', index=False)
        print("Data transformed successfully!")

    def load_data_to_database(**kwargs):
        ''' Load data to database '''

        # Create dataframe from transformed csv
        df = pd.read_csv('/tmp/transformed_transactions.csv')

        # Create database engine
        engine = create_engine(f"mysql+pymysql://{os.getenv("login")}:{os.getenv("password")}@{os.getenv("host")}/{os.getenv("database")}")

        # Load data to MySQL
        df.to_sql('transactions', con=engine, if_exists='replace', index=False)

        print("Data successfully loaded into MySQL!")



    def create_table(**kwargs):
        """Create table in database"""
        db = mysql.connector.connect(
            host=os.getenv('host'),
            user=os.getenv('login'),
            password=os.getenv('password'),
            database=os.getenv('database'),
        )
        cursor = db.cursor()
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS p42_transactions (
            customer_id VARCHAR(255),
            product_id VARCHAR(255),
            transaction_date DATE,
            transaction_amount DECIMAL(10, 2),
            transaction_type VARCHAR(50),
            product_category VARCHAR(50),
            amount_category VARCHAR(50),
            total_per_customer DECIMAL(10, 2)
);
        ''')
        db.commit()
        cursor.close()
        db.close()

    def execute_query(query: str,query_name: str = None):
        """Execute query in database"""
        db = mysql.connector.connect(
            host=os.getenv('host'),
            user=os.getenv('login'),
            password=os.getenv('password'),
            database=os.getenv('database'),
        )
        cursor = db.cursor()
        cursor.execute(query)
        result = cursor.fetchone()
        if result:
            print(f'query {query_name} executed successfully')

        db.commit()
        cursor.close()
        db.close()

    def transactions_per_prodcatergory(**kwargs):
        "Create transactions_per_prodcatergory table in database"
        query_name='transactions_per_prodcatergory'
        query = '''            
            CREATE OR REPLACE VIEW transactions_per_productcategory AS
                SELECT product_category, 
                       COUNT(*) AS total_transactions
                FROM planet42.p42_transactions
                GROUP BY product_category;
            '''
        execute_query(query, query_name)

    def top5_transactions(**kwargs):
        "Create top5_transactions table in database"
        query_name='top5_transactions'
        query = '''            
            CREATE OR REPLACE VIEW top5_transactions AS
                SELECT 
                    customer_id, 
                    SUM(transaction_amount) AS total_value
                FROM planet42.p42_transactions
                GROUP BY customer_id
                ORDER BY total_value DESC
                LIMIT 5;
            '''
        execute_query(query, query_name)

    def monthlyspend_trends(**kwargs):
        "Create monthlyspend_trends table in database"
        query_name='monthlyspend_trends'
        query = '''            
            CREATE OR REPLACE VIEW monthlyspend_trends AS
                SELECT 
                    MONTH(transaction_date) AS month, 
                    SUM(transaction_amount) AS total_spend
                FROM transactions
                GROUP BY month;
            '''
        execute_query(query, query_name)

    extract_task = PythonOperator(
        task_id='extract_data_from_api',
        python_callable=extract_data,
        dag=dag,
    )

    transform_task = PythonOperator(
        task_id='transform_csv_data',
        python_callable=transform_data,
        dag=dag,
    )

    create_table_task = PythonOperator(
        task_id='create_table',
        python_callable=create_table,
        dag=dag
    )

    load_task = PythonOperator(
        task_id='load_data_to_database',
        python_callable=load_data_to_database,
        dag=dag,
    )

    transactions_per_prodcatergory_task = PythonOperator(
        task_id='transactions_per_prodcatergory',
        python_callable=transactions_per_prodcatergory,
        dag=dag,
    )

    top5_transactions_task = PythonOperator(
        task_id='top5_transactions',
        python_callable=top5_transactions,
        dag=dag,
    )

    monthlyspend_trends_task = PythonOperator(
        task_id='monthlyspend_trends',
        python_callable=monthlyspend_trends,
        dag=dag,
    )

    extract_task >> transform_task >> create_table_task >>load_task >> [transactions_per_prodcatergory_task,
                                                                        top5_transactions_task,
                                                                        monthlyspend_trends_task]


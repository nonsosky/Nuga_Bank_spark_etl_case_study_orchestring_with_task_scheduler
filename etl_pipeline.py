from pyspark.sql import SparkSession
from pyspark.sql import DataFrameWriter
from pyspark.sql.functions import monotonically_increasing_id
import os
import psycopg2

# set java home
os.environ['JAVA_HOME'] = 'C:\java8'

# Initialise my Spark Session
spark = SparkSession.builder \
        .appName("Nuga Bank ETL") \
        .config("spark.jars", "postgresql-42.7.4.jar") \
        .getOrCreate()

# Extract this historical data into a spark dataframe
df = spark.read.csv(r'dataset\rawdata\nuga_bank_transactions (1).csv', header=True, inferSchema=True)

# Data cleaning and transformation
for column in df.columns:
    print(column, 'Nulls: ', df.filter(df[column].isNull()).count())

df_clean = df.fillna({
    'Customer_Name' : 'Unknown',
    'Customer_Address' : 'Unknown',
    'Customer_City' : 'Unknown',
    'Customer_State' : 'Unknown',
    'Customer_Country' : 'Unknown',
    'Company' : 'Unknown',
    'Job_Title' : 'Unknown',
    'Email' : 'Unknown',
    'Phone_Number' : 'Unknown',
    'Credit_Card_Number' : 0,
    'IBAN' : 'Unknown',
    'Currency_Code' : 'Unknown',
    'Random_Number' : 0.0,
    'Category' : 'Unknown',
    'Group' : 'Unknown',
    'Is_Active' : 'Unknown',
    'Description' : 'Unknown',
    'Gender' : 'Unknown',
    'Marital_Status' : 'Unknown'
})

# drop the missing values in the last_updated column
df_clean = df_clean.na.drop(subset=['Last_Updated'])


# Data Transformation to 2NF
#transaction table
transaction = df_clean.select('Transaction_Date', 'Amount', 'Transaction_Type') \
                    .withColumn('transaction_id', monotonically_increasing_id()) \
                    .select('transaction_id','Transaction_Date', 'Amount', 'Transaction_Type')

# Cutsomer Table
customer = df_clean.select('Customer_Name', 'Customer_Address', 'Customer_City', 'Customer_State', 'Customer_Country') \
                .withColumn('customer_id', monotonically_increasing_id()) \
                .select('customer_id', 'Customer_Name', 'Customer_Address', 'Customer_City', 'Customer_State', 'Customer_Country')

employee = df_clean.select('Company','Job_Title','Email','Phone_Number', 'Gender', 'Marital_Status') \
                .withColumn('employee_id', monotonically_increasing_id()) \
                .select('employee_id','Company','Job_Title','Email','Phone_Number', 'Gender', 'Marital_Status')


# Nuga Bank Fact Table
fact_table = df_clean.join(transaction, ['Transaction_Date', 'Amount', 'Transaction_Type'], 'inner') \
                                .join(customer, ['Customer_Name', 'Customer_Address', 'Customer_City', 'Customer_State', 'Customer_Country'], 'inner') \
                                .join(employee, ['Company','Job_Title','Email','Phone_Number', 'Gender', 'Marital_Status'], 'inner') \
                                .select('customer_id','transaction_id','employee_id','IBAN','Currency_Code','Random_Number','Category','Group','Is_Active','Last_Updated','Description')

# Data loading
def get_db_connection():
    connection = psycopg2.connect(
        host = 'localhost',
        database = 'nuga_banks',
        user = 'postgres',
        password = 'Nonsosky@1'
    )
    return connection

# connect to database
conn = get_db_connection()

# Create a function create tables
def create_table():
    conn = get_db_connection()
    cursor = conn.cursor()
    create_table_query = '''
                        DROP TABLE IF EXISTS customer;
                        DROP TABLE IF EXISTS transaction;
                        DROP TABLE IF EXISTS employee;
                        DROP TABLE IF EXISTS fact_table;


                        CREATE TABLE IF NOT EXISTS customer(
                            customer_id BIGINT,
                            Customer_Name VARCHAR(10000),
                            Customer_Address VARCHAR(10000),
                            Customer_City VARCHAR(10000),
                            Customer_State VARCHAR(10000),
                            Customer_Country VARCHAR(10000)
                        );

                        CREATE TABLE IF NOT EXISTS transaction(
                            transaction_id BIGINT,
                            Transaction_Date DATE,
                            Amount FLOAT,
                            Transaction_Type VARCHAR(1000)
                        );

                        CREATE TABLE IF NOT EXISTS employee(
                            employee_id BIGINT,
                            Company VARCHAR(10000),
                            Job_Title VARCHAR(10000),
                            Email VARCHAR(10000),
                            Phone_Number VARCHAR(10000),
                            Gender VARCHAR(100),
                            Marital_Status VARCHAR(1000)
                        );

                        CREATE TABLE IF NOT EXISTS fact_table(
                            customer_id BIGINT,
                            transaction_id BIGINT,
                            employee_id BIGINT,
                            IBAN VARCHAR(10000),
                            Currency_Code VARCHAR(10000),
                            Random_Number FLOAT,
                            Category VARCHAR(10000),
                            "Group" VARCHAR(10000),
                            Is_Active VARCHAR(1000),
                            Last_Updated DATE,
                            Description VARCHAR(1000)
                        );

                        '''
    cursor.execute(create_table_query)
    conn.commit()
    cursor.close()
    conn.close()

url = "jdbc:postgresql://localhost:5432/nuga_banks"
properties = {
    "user" : "postgres",
    "password" : "Nonsosky@1",
    "driver" : "org.postgresql.Driver"
}

customer.write.jdbc(url=url, table="customer",  mode="append", properties=properties)
employee.write.jdbc(url=url, table="employee",  mode="append", properties=properties)
transaction.write.jdbc(url=url, table="transaction",  mode="append", properties=properties)
fact_table.write.jdbc(url=url, table="fact_table",  mode="append", properties=properties)

print('database, table and data loaded successfully')
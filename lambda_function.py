import json
import boto3
import pandas as pd
from io import BytesIO
import pymysql

print('Loading function')

s3 = boto3.client('s3')
DB_HOST = 'host db'
DB_USER = 'user db'
DB_PASSWORD = 'password db'
DB_NAME = 'name db'

def lambda_handler(event, context):
    print("event: ", event)

    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    print("filename triggerS3Upload: ", key)

    response = s3.get_object(Bucket=bucket, Key=key)
    excel_data = pd.read_excel(BytesIO(response['Body'].read()), header=0)

    try:
        connection = pymysql.connect(host=DB_HOST, user=DB_USER, password=DB_PASSWORD, db=DB_NAME)
        cursor = connection.cursor()

        for index, row in excel_data.iterrows():
            insert_query = "INSERT INTO products (`name`, `price`) VALUES (%s, %s)"
            cursor.execute(insert_query, (row['product_name'], row['price']))

        connection.commit()

        cursor.execute('SELECT * FROM products')

        # Fetch the column names
        columns = [desc[0] for desc in cursor.description]

        # Fetch all rows
        rows = [dict(zip(columns, row)) for row in cursor.fetchall()]

        print("data products: ", rows)
        return {
            'statusCode': 200,
            'body': {
                "data": rows
            }
        }
    except Exception as e:
        print(f"Error: {str(e)}")
        return {
            'statusCode': 500,
            'body': "Error database"
        }
    finally:
        cursor.close()
        connection.close()
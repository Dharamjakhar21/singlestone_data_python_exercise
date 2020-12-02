import os
import io
import boto3
import pandas as pd
from json import dump
from dotenv import load_dotenv

load_dotenv()


def process_data(students, teachers):
    result = []
    for _, row in teachers.iterrows():
        student_data = []
        for _, _row in students[students['cid'] == row['cid']].iterrows():
            student_data.append({
                'student_id': _row['id'],
                'student_name': _row['fname'] + ' ' + _row['lname'],
                'email': _row['email'],
                'ssn': _row['ssn'],
                'address': _row['address']
            })
        result.append({
            'class_id': row['cid'],
            'teacher_id': row['id'],
            'teacher_name': row['fname'] + ' ' + row['lname'],
            'students': student_data
        })

    try:
        with open('output.json', 'w') as outfile:
            dump(result, outfile, indent=4)
        print('output.json file generated successfully')
    except Exception as ex:
        print('Unable to create output.json')


def read_files(file_path, n):
    print('Processing...')
    students = teachers = None
    if n == 1:
        for path in file_path:
            if path.endswith('.parquet'):
                teachers = pd.read_parquet(path, engine='pyarrow')
            elif path.endswith('.csv'):
                students = pd.read_csv(path, delimiter='_')
    elif n == 2:
        try:
            s3 = boto3.client('s3',
                              region_name=os.getenv('AWS_REGION_NAME'),
                              aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
                              aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
                              )
            for path in file_path:
                bucket = path.split('/')[0]
                file_name = path.replace(bucket + '/', '')
                obj = s3.get_object(Bucket=bucket, Key=file_name)
                if path.endswith('.parquet'):
                    teachers = pd.read_parquet(io.BytesIO(obj['Body'].read()), engine='pyarrow')
                elif path.endswith('.csv'):
                    students = pd.read_csv(io.BytesIO(obj['Body'].read()), delimiter='_')
        except Exception as err:
            print("Something went wrong, please check file paths and credentials")
            print(err)
    process_data(students, teachers)


if __name__ == "__main__":

    try:
        print("Select the file storage from the options given below :\n")
        print("1. Local file storage.\n2. AWS S3\n")
        n = int(input(">> "))
        print()
        file_path=[]
        if n == 1:
            file_path = ["./dataset/students.csv", "./dataset/teachers.parquet"]
        elif n == 2:
            print("Enter complete S3 file path, eg:- bucket/folder/students.csv")
            students = input("path for students file : ")
            teachers = input("path for teachers file : ")
            file_path = [students, teachers]
            if not teachers.endswith('.parquet') or not students.endswith('.csv'):
                print("Entered invalid paths, please try again.")
                exit(0)
        else:
            print("Invalid selection, please try again.")
            exit(0)
        read_files(file_path, n)
    except Exception as ex:
        print('Files not found, please check file paths and try again')

import sys
import shutil,os
import requests, zipfile, io
from utils.db_connect import conn
import pandas as pd
from sqlalchemy import create_engine


class raw_data():
    def __init__(self):
        self.data = {
            'pg': {'database': 'postgres',
                   'user': 'postgres',
                   'password': '123',
                   'host': 'localhost',
                   'port': '5432'},
            'downl_loc': r'C:\Users\namit\Downloads\test',
            'downl_url': r'https://content.explore-education-statistics.service.gov.uk/api/releases/cfb27ea6-5078'
                         r'-4329-b59d-38ac104806dc/files',
            'files_to_load': ['school-capacity_200910-202223.csv',
                              'capacity_200910-202223.csv'],
            'int_type_conv': {'school-capacity_200910-202223.csv': ['primary_capacity', 'secondary_capacity'],
                              'capacity_200910-202223.csv': []},
            'table_name': {'school-capacity_200910-202223.csv': 'school_capacity',
                           'capacity_200910-202223.csv': 'agg_table'},
            'df_size': []
        }

    def download_data(self):
        try:
            print("Starting Download")
            shutil.rmtree(self.data['downl_loc'])
            os.makedirs(self.data['downl_loc'])
            request_downl = requests.get(self.data['downl_url'])
            z = zipfile.ZipFile(io.BytesIO(request_downl.content))
            z.extractall(self.data['downl_loc'])
            print("Download Completed Successfully")
        except Exception as Err:
            print(f"Download Failed Due to {Err}")

    def validate(self, file_name):
        try:
            print('Validation Started')
            query = f"select count(*) from information_schema.columns where table_name='{self.data['table_name'][file_name]}'";
            self.data['pg_conn'].execute(query)
            col_count = self.data['pg_conn'].fetchall()

            query = f"select count(*) from {self.data['table_name'][file_name]}"
            self.data['pg_conn'].execute(query)
            row_count = self.data['pg_conn'].fetchall()

            if row_count[0][0] == self.data['df_size'][0] and col_count[0][0] == self.data['df_size'][1]:
                print("Validation complted")
            else:
                print(f"validation failed for {file_name}")
                raise Exception(
                    f"validation failed due to mismatch in row and col count \n csv row count :{self.data['df_size'][0]} \n csv col count :{self.data['df_size'][1]} \n table row count : {row_count[0][0]}\n table col count : {col_count[0][0]}")
        except Exception as err:
            print(f"Validation Failed Due to {err}")

    def push_data(self):
        try:
            for file in self.data['files_to_load']:
                print(f'loading {file}')
                df = pd.read_csv(self.data['downl_loc'] + '\\data\\' + file)
                self.data['df_size'] = list(df.shape)
                conv = self.data['int_type_conv'].get(file, [])
                for type_conv in conv:
                    df[type_conv] = df[type_conv].replace(to_replace='z', value=0)
                    df[type_conv] = df[type_conv].astype(int)
                engine = create_engine(
                    f'postgresql://{self.data['pg']['user']}:{self.data['pg']['password']}@{self.data['pg']['host']}/{self.data['pg']['database']}')
                df.to_sql(self.data['table_name'][file], engine, if_exists='replace', index=False)
                print('Upload Completed \n validating the table')
                self.validate(file)
        except Exception as err:
            print(f"Push_data Failed Due to {err}")

    def main(self):
        self.download_data()
        conn.est_conn(self)
        self.push_data()


if __name__ == '__main__':
    print("Begin fetching data")
    download_and_push = raw_data()
    download_and_push.main()
    print("Table are created")

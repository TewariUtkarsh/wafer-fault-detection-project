import sqlite3
import json
import csv
from more_itertools import numeric_range
import pandas as pd
from File_Operations.file import operations


class db_operations():

    def __init__(self, mdm_path=None, good_raw_path=None, bad_raw_path=None,archive_path=None , database_path=None, final_training_data_path=None, table_name=None):
        # self.path = "Training_Data\Training_Data_DB"
        
        # self.good_raw_path = "Training_Data\Raw_Training_Data\Good_Raw_Data" 
        self.good_raw_path = good_raw_path
        
        
        # self.bad_raw_path = "Training_Data\Raw_Training_Data\Bad_Raw_Data"
        self.bad_raw_path = bad_raw_path
        
        # self.mdm_path = "Client_Data/master_data_manage.json"
        self.mdm_path = mdm_path

        self.archive_path = archive_path
        
        # self.database_path = "Training_Data/Training_Data_DB/"
        self.database_path = database_path
        
        # self.final_training_data_path = "Training_Data/Final_Training_Data/InputFile.csv"
        self.final_training_data_path = final_training_data_path
        
        # self.table_name = "Good_Raw_data"
        self.table_name = table_name
        
        self.file_operation_object = operations(self.good_raw_path, self.bad_raw_path, self.archive_path)
        
    # Func1: initiate connect and create db
    def initConnection(self, database):
        
        conn = sqlite3.connect(database=database)

        return conn
    
    # Func2: create table 
    def createTable(self, database):

        database = f"{self.database_path}{database}.db"
        
        with open(self.mdm_path, 'r') as f:
            columns = json.load(f)['ColName']

        conn = self.initConnection(database)

        curr = conn.cursor()

        curr.execute(f"select count(name) from sqlite_master where type='table' and name='{self.table_name}'")

        if curr.fetchone()[0] == 1:

            # Table Already Exists
            conn.close()
            pass


        else:

            # One method could be to create table the first time then alter it for every columns

            
            
            # Two Method could be to perform string transformation on the query to be passed, my method
            # More computational cost

            res_col = ''
            for col in columns:
                res_col = res_col + f"'{col}'" + f" {columns[col]}, "


            query = f"create table {self.table_name}({res_col[:-2]});"

            curr.execute(query)

            conn.commit()
            conn.close()





    # Func3: insert into table for good files
    def insertRecordsToTable(self, database):

        database = f"{self.database_path}{database}.db"

        conn = self.initConnection(database)
        curr = conn.cursor()

        files = self.file_operation_object.getFilesfromdir(self.good_raw_path)

        for file in files:

            file_path = self.good_raw_path + file

            with open(file_path, 'r') as f:

                next(f)

                reader = csv.reader(f, delimiter="\n")

                for i in reader:
                    # print(i[0])
                    curr.execute(f"insert into {self.table_name} values({i[0]});")
                    # break

            
        conn.commit()

        conn.close()
            

    # Func4: .db to csv
    def getInputFileCSV(self, database):

        database = self.database_path + database + '.db'

        conn = self.initConnection(database)

        # curr = conn.cursor()

        df = pd.read_sql(f"select * from {self.table_name}", conn)

        df.to_csv(self.final_training_data_path, index=None, header=True)

        conn.close()

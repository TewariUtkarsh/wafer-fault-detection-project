import json
from time import sleep
from Data_Validation.file_level_validation import validate_file
from Data_Validation.column_level_validation import validate_column
from Data_Transformation.data_transform import transformations
from Data_Preprocessing.data_processor import preprocess
from DB_Operations.db_oper import db_operations
from File_Operations.file import operations

class prediction_data_validation():

    def __init__(self, path, ) -> None:

        self.path = path
        self.good_raw_data = "Prediction_Data/Prediction_Validation_Data/Good_Prediction_Data/"
        self.bad_raw_data = "Prediction_Data/Prediction_Validation_Data/Bad_Prediction_Data/"
        self.prediction_mdm_path = "Prediction_Data/Prediction_Data_Files/prediction_schema.json"
        self.archive_path = "Prediction_Data/Archive_Prediction_Data/"
        self.database_path = "Prediction_Data/Prediction_Data_DB/"
        self.database_name = "Prediction"
        self.table_name = "Good_Prediction_Data"
        self.final_prediction_path = "Prediction_Data/Final_Prediction_Data/PredictionFile.csv"
        
        self.file_validation_object = validate_file(self.good_raw_data, self.bad_raw_data, self.archive_path)
        self.column_validation_object = validate_column(self.prediction_mdm_path, self.good_raw_data, self.bad_raw_data, self.archive_path)
        self.data_transformation_object = transformations(self.good_raw_data, self.bad_raw_data, self.archive_path)
        self.data_preprocessing_object = preprocess(self.good_raw_data, self.bad_raw_data, self.archive_path)
        self.db_operations_object = db_operations(self.prediction_mdm_path, self.good_raw_data, self.bad_raw_data, self.archive_path,self.database_path, self.final_prediction_path, self.table_name)
        self.file_operations_object = operations(self.good_raw_data, self.bad_raw_data, self.archive_path)



    def validate(self):
        

        # get regex, timestamp, date, col_num, col_name_dtype
        self.regex, self.timestamp_length, self.date_length, self.num_of_cols, self.colName = self.get_mdm_data()

        # validate file name (path, regex, timestamp, date) keep_source=true
        self.file_validation_object.validate_file_name(self.path, self.regex, self.timestamp_length, self.date_length)                    

        # validate column (col_num, name) keep_true=false
        self.column_validation_object.validate_column_name_number(self.num_of_cols, self.colName)
        
        
        # move to bad folder -> for all null cols keep_true=false
        self.data_transformation_object.checkNANcols()

        # rename unnamed col ot wafer and transform wafer col select only num cols
        self.data_transformation_object.colTransform()
        
        # fill np.Nan with NULL
        self.data_preprocessing_object.fillNAwithNULL()
        

        # create table and save db 
        self.db_operations_object.createTable(self.database_name)
        
        # insert into table 
        self.db_operations_object.insertRecordsToTable(self.database_name)
        
        
        # table to csv 
        self.db_operations_object.getInputFileCSV(self.database_name)
        


        # del good raw files
        self.file_operations_object.delExistingGoodRaw()
        

        # move bad raw to archive keep_source=false
        self.file_operations_object.archiveBadRaw()
        


    def get_mdm_data(self):

        # uses self.prediction_mdm_path
        with open(self.prediction_mdm_path, 'r') as f:

            mdm_data = json.load(f)

            return mdm_data['regex'], mdm_data['LengthOfTimeStampInFile'], mdm_data['LengthOfDateStampInFile'], mdm_data['NumberofColumns'], mdm_data['ColName']








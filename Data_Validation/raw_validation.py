from tkinter import E
from Data_Validation.file_level_validation import validate_file
from Data_Validation.column_level_validation import validate_column
from Data_Transformation.data_transform import transformations
from DB_Operations.db_oper import db_operations
from File_Operations.file import operations
from Data_Preprocessing.data_processor import preprocess
from Logging_Operations.app_logger import logger
import json


class raw_data_validation():
    
    """This class is responsible for initiating the data validation pipeline for 
    both training data and prediction data.


    Parameters
    ----------
    path : str
        The path to the Data Folder to be validated.
    
    Attributes
    ----------
    client_mdm_path : str
        The path to the Master Data Management File.
    good_raw_path : str
        The path to the Good Raw Data Folder.
    bad_raw_path : str
        The path to the Bad Raw Data Folder.
    final_training_data_path : str
        The path to the Good Raw Data Folder.
    archive_path : str
        The path to the Archival Folder for the Bad Raw Data.
    database_path : str
        The path to the Database File for the Good Raw Data.
    table_name : str
        The name of the table to be created for storing Validated Good Raw Data.
    """


    # path = "Client_Data/Batch_Training_Files/"
    def __init__(self, path) -> None:
        
        self.path = path
        self.client_mdm_path = "Client_Data/master_data_manage.json"
        
        self.good_raw_path = "Training_Data/Raw_Training_Data/Good_Raw_Data/"
        self.bad_raw_path = "Training_Data/Raw_Training_Data/Bad_Raw_Data/"
        
        self.archive_path = "Training_Data/Bad_Data_Archive/"
        
        self.final_training_data_path = "Training_Data/Final_Training_Data/InputFile.csv"
        
        self.database_path = "Training_Data/Training_Data_DB/"
        self.table_name = "Good_Raw_data"
        
        
        self.file_validation_object = validate_file(self.good_raw_path, self.bad_raw_path, self.archive_path)
        self.column_validation_object = validate_column(self.client_mdm_path, self.good_raw_path, self.bad_raw_path, self.archive_path)
        self.data_transform_object = transformations(self.good_raw_path, self.bad_raw_path, self.archive_path)
        self.db_operation_object = db_operations(self.client_mdm_path, self.good_raw_path, self.bad_raw_path,self.archive_path , self.database_path, self.final_training_data_path, self.table_name)
        self.file_opereration_object = operations(self.good_raw_path, self.bad_raw_path, self.archive_path)
        self.data_preprocess_object = preprocess(self.good_raw_path, self.bad_raw_path, self.archive_path)
        self.logger_object = logger("Log_Files/Training_Logs/Raw_Validation_Log.log", "DEBUG")



    def validate(self):
        """This function performs the step by step execution of the Data Validation Process.
        Each step in this function plays a significant role in the Validation Pipeline.

        Version : 1.0
        -------------
            Initial Version: 1.0

        """

        
        ## Beginning of Data Validation Process:
        self.logger_object.log("info", "Beginning of Raw Data Validation Process.")

        # Getting the Regex, Timestamp Length, Number of Columns and Column Details from MDM file for validation.
        self.regex, self.timestamp_length, self.date_length, self.noOfColumns, self.columnNames = self.get_MDM_data()

        # Calling validate_file_name() function to perform filename level validation.
        self.logger_object.log("info", "Beginning of Filename Validation Process.")
        self.file_validation_object.validate_file_name(self.path, self.regex,self.timestamp_length, self.date_length)

        # Calling validate_file_name() function to perform the column level validation.
        self.logger_object.log("info", "Beginning of Columns Level Validation Process.")
        self.column_validation_object.validate_column_name_number(self.noOfColumns, self.columnNames)
        
        self.logger_object.log("info", "Raw Data Validation Process Completed.")

        
        ## Beginning of Data Transformation Process:
        self.logger_object.log("info", "Beginning of Data Transformation Process.")

        # Checking files containing columns with all Nan values and moving them to Bad Raw Folder
        self.logger_object.log("info", "Checking files containing columns with all Nan values and moving them to Bad Raw Folder.")
        self.data_transform_object.checkNANcols()

        # Transforming the Wafer column by only storing the Wafer number and dropping the remaining string
        self.logger_object.log("info", "Transforming the Wafer column.")
        self.data_transform_object.colTransform()

        """#Test Phase
        # replace NAN values to str NULL as next step is move to db
        # self.data_transform_object.replaceNANvalues()
        """

        # Data Preprocessing Process:
        # Filling Nan locations with 'NULL' as when inputting Data in DB it won't recognize np.Nan
        self.logger_object.log("info", "Filling Nan locations with 'NULL'.")
        self.data_preprocess_object.fillNAwithNULL()

        self.logger_object.log("info", "Data Transformation Process Completed.")


        ## Database Operations:
        self.logger_object.log("info", "Beginning of Database Operations Process.")

        # Creating Table for storing our Final Good Raw Data together. First DB created by initiating a connction.
        self.logger_object.log("info", "Creating Table for Final Good Raw Data.")
        self.db_operation_object.createTable("Training")

        # Inserting data from each file from Good Raw Data folder one by one.
        self.logger_object.log("info", "Inserting Good Raw Data to the table created.")
        self.db_operation_object.insertRecordsToTable("Training")
        
        
        # Converting the table to .csv to generate InputFile.csv for further data processing.
        self.logger_object.log("info", "Extracting csv file for the table created.")
        self.db_operation_object.getInputFileCSV("Training")
        
        self.logger_object.log("info", "Database Operations Process Completed.")


        ## File Operations:
        self.logger_object.log("info", "Beginning of File Operations Process.")
        # Archiving the Bad Raw Data and deleting the Bad Raw Data Folder.
        self.logger_object.log("info", "Archiving Bad Raw Data.")
        self.file_opereration_object.archiveBadRaw()
        
        # Deleting the Good Raw Data Folder as we already have the InputFile.csv
        self.logger_object.log("info", "Deleting Good Raw Data.")
        self.file_opereration_object.delExistingGoodRaw()
        


    def get_MDM_data(self):
        """This function is responsible for extracting the MDM details: regex, 
        timestamp_length, data_length, noOfColumns and columnNames from the 
        mdm file path.

        Returns
        -------
        regex : str
            Regex pattern for the filename.
        timestamp_length : int
            Length of the timestamp in the filename.
        date_length : int
            Length of the data in the filename.
        noOfColumns : int
            Number of columns associated with each file.
        columnNames : dict
            Dictionary representing the details of each column.
            Keys showcasing the name of the column and respective
            Value showcasing the datatype of the column
        """
        
        try:
            # Creating mdm file object and loading the json for it.
            self.logger_object.log("info", "Loading json data from MDM file.")
            file = open(self.client_mdm_path, 'r')
            mdm = json.load(file)
            file.close()

            # Extracting the required details.
            regex = mdm['regex']
            timestamp_length = int(mdm['LengthOfTimeStampInFile'])
            date_length = int(mdm['LengthOfDateStampInFile'])
            noOfColumns = int(mdm['NumberofColumns'])
            columnNames = mdm['ColName']

            return regex, timestamp_length, date_length, noOfColumns, columnNames

        except Exception as e:
            # Error Occurred: 
            self.logger_object.log(f"error", "Error while loading Data from MDM file:\n{e}")
            raise e


            

from File_Operations.file import operations
from File_Operations.file import operations
from Logging_Operations.app_logger import logger
import pandas as pd


class validate_column():
    """This class is utility class for performing Column Level Validation.
    It consists of various utility functions for performing step by step
    validation process.

    Parameters
    ----------
    mdm_path : str, default=None
        Path to the Master Data Management File.
    good_raw_path : str, default=None
        Path to the Good Raw Directory.
    bad_raw_path : str, default=None
        Path to the Bad Raw Directory.
    archive_path : str, default=None
        Path to the Archival Location for Bad Raw Files.


    """
        
    def __init__(self, mdm_path=None, good_raw_path=None, bad_raw_path=None,archive_path=None) -> None:
        
        self.client_mdm_path = mdm_path
        self.good_raw_path = good_raw_path
        self.bad_raw_path = bad_raw_path
        self.archive_path = archive_path

        self.file_operation_object = operations(self.good_raw_path, self.bad_raw_path, self.archive_path)
        self.logger_object = logger("Log_Files/Training_Logs/Column_Level_Validation_Log.log", "DEBUG")

    """TEST PHASE
    # Func1: get no_of_col, colname from mdm
    def getColumnDetails(self):
    """

    # Func: Validate the Number of Columns in a particular file
    def validate_column_name_number(self, noOfColumns, columnNames):
        """This function performs the step by step execution of column level validation.
        By comparing the parameters mentioned in the MDM file with the training files.

        Parameters
        ----------
        noOfColumns : int
            Number of Columns with respct to each file.
        columnNames : dict
            Dictionary with Keys representing the name of the columns and
            Values representing the datatype of the respective column.

        """
        
        try:
            self.logger_object.log("info", "Beginning of Column Validation Process.")
            # Extracting files from the Good Raw Directory.
            path = self.good_raw_path
            files = self.file_operation_object.getFilesfromdir(path)
            
            # If file does not consists of valid number of column then
            # move it to the Bad Raw Directory else pass.
            for file in files:

                file_path = path + file

                df = pd.read_csv(file_path)

                if noOfColumns == df.shape[1]:
                    pass

                else:
                    self.file_operation_object.moveToBadRaw(file_path, keep_source=False)
        except Exception as e:
            # Error occurred
            self.logger_object.log("error", f"Error while Validating Columns: \n{e}")
            raise e

    # THE FUNCTIONS BELOW ARE PRESENT IN DATA PREPROCESSING PACKAGE UTILITY
    # Func3: validate col name: move to good and bad
    # Func4: Full NULL in cols: move to good and bad

    


import json
import re
from File_Operations.file import operations
from Logging_Operations.app_logger import logger

class validate_file():
    """This class is responsible for performing file level data validation.
    The steps simply involves validation of the filename and other attributes.

    Parameters
    ----------
    good_raw_path : str, default=None
        Path to the Good Raw Data Files.
    bad_raw_path : str, default=None
        Path to the Bad Raw Data Files.
    archive_path : str, default=None
        Path to the Bad Data Archival Directory.

    Attributes
    ----------
    good_raw_path : str, default=None
        Path to the Good Raw Data Files.
    bad_raw_path : str, default=None
        Path to the Bad Raw Data Files.
    archive_path : str, default=None
        Path to the Bad Data Archival Directory.

    """    

    def __init__(self, good_raw_path=None, bad_raw_path=None, archive_path=None) -> None:

        
        self.good_raw_path = good_raw_path
        self.bad_raw_path = bad_raw_path
        self.archive_path = archive_path

        self.file_operation_object = operations(self.good_raw_path, self.bad_raw_path, self.archive_path)
        self.logger_object = logger("Log_Files/Training_Logs/File_Level_Validation_Log.log", "DEBUG")
        


    """TEST PHASE
    # Func 1: get data from MDM (itmelength, datelength)
    def get_MDM_data(self):

        f = open(self.client_mdm_path, 'r')
        mdm = json.load(f)
        return mdm

    # Func 2: get file regex 
    def get_regex(self, mdm):

        regex = mdm['regex']
        return regex

    def get_timestamp_length(self, mdm):
        timestamp_length = mdm['LengthOfTimeStampInFile']
        return timestamp_length

    def get_date_length(self, mdm):
        date_length = mdm['LengthOfDateStampInFile']
        return date_length


    def get_file_details(self, mdm):
        
        regex = self.get_regex(mdm)
        timestamp_length = int(self.get_timestamp_length(mdm))
        date_length = int(self.get_date_length(mdm))

        return [regex, timestamp_length, date_length]
    """

    # Func: Validate file extension and pattern by matching regex(csv): move to good and bad folder respectively
    def validate_file_name(self, path, regex, timestamp_length, date_length):
        """This function performs the step by step execution of filename validation.
        By comparing the regex pattern of the file and comparing the timestamp and date length.

        Parameters
        ----------
        path : str
            Path to the Training Files.
        regex : str
            Regex pattern for validating the filename
        timestamp_length : int
            Length of the timestamp in the filename.
        date_length : int
            Length of the date in the filename

        """
        
        self.logger_object.log("info", "Beginning of Filename Validation Process.")
        self.logger_object.log("info", "Extracting the list of the training files from the path specified.")
        # Extracting the list of the training files from the path specified.
        train_files = self.file_operation_object.getFilesfromdir(path)
        
        # Deleting the existing Good Raw and Bad Raw Directories if they exist.
        self.logger_object.log("info", "Deleting the existing Good Raw and Bad Raw Directories if they exist.")
        self.file_operation_object.delExistingGoodRaw()
        self.file_operation_object.delExistingBadRaw()
        

        try: 
            ## Beginning of the Filename Validation Process.

            for file in train_files:

                file_path = path + file
                
                # regex = "['wafer']+['\_'']+[\d_]+[\d]+\.csv"
                # Matching the pattern. If match found then execute next step. Else Move file to Bad Raw Folder
                if re.match(regex, file):

                    first = re.split('.csv', file)[0]
                    first = re.split('_', first)

                    # Matching the Date Length. If not matched then move file to Bad Raw Folder.
                    if len(first[1]) == date_length:

                        # Matching Time Stamp Length.If match found then move file to Good Raw Folder else 
                        # move file to Bad Raw Folder.
                        if len(first[2]) == timestamp_length:
                            self.logger_object.log("info", f"Copying Valid File to Good Raw Data Folder: {file}")
                            self.file_operation_object.moveToGoodRaw(file_path, keep_source=True)

                        else:
                            self.logger_object.log("info", f"Copying Invalid File to Bad Raw Data Folder: {file}")
                            self.file_operation_object.moveToBadRaw(file_path, keep_source=True)

                    else:
                        self.logger_object.log("info", f"Copying Invalid File to Bad Raw Data Folder: {file}")
                        self.file_operation_object.moveToBadRaw(file_path, keep_source=True)
                    

                else:
                    self.logger_object.log("info", f"Copying Invalid File to Bad Raw Data Folder: {file}")
                    self.file_operation_object.moveToBadRaw(file_path, keep_source=True)

        except Exception as e:
            # Error occurred
            self.logger_object.log("error", f"Error while Validating Filename: \n{e}")
            raise e



import os, shutil
import pandas as pd
import pickle
from Logging_Operations.app_logger import logger


class operations():
    """This class is a file utility class.
    This class is responsible for performing all important important file level operations
    for proper working of the system.
    It contains various utility functions performing unit tasks.

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


    def __init__(self, good_raw_data=None, bad_raw_data=None, archive_path=None) -> None:
        
        self.good_raw_path = good_raw_data
        self.bad_raw_path = bad_raw_data
        self.archive_path = archive_path
        
        self.logger_object = logger("Log_Files/Training_Logs/File_Operations.log", "DEBUG")


    # Func: To Check if the Directory is present or not
    def checkDIR(self, path):
        """This function checks if the Directory is present or not for
        the given path passed as arguement.

        Parameters
        ----------
        path : str
            Path to the directory to be checked.
        
        Returns
        -------
        bool : True or False
            True: if the directory path exists.
            False: if the directory path doesn't exists.

        """
        try:
            self.logger_object.log("info", f"Checking if the directory exists for path: {path}")
            # Checks if the directory path exists, if yes then return True else return Flase
            if os.path.isdir(path):
                return True

            else:
                return False
        except Exception as e:
            # Error occurred
            self.logger_object.log("error", f"Error while checking if directory exists for path({path}):\n{e}")
            raise e


    # Func: Returns a list of files and folders present in the directory path passed
    def getFilesfromdir(self, path):
        """This function checks and returns a list of files and folders present in 
        the directory path passed.

        Parameters
        ----------
        path : str
            Path to the directory to be checked.
        
        Returns
        -------
        files : list
            List of the files and folders present in the directory path
            passed as an arguement.

        """ 

        try:
            self.logger_object.log("info", f"Extracting the list of files and folders present at path: {path}")
            files = []

            for i in os.listdir(str(path)):
                files.append(i)

            return files
        except Exception as e:
            # Error Occurred
            self.logger_object.log("error", f"Error while extracting files and folders from path({path}):\n{e}")
            raise e



    # Func: Checks if there exist a Good Raw Directory, if yes then delete
    # else pass.
    def delExistingGoodRaw(self):
        """This function Checks if there exist a Good Raw Directory, if yes then delete
        the existing directory else do nothing.


        """ 

        try:
            self.logger_object.log("info", f"Deleting Existing Good Raw Directory for path: {self.good_raw_path}")
            # It checks if the Good Raw Directory exists, if yes then
            # delete the existing directory else pass.
            if self.checkDIR(self.good_raw_path):
                shutil.rmtree(self.good_raw_path)

            else:
                pass
        except Exception as e:
            # Error occured
            self.logger_object.log("error", f"Error while Deleting Existing Good Raw Directory for path({self.good_raw_path}):\n{e}")
            raise e


    # Func: Checks if there exist a Bad Raw Directory, if yes then delete
    # else pass.
    def delExistingBadRaw(self):
        """This function Checks if there exist a Bad Raw Directory, if yes then delete
        the existing directory else do nothing.


        """ 

        try:
            self.logger_object.log("info", f"Deleting Existing Bad Raw Directory for path: {self.bad_raw_path}")
            # It checks if the Bad Raw Directory exists, if yes then
            # delete the existing directory else pass.
            if self.checkDIR(self.bad_raw_path):
                shutil.rmtree(self.bad_raw_path)

            else:
                pass
        except Exception as e:
            # Error occured
            self.logger_object.log("error", f"Error while Deleting Existing Bad Raw Directory for path({self.bad_raw_path}):\n{e}")
            raise e



    # Func: Moves the source file to the Good Raw Folder.
    def moveToGoodRaw(self, source, keep_source):
        """This function moves the source file to the Good Raw Folder based on 
        the arguements passed.

        Parameters
        ----------
        source : str
            Path of the source file to be moved.
        keep_source : bool
            True: Copes the original source file. Original copy of the source file is untouched
            False: Moves the original copy of the source file.

        """
        try:
            self.logger_object.log("info", f"Moving source file({source}) to Good Raw Directory({self.good_raw_path})")
            # Checking if Good Raw Directory exists and creating if not present.
            self.createGoodRaw() 

            # Based on the arguement performing the operations.
            if keep_source == True:
                shutil.copy(source, self.good_raw_path)

            elif keep_source == False:
                shutil.copy(source, self.good_raw_path)
                os.remove(source)
        except Exception as e:
            # Error Occurred
            self.logger_object.log("error", f"Error while Moving source file({source}) to Good Raw Directory({self.good_raw_path}):\n{e}")
            raise e


    # Func: Moves the source file to the Bad Raw Folder.
    def moveToBadRaw(self, source, keep_source):
        """This function moves the source file to the Bad Raw Folder based on 
        the arguements passed.

        Parameters
        ----------
        source : str
            Path of the source file to be moved.
        keep_source : bool
            True: Copes the original source file. Original copy of the source file is untouched
            False: Moves the original copy of the source file.

        """

        try:
            self.logger_object.log("info", f"Moving source file({source}) to Bad Raw Directory({self.bad_raw_path})")
            # Checking if Bad Raw Directory exists and creating if not present.
            self.createBadRaw()

            # Based on the arguement performing the operations.
            if keep_source == True:
                shutil.copy(source, self.bad_raw_path)

            elif keep_source == False:
                shutil.copy(source, self.bad_raw_path)
                os.remove(source)
        except Exception as e:
            # Error Occurred
            self.logger_object.log("error", f"Error while Moving source file({source}) to Bad Raw Directory({self.bad_raw_path}):\n{e}")
            raise e


    # Func: Checks if Good Raw Directory exists and create directory if not present.
    def createGoodRaw(self):
        """This function Checks if the Good Raw Directory exists and creates thr
        directory if not present else pass.

        """
        
        try:
            self.logger_object.log("info", f"Creating Good Raw Directory({self.good_raw_path})")
            # Based on the presence of the directory performing the operations.
            if self.checkDIR(self.good_raw_path):
                pass

            else:
                os.makedirs(self.good_raw_path)
        except Exception as e:
            # Error Occurred
            self.logger_object.log("error", f"Error while Creating Good Raw Directory({self.good_raw_path}):\n{e}")
            raise e



    # Func: Checks if Bad Raw Directory exists and create directory if not present.
    def createBadRaw(self):
        """This function Checks if the Bad Raw Directory exists and creates thr
        directory if not present else pass.

        """
        try:
            self.logger_object.log("info", f"Creating Bad Raw Directory({self.bad_raw_path})")
            # Based on the presence of the directory performing the operations.
            if self.checkDIR(self.bad_raw_path):
                pass

            else:
                os.makedirs(self.bad_raw_path)
        except Exception as e:
            # Error Occurred
            self.logger_object.log("error", f"Error while Creating Bad Raw Directory({self.bad_raw_path}):\n{e}")
            raise e



    """TEST PHASE
    # Func: 
    def getDFfromDIR(self, path):
        files = self.getDFfromDIR(path)

        for file in files:

            file_path = self.good_raw_path + file

            df = pd.read_csv(file_path)        

        return df
    """
    
    # Func: Archive Bad Raw Files by moving them to archival location
    def archiveBadRaw(self):
        """This function moves all the files present in the Bad Raw Directory to
        the specified Archival Directory location.
        NOTE: Original files won't exist as they are moved.

        """

        try:
            self.logger_object.log("info", f"Archiving Bad Raw Directory({self.bad_raw_path}) files to path: {self.archive_path}")
            # Getting Bad Raw Files from path
            files = self.getFilesfromdir(self.bad_raw_path)

            # Moving each file to the archival location one by one.
            for file in files:

                file_path = self.bad_raw_path + file

                shutil.move(file_path, self.archive_path)
                # os.remove(file_path)

            # Deleting the Bad Raw Directory after moving the files.
            shutil.rmtree(self.bad_raw_path)  
        except Exception as e:
            # Error Occurred
            self.logger_object.log("error", f"Error while Archiving Bad Raw Directory({self.bad_raw_path}) files to path({self.archive_path}):\n{e}")
            raise e
    
    
    # Func: Saving the required model
    def save_model(self, model_name, model, path):
        """This function is responsible for saving the desired model in the form of
        serialized pickle format to the desired location.

        Parameters
        ----------
        model_name : str
            String specifying the name along with the extension of the model.
        model : model instance/object
            Instance/object of the model to be saved/dumped.
        path : str
            Path at which the model needs to be stored/dumped.

        """
        try:
            self.logger_object.log("info", f"Saving model({model_name}) to path({path})")
            # Saves the model to the desired location
            with open(path + model_name, 'wb') as f:
                pickle.dump(model, f)
        except Exception as e:
            # Error Occurred
            self.logger_object.log("error", f"Error while Saving model({model_name}) to path({path}):\n{e}")
            raise e
    

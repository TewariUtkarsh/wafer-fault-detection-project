from numpy import ndarray
import pandas as pd
from File_Operations.file import operations

class preprocess():

    def __init__(self, good_raw_path=None, bad_raw_path=None, archive_path=None) -> None:
        # self.good_raw_path = "Training_Data\Raw_Training_Data\Good_Raw_Data"
        self.good_raw_path = good_raw_path

        # self.bad_raw_path = "Training_Data\Raw_Training_Data\Bad_Raw_Data"
        self.bad_raw_path = bad_raw_path

        self.archive_path = archive_path

        self.file_operation_object = operations(self.good_raw_path, self.bad_raw_path, self.archive_path)

    # Func: Replace NAN values with NULL as going to store in DB
    def fillNAwithNULL(self):
        
        files = self.file_operation_object.getFilesfromdir(self.good_raw_path)

        for file in files:

            file_path = self.good_raw_path + file

            df = pd.read_csv(file_path)

            df.fillna("NULL", inplace=True)

            df.to_csv(file_path, index=None, header=True)
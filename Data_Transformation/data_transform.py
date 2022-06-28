import pandas as pd
from File_Operations.file import operations

class transformations():

    def __init__(self, good_raw_path=None, bad_raw_path=None, archive_path=None) -> None:
        # self.good_raw_path = "Training_Data\Raw_Training_Data\Good_Raw_Data" 
        self.good_raw_path = good_raw_path

        # self.bad_raw_path = "Training_Data\Raw_Training_Data\Bad_Raw_Data"
        self.bad_raw_path = bad_raw_path

        self.archive_path = archive_path
        
        self.file_operation_object = operations(self.good_raw_path, self.bad_raw_path, self.archive_path)

    
    # Func: mov to bad raw for files with all nan values
    def checkNANcols(self):

        files = self.file_operation_object.getFilesfromdir(self.good_raw_path)

        for file in files:

            file_path = self.good_raw_path + file
            df = pd.read_csv(file_path)

            cols = df.columns
            for col in cols:
                # if (len(df[col]) - df[col].count()) == len(df[col]):
                if df[col].count() == 0:                    
                    self.file_operation_object.moveToBadRaw(file_path, keep_source=False)
                    break

    # Func: transform wafer col
    def colTransform(self):
        
        files = self.file_operation_object.getFilesfromdir(self.good_raw_path)

        for file in files:
            
            file_path = self.good_raw_path + file
            df = pd.read_csv(file_path)
            # df.columns[0] = "Wafer"
            df.rename(columns={"Unnamed: 0": "Wafer"}, inplace=True)
            df['Wafer'] = df['Wafer'].str[6:]
            df.to_csv(file_path, index=None)


            

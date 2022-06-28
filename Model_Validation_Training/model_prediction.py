import pandas as pd
from Model_Validation_Training.model_utility import model_utility
from Data_Preprocessing.data_processor import preprocess


class prediction_operations():

    def __init__(self) -> None:
        self.prediction_file_path = "Prediction_Data/Final_Prediction_Data/PredictionFile.csv"
        self.prediction_outputfile_path = "Prediction_Data/Prediction_Output_Data/PredictionOutputFile.csv"

        self.model_utility_object = model_utility()
        self.data_preprocessing_object = preprocess()

    def predict(self):
        
        # DATA PROCESSING (only x present)

        # load file as df
        df = pd.read_csv(self.prediction_file_path)
        self.columns = df.columns

        # drop wafer col
        # df = self.model_utility_object.dropColumns(data=df, columns=['Wafer'])

        # fill null values
        df = self.model_utility_object.imputeMissingValues(df)
        

        # find cols with 0 sd
        cols_to_drop = self.model_utility_object.zeroSDcolumns(df)

        # drop col with 0 sd
        df = self.model_utility_object.dropColumns(df, cols_to_drop)
        
        # get cluster by loading model 
        num_of_cluster, cluster = self.model_utility_object.getClusterforPredictData(data=df.drop(['Wafer'], axis=1))
        df['Cluster'] = cluster

        # for each cluster: find suitable mapped model
        for i in range(num_of_cluster):
            
            cluster_data = df[df['Cluster']==i]
            wafer_data = cluster_data['Wafer']

            cluster_data = df[df['Cluster']==i].drop(columns=['Wafer'])
            cluster_data = cluster_data.drop(columns=['Cluster'])

            model_name, cluster_model = self.model_utility_object.getSuitableModel(i)
            cluster_prediction = cluster_model.predict(cluster_data)

            cluster_data['Wafer'] = wafer_data
            cluster_data['Prediction'] = cluster_prediction

            cluster_data.to_csv(self.prediction_outputfile_path,index=None, header=True, mode='a+')




        # predict and dump to df then to csv

        
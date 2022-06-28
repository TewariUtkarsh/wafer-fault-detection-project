import pandas as pd
from sklearn import cluster
from sqlalchemy import column
from Model_Validation_Training.model_utility import model_utility
from File_Operations.file import operations
import pickle


class training_operations():

    def __init__(self):
        # self inputfile path
        self.input_file_path = "Training_Data/Final_Training_Data/InputFile.csv"
        self.preprocessed_file_path = "Training_Data/Final_Training_Data/PreprocessedInputFile.csv"
        self.label_column = "Output"
        self.model_dir_path = "Models/"
        self.model_utility_object = model_utility()
        self.file_operations_object = operations()
        pass

    def train_model(self):


        
        # PREPROCESS:

        # load file as df
        df = pd.read_csv(self.input_file_path)

        # x,y split after dropping wafer col
        X, Y = self.model_utility_object.feature_label_selection(df, self.label_column)
        
        # knn impute
        X = self.model_utility_object.imputeMissingValues(X)
        
        # Get cols with 0 SD
        columns_to_drop = self.model_utility_object.zeroSDcolumns(X)

        # drop 0 sd col
        X = self.model_utility_object.dropColumns(X, columns_to_drop)
        
        
        df = X.copy(deep=True)
        df[self.label_column] = Y
        df.to_csv(self.preprocessed_file_path, index=None, header=True)
        

        ## CLUSTER:

        # kmean = best model from utility using x
        n_cluster, kmeans = self.model_utility_object.getBestKMeansModel(X)


        # clustering our data
        y_cluster_output = kmeans.fit_predict(X)


        # Saving KMeans model
        self.file_operations_object.save_model("KMeans.sav", kmeans, self.model_dir_path)
        

        # noc = clust.uniq
        unique_clusters = list(set(y_cluster_output))
        

        # combine x,y,labels
        X['Label'] = Y
        X['Cluster'] = y_cluster_output

        ## MODEL CREATION AND SELECTION

        for i in unique_clusters:

            # data = X[X['Cluster']==i].drop(columns=['Cluster'])
            data = self.model_utility_object.dropColumns(X[X['Cluster']==i], ['Cluster'])

            # cluster_features = data.drop(columns=['Label'])
            cluster_features = self.model_utility_object.dropColumns(data, ['Label'])
            cluster_label = data['Label']

            model_name, model_object = self.model_utility_object.getBestModel(cluster_features, cluster_label)

            # Saving the best model
            model_name = f"{model_name}{i}.sav"
            self.file_operations_object.save_model(model_name, model_object, self.model_dir_path)
            
        pass
        
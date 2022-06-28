import os
import pandas as pd
import numpy as np
import pickle
from sklearn import cluster
from sklearn.cluster import KMeans 
from sklearn.model_selection import GridSearchCV, train_test_split
from sklearn.impute import KNNImputer
from kneed import KneeLocator
from xgboost import XGBClassifier
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, roc_auc_score
import matplotlib.pyplot as plt


class model_utility():

    def __init__(self):
        pass


    def feature_label_selection(self, data, label):

        X = data.drop(columns=[label, 'Wafer'])
        Y = data[label]

        return X,Y

    
    def imputeMissingValues(self, data):

        imputor = KNNImputer(missing_values=np.nan, n_neighbors=3, weights="uniform")
        new_data = imputor.fit_transform(data)

        new_data = pd.DataFrame(new_data, columns=data.columns)

        return new_data


    def zeroSDcolumns(self, data):

        std = data.describe()[data.describe().index == 'std'].transpose()
        
        zero_std_cols = std[std['std'] == 0].index
        
        return zero_std_cols

    def dropColumns(self, data, columns):
        
        new_data = data.drop(columns=columns)

        return new_data


    def get_train_test(self, X, Y):

        x_train, x_test, y_train, y_test = train_test_split(X, Y, test_size=0.25, random_state=42)

        return x_train, x_test, y_train, y_test



    def getBestKnee(self, wcss, cluster):

        
        

        knee_loc_object = KneeLocator(cluster, wcss, curve="convex", direction="decreasing")
        elbow_value = knee_loc_object.elbow
        
        # Plotting the elbow plot for the KNN model.
        plt.plot(cluster, wcss, marker='o', linestyle='--') 
        wcss_for_elbow = wcss[cluster.index(elbow_value)] 
        plt.plot((0,elbow_value), (wcss_for_elbow, wcss_for_elbow), marker='o', linestyle='--', color='red') 
        plt.plot((elbow_value, elbow_value),(wcss_for_elbow, 0), marker='o', linestyle='--', color='red')
        plt.title("Elbow Plot(Number of Clusters VS WCSS)") 
        plt.xlabel("Number of Clusters") 
        plt.ylabel("WCSS")
        plt.savefig("static/img/Elbow_Plot")
        return knee_loc_object.elbow


    def getBestKMeansModel(self, data):

        wcss = []
        cluster = [i for i in range(1, 16)] 
        
        for i in cluster:

            kmeans = KMeans(n_clusters=i, init='k-means++', n_init=10, random_state=42)
            kmeans.fit(data)
            wcss.append(kmeans.inertia_)
        

        best_knee_value = self.getBestKnee(wcss, cluster)

        kmeans = KMeans(n_clusters=best_knee_value, init='k-means++', n_init=10, random_state=42)

        return best_knee_value, kmeans



    ### MODELS

    # Performs GridSearchCV
    def bestParamXGB(self, X, Y):

        param_grid = {
            'learning_rate': [0.5, 0.1, 0.01, 0.001],
            'max_depth': [3, 5, 10, 20],
            'n_estimators': [10, 50, 100, 200]
        }

        
        xgb_grid = GridSearchCV(estimator=XGBClassifier(objective="binary:logistic", ), param_grid=param_grid, cv=5)

        xgb_grid.fit(X, Y)

        xgb_best_params = xgb_grid.best_params_

        xgb_learning_rate = xgb_best_params['learning_rate']
        xgb_max_depth = xgb_best_params['max_depth']
        xgb_n_estimators = xgb_best_params['n_estimators']

        xgb = XGBClassifier(objective='binary:logistic', learning_rate=xgb_learning_rate, max_depth=xgb_max_depth, n_estimators=xgb_n_estimators)
        xgb.fit(X, Y)
        return xgb
        

        


    # Performs GridSearchCV
    def bestParamRF(self, X, Y):
        
        param_grid = {
            "n_estimators": [10, 50, 100, 130], 
            "criterion": ['gini', 'entropy'],
            "max_depth": range(2, 4, 1), 
            "max_features": ['auto', 'log2']
        }
        
        rf = RandomForestClassifier()
        rf_grid = GridSearchCV(estimator=rf, param_grid=param_grid, cv=5)

        rf_grid.fit(X, Y)

        rf_best_param = rf_grid.best_params_

        rf_n_estimators = rf_best_param["n_estimators"]
        rf_criterion=rf_best_param["criterion"]
        rf_max_depth=rf_best_param["max_depth"]
        rf_max_features=rf_best_param["max_features"]

        rf = RandomForestClassifier(n_estimators=rf_n_estimators, criterion=rf_criterion, max_depth=rf_max_depth, max_features=rf_max_features)
        rf.fit(X, Y)
        return rf


    # Get best params first then train model and compare accuracy/roc_auc_score for best model
    def getBestModel(self, X, Y):
        
        x_train, x_test, y_train, y_test = self.get_train_test(X, Y)

        xgb_final = self.bestParamXGB(x_train, y_train)
        rf_final = self.bestParamRF(x_train, y_train)

        # Temporary Storing these models so that I will not need to run the above code again and again

        # pickle.dump(xgb_final, open("Test/xgb_final.sav", 'wb'))
        # pickle.dump(rf_final, open("Test/rf_final.sav", 'wb'))

        xgb_y_pred = xgb_final.predict(x_test)
        rf_y_pred = rf_final.predict(x_test)

        if len(y_train.unique()) == 1:
            # Accuracy Score comparison as roc_auc_score does not work with single class
            xgb_score = accuracy_score(y_test, xgb_y_pred)
            rf_score = accuracy_score(y_test, rf_y_pred)

            
        else:
            # roc_auc_score comparison
            xgb_score = roc_auc_score(y_test, xgb_y_pred)
            rf_score = roc_auc_score(y_test, rf_y_pred)


        if xgb_score > rf_score:
            return "XGBClassifier", xgb_final

        else:
            return "RandomForestClassifier", rf_final



    # PREDICTION PIPELINE

    # Func: loading model
    def load_model(self, model_name=None, model_path=None, cluster_num=None):

        if model_name == "KMeans":
            with open(model_path, 'rb') as f:
                model = pickle.load(f)
            return model  

        else:
            models = os.listdir(model_path)
            for i in models:
                if str(cluster_num) in i:
                    with open(f"{model_path}{i}", 'rb') as f:
                        cluster_model = pickle.load(f)
                        model_name = i[:-5]
                        return model_name, cluster_model

                      

    # Func: get cluster for prediction data
    def getClusterforPredictData(self, data):

        self.kmeans_model_name = "KMeans"
        self.kmeans_model_path = "Models/KMeans.sav"

        model = self.load_model(self.kmeans_model_name, self.kmeans_model_path)

        cluster = model.predict(data)

        # data['Cluster'] = cluster
        num_of_cluster = len(np.unique(cluster))
        return num_of_cluster, cluster


    # Func: get suitable model for respective cluster
    def getSuitableModel(self, cluster_num):

        model_name, model = self.load_model(model_path="Models/",cluster_num=cluster_num)

        return model_name, model



        


from flask import Flask, render_template, request, Response, jsonify
from flask_cors import CORS, cross_origin

from Data_Validation.raw_validation import raw_data_validation
from Model_Validation_Training.model_training import training_operations

from Data_Validation.prediction_validation import prediction_data_validation
from Model_Validation_Training.model_prediction import prediction_operations
import logging

"""
Logging:

Info ->
Parameters:
Attributes:
Returns:
----------------------------------------------
Info-> Gets and prints the spreadsheet's header columns

Parameters
----------
file_loc : str, default=None
    The file location of the spreadsheet
print_cols : bool, optional
    A flag used to print the columns to the console (default is False)

Attributes
----------
x : dataframe
    This variable represents this and that
y : dataframe
    This variable represents this and that

Returns
-------
list :
    a list of strings representing the header columns


"""


app = Flask(__name__)
CORS(app)

@app.route("/", methods=['POST', 'GET'])
@cross_origin()
def index():

    return render_template('index.html')


@app.route("/train", methods=['POST', 'GET'])
@cross_origin()
def train():

    # train_file_path = request.form['csvfile']
    # return str(train_file_path)
    # Training Pipeline:
    
    train_file_path = "Client_Data/Batch_Training_Files/"

    train_validation_obj = raw_data_validation(train_file_path)
    train_validation_obj.validate()

    training_obj = training_operations()
    training_obj.train_model()
    
    
    # Prediction Pipeline:
    
    predict_file_path = "Prediction_Data/Prediction_Data_Files/Prediction_Batch_Files/"


    predict_validation_object = prediction_data_validation(predict_file_path)
    predict_validation_object.validate()

    
    prediction_object = prediction_operations()
    prediction_object.predict()
    
    logging.shutdown()
    
    return render_template('index.html')


if __name__ == '__main__':

    app.run(debug=True)





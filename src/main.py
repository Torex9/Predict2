from appwrite.client import Client
from appwrite.services.databases import Databases
from appwrite.services.storage import Storage
from appwrite.exception import AppwriteException
from appwrite.query import Query
import os
import joblib
import numpy as np
import logging
from datetime import datetime

logging.basicConfig(level=logging.DEBUG)




def fetch_file_from_storage(context, storage, bucket_id, file_id, local_path):
    """
    Download a file from Appwrite Storage and save it locally.
    """
    try: 
        # Try to download the file
        response = storage.get_file_download(bucket_id=bucket_id, file_id=file_id)

        # Save the file locally
        with open(local_path, "wb") as f:
            f.write(response)
        # Log the file path
        context.log(f"File saved to: {local_path}")
        return local_path
    except Exception as e: 
        context.error(f"Error fetching file from storage: {e}")
        return None



def preprocess_data(document, scaler):
    """
    Convert the Appwrite document to a format suitable for the ML model.
    """
    # Extract features and transform to match model input
    try:
        # Parse the 'schedule' field to extract datetime components
        schedule_datetime = datetime.fromisoformat(document['schedule'].replace('Z', ''))

        features = [
            int(document['gender'] == 'M'),  # Male: 1, Female: 0
            int(document['age']),
            int(document['hypertension']),
            int(document['scholarship']),
            int(document['diabetes']),
            int(document['alcoholism']),
            int(document['handicap']),
            int(document['smsRecieved']),
            schedule_datetime.month,        # ScheduledMonth (1-12)
            schedule_datetime.weekday(),    # ScheduledDayOfWeek (0=Monday, 6=Sunday)
            schedule_datetime.hour          # ScheduledHour (0-23)
        ]
    except Exception as e:
        # Log and handle if parsing fails
        print(f"Error parsing schedule field: {e}")
        # Include default values in case of parsing error
        features = [
            int(document['gender'] == 'M'),
            int(document['age']),
            int(document['hypertension']),
            int(document['scholarship']),
            int(document['diabetes']),
            int(document['alcoholism']),
            int(document['handicap']),
            int(document['smsRecieved']),
            0,  # Default ScheduledMonth
            0,  # Default ScheduledDayOfWeek
            0   # Default ScheduledHour
        ]
        
    # Scale features
    #return scaler.transform([features])
    return features

# This Appwrite function will be executed every time your function is triggered
def main(context):
    # Initialize Appwrite client
    client = (
        Client()
        .set_endpoint(os.environ["NEXT_PUBLIC_ENDPOINT"])  # Replace with actual endpoint
        .set_project(os.environ["PROJECT_ID"])  # Replace with actual project ID
        .set_key(os.environ["API_KEY"])  # Replace with actual API key
    )

    # Initialize Databases service
    databases = Databases(client)
    storage = Storage(client)

    try:

        # File IDs for scaler and model
        bucket_id = os.environ["NEXT_PUBLIC_PREDICTIONFILES"]
        scaler_file_id = os.environ["SCALER_ID"]
        model_file_id = os.environ["LOGISTIC_REGRESSION_MODEL_ID"]

        # Temporary paths for scaler and model
        scaler_path = "/tmp/scaler.pkl"
        model_path = "/tmp/logistic_regression_model.pkl"

        # Fetch scaler and model from Appwrite Storage
        fetch_file_from_storage(context, storage, bucket_id, scaler_file_id, scaler_path)
        fetch_file_from_storage(context, storage, bucket_id, model_file_id, model_path)

        # # Fetch the file from Appwrite Storage and log the result
        # result = fetch_file_from_storage(context, storage, bucket_id, scaler_file_id, scaler_path)
    
        # # Log what is returned
        # if result:
        #     context.log(f"Scaler file fetched successfully, saved at {result}")
        # else:
        #     context.error("Failed to fetch scaler file.")
    


        

        # Load scaler and model
        scaler = joblib.load(scaler_path)
        model = joblib.load(model_path)


        # List documents, sorting by $createdAt in descending order
        response = databases.list_documents(
            database_id=os.environ["DATABASE_ID"],  # Your database ID
            collection_id=os.environ["APPOINTMENT_COLLECTION_ID"],  # Your collection ID
            queries = [Query.order_desc("$createdAt")],
            #test
        )

        # Check if any documents are returned
        if response["documents"]:
            # Return the most recent document
            latest_document = response["documents"][0]  # First document is the latest
            context.log(f"Latest document full data: {latest_document}")

            features = preprocess_data(latest_document, scaler)

            context.log(f"Latest document features needed: {features}")
            return context.res.json(features)
        else:
            # No documents found
            error_response = {"error": "No documents found in the collection"}
            context.log(error_response)
            return context.res.json(error_response)

    except AppwriteException as err:
        # Log and return any error that occurs
        context.error(f"Error fetching the latest document: {repr(err)}")
        return context.res.json({"error": "Could not fetch the latest document", "message": str(err)})

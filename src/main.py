from appwrite.client import Client
from appwrite.services.databases import Databases
from appwrite.services.storage import Storage
from appwrite.exception import AppwriteException
from appwrite.query import Query
import os
import joblib
import numpy as np
import smtplib
import requests
import logging
from datetime import datetime

logging.basicConfig(level=logging.DEBUG)


#function to get zoom access token 
def get_access_token(client_id, client_secret):
    url = "https://zoom.us/oauth/token"
    payload = {
        "grant_type": "client_credentials"
    }
    headers = {
        "Authorization": f"Basic {requests.auth._basic_auth_str(client_id, client_secret)}"
    }
    response = requests.post(url, params=payload, headers=headers)
    if response.status_code == 200:
        access_token = response.json().get("access_token")
        logging.debug(f"Access token received: {access_token}")  # Log the access token
        return access_token
    else:
        raise Exception(f"Error: {response.status_code}, {response.text}")

#Function to create zoom meeting
def create_zoom_meeting(access_token, topic, start_time, duration):
    url = "https://api.zoom.us/v2/users/me/meetings"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    meeting_details = {
        "topic": topic,
        "type": 2,  # Scheduled meeting
        "start_time": start_time,  # Format: "2024-12-01T15:00:00Z"
        "duration": duration,  # In minutes
        "timezone": "UTC",
        "settings": {
            "join_before_host": True,
            "waiting_room": False
        }
    }
    response = requests.post(url, json=meeting_details, headers=headers)
    if response.status_code == 201:
        meeting = response.json()
        logging.debug(f"Zoom meeting created: {meeting}")  # Log the meeting details
        return meeting
    else:
        raise Exception(f"Error: {response.status_code}, {response.text}")
    



#Function to send email
def send_email(subject, body, to_email):
    """
    Sends an email after document update based on the prediction.
    """
    try:
        # Email configuration
        email = os.environ["EMAIL"]
        authKey = os.environ["EMAIL_AUTH_KEY"]
        smtp_server = "smtp.gmail.com"
        smtp_port = 587

        # Create an SMTP object
        server = smtplib.SMTP(smtp_server, smtp_port)
        server.starttls()
        server.login(email, authKey)

        # Create the message
        message = f"Subject: {subject}\n\n{body}"

        # Send the email
        server.sendmail(email, to_email, message)
        server.quit()

        return True
    except Exception as e:
        logging.error(f"Error sending email: {e}")
        return False



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

    valid_neighbourhoods = ['AEROPORTO',
       'ANDORINHAS', 'ANTÔNIO HONÓRIO', 'ARIOVALDO FAVALESSA',
       'BARRO VERMELHO', 'BELA VISTA', 'BENTO FERREIRA', 'BOA VISTA', 'BONFIM',
       'CARATOÍRA', 'CENTRO', 'COMDUSA', 'CONQUISTA', 'CONSOLAÇÃO',
       'CRUZAMENTO', 'DA PENHA', 'DE LOURDES', 'DO CABRAL', 'DO MOSCOSO',
       'DO QUADRO', 'ENSEADA DO SUÁ', 'ESTRELINHA', 'FONTE GRANDE',
       'FORTE SÃO JOÃO', 'FRADINHOS', 'GOIABEIRAS', 'GRANDE VITÓRIA',
       'GURIGICA', 'HORTO', 'ILHA DAS CAIEIRAS', 'ILHA DE SANTA MARIA',
       'ILHA DO BOI', 'ILHA DO FRADE', 'ILHA DO PRÍNCIPE',
       'ILHAS OCEÂNICAS DE TRINDADE', 'INHANGUETÁ', 'ITARARÉ', 'JABOUR',
       'JARDIM CAMBURI', 'JARDIM DA PENHA', 'JESUS DE NAZARETH', 'JOANA D´ARC',
       'JUCUTUQUARA', 'MARIA ORTIZ', 'MARUÍPE', 'MATA DA PRAIA', 'MONTE BELO',
       'MORADA DE CAMBURI', 'MÁRIO CYPRESTE', 'NAZARETH', 'NOVA PALESTINA',
       'PARQUE INDUSTRIAL', 'PARQUE MOSCOSO', 'PIEDADE', 'PONTAL DE CAMBURI',
       'PRAIA DO CANTO', 'PRAIA DO SUÁ', 'REDENÇÃO', 'REPÚBLICA',
       'RESISTÊNCIA', 'ROMÃO', 'SANTA CECÍLIA', 'SANTA CLARA', 'SANTA HELENA',
       'SANTA LUÍZA', 'SANTA LÚCIA', 'SANTA MARTHA', 'SANTA TEREZA',
       'SANTO ANDRÉ', 'SANTO ANTÔNIO', 'SANTOS DUMONT', 'SANTOS REIS',
       'SEGURANÇA DO LAR', 'SOLON BORGES', 'SÃO BENEDITO', 'SÃO CRISTÓVÃO',
       'SÃO JOSÉ', 'SÃO PEDRO', 'TABUAZEIRO', 'UNIVERSITÁRIO', 'VILA RUBIM']
    

    features = [
            int(document['age']),
            int(document['scholarship']),
            int(document['hypertension']),
            int(document['diabetes']),
            int(document['alcoholism']),
            int(document['handicap']),
            int(document['smsRecieved']),
            document['gender'] == 'M',  # Male: 1, Female: 0
            ]
    
    # Extract features and transform to match model input
    try:
        # Parse the 'schedule' field to extract datetime components
        schedule_datetime = datetime.fromisoformat(document['schedule'].replace('Z', ''))

        # Parse the '$createdAt' field represended with appointment_datetime to extract datetime components
        appointment_datetime = datetime.fromisoformat(document['$createdAt'].replace('Z', ''))

        features.extend([
            schedule_datetime.month,          # ScheduledMonth (1-12)
            schedule_datetime.weekday(),      # ScheduledDayOfWeek (0=Monday, 6=Sunday)
            schedule_datetime.hour,           # ScheduledHour (0-23)
            appointment_datetime.month,       # CreatedAtMonth (1-12)
            appointment_datetime.weekday(),   # CreatedAtDayOfWeek (0=Monday, 6=Sunday)
            appointment_datetime.hour         # CreatedAtHour (0-23)
        ])

        # Iterate over the list of valid neighbourhoods and add a corresponding feature for each one
        for neighbourhood in valid_neighbourhoods:
            features.append(document['neighbourhood'] == neighbourhood)    
            
        
    except Exception as e:
        # Log and handle if parsing fails
        print(f"Error parsing schedule field: {e}")
    
        # Add default values if parsing fails
        features.extend([0, 0, 0, 0, 0, 0, ])

        # Add False for each valid neighbourhood if date parsing fails
        features.extend([False] * len(valid_neighbourhoods))  # False for each neighbourhood
    
        
    # Scale features
    return scaler.transform([features])
    #return features





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

            # Preprocess data for prediction
            features = preprocess_data(latest_document, scaler)


            # Predict no-show
            prediction = model.predict(features)[0]  # True: Will show up, False: No-show


            # Update the document based on the prediction
            updated_status = "cancelled" if not prediction else "scheduled"
            update_payload = {"status": updated_status}

            context.log(f"Latest prediction: {prediction}")
            context.log(f"Latest Status After prediction: {updated_status}")


            # Update the document in the collection
            databases.update_document(
            database_id=os.environ["DATABASE_ID"],
            collection_id=os.environ["APPOINTMENT_COLLECTION_ID"],
            document_id=latest_document["$id"],
            data=update_payload,
            )

            
            CLIENT_ID = os.environ["CLIENT_ID"]
            CLIENT_SECRET = os.environ["CLIENT_SECRET"]
    
            try:
                # Step 1: Get Access Token
                access_token = get_access_token(CLIENT_ID, CLIENT_SECRET)
                context.log("Access Token:", access_token)
                
                # Step 2: Create a Zoom Meeting
                TOPIC = "My Test Meeting"
                START_TIME = "2024-12-30T15:00:00Z"  # ISO 8601 format
                DURATION = 30  # Minutes
                meeting = create_zoom_meeting(access_token, TOPIC, START_TIME, DURATION)
                
                context.log("Meeting Created!")
                context.log("Join URL:", meeting["join_url"])
                context.log("Start URL:", meeting["start_url"])
            
            except Exception as e:
                context.log(str(e))


            # Send the email after the update
            subject = f"Appointment Status Updated: {updated_status}"
            body = (
                f"The status of the appointment with Dr. {latest_document['primaryPhysician']}, "
                f"scheduled for {datetime.fromisoformat(latest_document['schedule'].replace('Z', '')).strftime('%Y-%m-%d %H:%M:%S')}, "
                f"has been updated to '{updated_status}' based on the prediction."
            )
            recipient_email = os.environ["EMAIL"]

            # Send the email to the recipient
            send_email(subject, body, recipient_email)
            
            return context.res.json({"status": "success", "updated_status": updated_status})
        else:
            # No documents found
            error_response = {"error": "No documents found in the collection"}
            context.log(error_response)
            return context.res.json(error_response)

    except AppwriteException as err:
        # Log and return any error that occurs
        context.error(f"Error: {repr(err)}")
        return context.res.json({"error": "An error occurred", "message": str(err)})

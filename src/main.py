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


# CLIENT_ID = os.environ["CLIENT_ID"]
# CLIENT_SECRET = os.environ["CLIENT_SECRET"]

# replace with your client ID
client_id = os.environ["CLIENT_ID"]

# replace with your account ID
account_id = os.environ["ACCOUNT_ID"]

# replace with your client secret
client_secret = os.environ["CLIENT_SECRET"]

auth_token_url = "https://zoom.us/oauth/token"
api_base_url = "https://api.zoom.us/v2"
    

# create the Zoom link function
def create_meeting(context, topic, duration, start_date, start_time):
        data = {
        "grant_type": "account_credentials",
        "account_id": account_id,
        "client_secret": client_secret
        }
        response = requests.post(auth_token_url, 
                                 auth=(client_id, client_secret), 
                                 data=data)
        
        if response.status_code!=200:
            context.log("Unable to get access token")
            return None  # Return None in case of failure
        
        response_data = response.json()
        access_token = response_data["access_token"]

        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
        payload = {
            "topic": topic,
            "duration": duration,
            'start_time': f'{start_date}T10:{start_time}',
            "type": 2
        }

        resp = requests.post(f"{api_base_url}/users/me/meetings", 
                             headers=headers, 
                             json=payload)
        
        if resp.status_code!=201:
            context.log("Unable to generate meeting link")
            return None  # Return None in case of failure
        
        response_data = resp.json()
        
        meeting_details = {
                    "meeting_url": response_data["join_url"], 
                    "password": response_data["password"],
                    "meetingTime": response_data["start_time"],
                    "purpose": response_data["topic"],
                    "duration": response_data["duration"],
                    "message": "Success",
                    "status":1
        }
        context.log(meeting_details)
        return meeting_details



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

            
            meeting_details = create_meeting(
                context,
                "Carepulse Zoom Meeting",
                "60",
                "2024-12-23",
                "18:24",
            )


            # Send the email after the update
            subject = f"Appointment Status Updated: {updated_status}"
            body = (
                f"The status of the appointment with Dr. {latest_document['primaryPhysician']}, "
                f"scheduled for {datetime.fromisoformat(latest_document['schedule'].replace('Z', '')).strftime('%Y-%m-%d %H:%M:%S')}, "
                f"has been updated to '{updated_status}' based on the prediction."
            )

            # Check if meeting details exist and status is 'scheduled'
            if updated_status == 'scheduled' and meeting_details:
                body += (
                    f"\n\nA Zoom meeting has been scheduled for this appointment. You can join the meeting using the following details:\n"
                    f"Meeting Link: {meeting_details['meeting_url']}\n"
                    f"Password: {meeting_details['password']}\n"
                    f"Meeting Time: {meeting_details['meetingTime']}\n"
                    f"Purpose: {meeting_details['purpose']}\n"
                    f"Duration: {meeting_details['duration']} minutes"
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

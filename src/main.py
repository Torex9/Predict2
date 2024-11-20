from appwrite.client import Client
from appwrite.services.databases import Databases
from appwrite.exception import AppwriteException
import os

def main(context):
    # Initialize Appwrite client
    client = (
        Client()
        .set_endpoint(os.environ["APPWRITE_FUNCTION_API_ENDPOINT"])  # API endpoint
        .set_project(os.environ["APPWRITE_FUNCTION_PROJECT_ID"])      # Project ID
        .set_key(context.req.headers["x-appwrite-key"])               # API key from request headers
    )
    
    # Initialize the Databases service
    databases = Databases(client)
    
    # Specify the database and collection IDs
    database_id = "67317e6000069e6c10c9"  # Replace with your database ID
    collection_id = "67317eca00290232aa78"  # Replace with your collection ID
    
    try:
        # Fetch all documents in the collection
        response = databases.list_documents(database_id=database_id, collection_id=collection_id)
        
        # Log the total count of documents
        context.log("Total documents: " + str(response["total"]))
        
        # Return the documents as a JSON response
        return context.res.json(response["documents"])
    
    except AppwriteException as err:
        # Handle errors
        context.error("Could not fetch documents: " + repr(err))
        return context.res.json({"error": str(err)}, status_code=500)

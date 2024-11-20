from appwrite.client import Client
from appwrite.services.databases import Databases
from appwrite.exception import AppwriteException
import os

def main(context):
    # Initialize Appwrite client
    client = (
        Client()
        .set_endpoint("https://cloud.appwrite.io/v1")  # API endpoint
        .set_project("67317d0100250021489f")      # Project ID
        .set_key('standard_ebc15b8b03726bfeb75ee3153cda1c16c0d353697508f691e3552a0e3c5e07d2d376c62e0b9c8ee742dec9f206eaa48f2916388d7fb907b32a69a0cad3f7571e52758602c60841549ed475cc557cbb9324404387d31f5d123e5c9ba1f126abab5cc14a8efbec8197afffba59636c2359f4618b48d396aac79134203d4563af6a')               # API key from request headers
    )
    
    # Initialize the Databases service
    databases = Databases(client)
    
    # Specify the database and collection IDs
    database_ID = "67317e6000069e6c10c9"  # Replace with your database ID
    collection_ID = "67317eca00290232aa78"  # Replace with your collection ID
    
    try:
        # Fetch all documents in the collection
        response = databases.list_documents(database_id=database_ID, collection_id=collection_ID)
        
        # Log the total count of documents
        context.log("Total documents: " + str(response["total"]))
        
        # Return the documents as a JSON response
        return context.res.json(response["documents"])
    
    except AppwriteException as err:
        # Handle errors
        context.error("Could not fetch documents: " + repr(err))
        return context.res.json({"error": str(err)}, 500)


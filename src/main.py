from appwrite.client import Client
from appwrite.services.databases import Databases
from appwrite.exception import AppwriteException
from appwrite.query import Query
import os

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

    try:
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
            context.log(f"Latest document: {latest_document}")
            return context.res.json(latest_document)
        else:
            # No documents found
            error_response = {"error": "No documents found in the collection"}
            context.log(error_response)
            return context.res.json(error_response)

    except AppwriteException as err:
        # Log and return any error that occurs
        context.error(f"Error fetching the latest document: {repr(err)}")
        return context.res.json({"error": "Could not fetch the latest document", "message": str(err)})

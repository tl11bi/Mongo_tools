
from pymongo import MongoClient
from pymongo.errors import PyMongoError
import threading
import logging

# Configuration Variables
PRIMARY_MONGO_URI = "mongodb://localhost:27017"
SECONDARY_MONGO_URI = "mongodb://localhost:27018"
DATABASE_NAME = 'training'
LOGGING_LEVEL = logging.INFO  # Can be set to logging.DEBUG for more detailed logs

# Establish connections
primary_client = MongoClient(PRIMARY_MONGO_URI)
primary_db = primary_client[DATABASE_NAME]

secondary_client = MongoClient(SECONDARY_MONGO_URI)
secondary_db = secondary_client[DATABASE_NAME]

# Setup Logging
logging.basicConfig(level=LOGGING_LEVEL, format='%(asctime)s - %(levelname)s - %(message)s')


# Function to copy collection if it does not exist
def copy_collection_if_not_exists(collection_name):
    if collection_name not in secondary_db.list_collection_names():
        logging.info(f"Copying collection '{collection_name}' to secondary database...")
        primary_collection = primary_db[collection_name]
        secondary_collection = secondary_db[collection_name]

        documents = list(primary_collection.find())

        if documents:
            secondary_collection.insert_many(documents)
        logging.info(f"Collection '{collection_name}' copied successfully.")
    else:
        logging.info(f"Collection '{collection_name}' already exists in secondary database.")


# Function to start database-wide change stream
def start_db_change_stream():
    try:
        with primary_db.watch(full_document='updateLookup') as stream:
            logging.info("Database change stream started...")
            for change in stream:
                collection_name = change["ns"]["coll"]
                operation_type = change["operationType"]
                document = change.get("fullDocument")
                document_id = change["documentKey"]["_id"]

                secondary_collection = secondary_db[collection_name]

                if operation_type == "insert":
                    secondary_collection.insert_one(document)
                    logging.info(f"Inserted new document into '{collection_name}': {document}")

                elif operation_type == "update":
                    update_description = change["updateDescription"]
                    updated_fields = update_description["updatedFields"]
                    removed_fields = update_description["removedFields"]

                    update_query = {}
                    if updated_fields:
                        update_query["$set"] = updated_fields
                    if removed_fields:
                        update_query["$unset"] = {field: "" for field in removed_fields}

                    secondary_collection.update_one({"_id": document_id}, update_query)
                    logging.info(f"Updated document in '{collection_name}': {document_id} with changes: {update_query}")

                elif operation_type == "replace":
                    secondary_collection.replace_one({"_id": document_id}, document)
                    logging.info(f"Replaced document in '{collection_name}': {document_id} with new document: {document}")

                elif operation_type == "delete":
                    secondary_collection.delete_one({"_id": document_id})
                    logging.info(f"Deleted document from '{collection_name}': {document_id}")

                # Handle other operation types if necessary

    except PyMongoError as e:
        logging.error(f"Error in change stream: {e}")


if __name__ == "__main__":
    # Copy collections if they do not exist
    for collection_name in primary_db.list_collection_names():
        copy_collection_if_not_exists(collection_name)

    # Start the change stream in a separate thread
    change_stream_thread = threading.Thread(target=start_db_change_stream, daemon=True)
    change_stream_thread.start()

    # Keep the main thread alive
    try:
        while True:
            pass
    except KeyboardInterrupt:
        logging.info("Shutting down synchronization service...")

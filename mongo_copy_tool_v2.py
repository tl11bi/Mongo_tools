from pymongo import MongoClient
from pymongo.errors import PyMongoError, OperationFailure
from concurrent.futures import ThreadPoolExecutor
import threading
import logging
from tenacity import retry, stop_after_attempt, wait_fixed

# Configuration Variables
PRIMARY_MONGO_URI = "mongodb://localhost:27017"
SECONDARY_MONGO_URI = "mongodb://localhost:27018"
DATABASE_NAME = 'training'
LOGGING_LEVEL = logging.INFO  # Can be set to logging.DEBUG for more detailed logs
BATCH_SIZE = 1000  # Number of documents to insert in each batch

# Establish connections
primary_client = MongoClient(PRIMARY_MONGO_URI)
primary_db = primary_client[DATABASE_NAME]

secondary_client = MongoClient(SECONDARY_MONGO_URI)
secondary_db = secondary_client[DATABASE_NAME]

# Setup Logging
logging.basicConfig(level=LOGGING_LEVEL, format='%(asctime)s - %(levelname)s - %(message)s')


@retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
def batch_insert(documents, secondary_collection):
    """
    Inserts documents into the secondary collection with retry logic.
    """
    if documents:
        secondary_collection.insert_many(documents)


# Function to copy documents from primary to secondary in batches
def copy_documents_in_batches(primary_collection, secondary_collection):
    """
    Copy documents from the primary to the secondary collection in batches.
    """
    cursor = primary_collection.find()
    buffer = []
    for document in cursor:
        buffer.append(document)
        if len(buffer) >= BATCH_SIZE:
            batch_insert(buffer, secondary_collection)
            buffer.clear()

    if buffer:
        batch_insert(buffer, secondary_collection)


def copy_collection_options(collection_name):
    """
    Copy collection options like collation, validation, etc., from primary to secondary.
    """
    options = primary_db.command('listCollections', filter={"name": collection_name})['cursor']['firstBatch'][0].get('options', {})

    # Ensure collation has a locale if it exists
    if 'collation' in options and 'locale' not in options['collation']:
        logging.warning(f"Skipping collation for collection '{collection_name}' due to missing locale.")
        options.pop('collation', None)  # Remove collation if locale is missing

    if options:
        secondary_db.create_collection(collection_name, **options)
        logging.info(f"Created collection '{collection_name}' with options: {options}")
    else:
        logging.info(f"Creating collection '{collection_name}' without additional options.")


def copy_indexes_if_not_exists(collection_name):
    """
    Copy indexes from the primary to the secondary collection if they don't exist.
    """
    primary_collection = primary_db[collection_name]
    secondary_collection = secondary_db[collection_name]

    primary_indexes = primary_collection.index_information()
    secondary_indexes = secondary_collection.index_information()

    for index_name, primary_index_info in primary_indexes.items():
        secondary_index_info = secondary_indexes.get(index_name)

        # Exclude 'ns' (namespace) from comparison for valid index check
        if primary_index_info:
            primary_index_info.pop('ns', None)
        if secondary_index_info:
            secondary_index_info.pop('ns', None)

        # Check if index exists in db2, if not create it
        if primary_index_info != secondary_index_info:
            logging.warning(f"Indexes differ in collection '{collection_name}': db1={primary_index_info} vs db2={secondary_index_info}")
            
            # Log the full index information for further debugging
            logging.debug(f"Attempting to create index: {primary_index_info}")

            # Validate index specifiers and create index
            try:
                index_keys = primary_index_info['key']
                
                # Validate the key values, ensuring they are valid MongoDB specifiers
                valid_key = True
                for field, value in index_keys:
                    if value not in [1, -1, '2d', '2dsphere', 'text', 'hashed']:
                        logging.error(f"Invalid index specifier: {field}={value} in collection '{collection_name}'. Skipping this index.")
                        valid_key = False
                        break
                
                if valid_key:
                    # Create the index only if the key specifiers are valid
                    index_kwargs = {k: v for k, v in primary_index_info.items() if k != 'key'}
                    secondary_collection.create_index(index_keys, **index_kwargs)
                    logging.info(f"Created index '{index_name}' in collection '{collection_name}' for db2.")
            except Exception as e:
                logging.error(f"Failed to create index '{index_name}' in collection '{collection_name}' for db2: {e}")
        else:
            logging.info(f"Index '{index_name}' already exists in db2 for collection '{collection_name}'.")

def copy_capped_collection_if_not_exists(collection_name):
    """
    Handle capped collections, ensuring they are copied as capped collections in the secondary DB.
    """
    primary_collection = primary_db[collection_name]
    options = primary_collection.options()

    if options.get('capped'):
        secondary_db.create_collection(
            collection_name,
            capped=True,
            size=options['size'],
            max=options.get('max')
        )
        logging.info(f"Capped collection '{collection_name}' created in secondary database.")


def copy_collection_if_not_exists(collection_name):
    """
    Copy collections, handle capped collections, copy indexes, and ensure collection options are copied.
    """
    if collection_name not in secondary_db.list_collection_names():
        logging.info(f"Copying collection '{collection_name}' to secondary database...")

        primary_collection = primary_db[collection_name]

        # Check if it's a capped collection and handle accordingly
        if primary_collection.options().get('capped'):
            copy_capped_collection_if_not_exists(collection_name)
        else:
            # Copy collection options
            copy_collection_options(collection_name)

            # Copy documents in batches to avoid memory or timeout issues
            secondary_collection = secondary_db[collection_name]
            copy_documents_in_batches(primary_collection, secondary_collection)

        # Ensure indexes are identical
        copy_indexes_if_not_exists(collection_name)

        logging.info(f"Collection '{collection_name}' copied successfully.")
    else:
        logging.info(f"Collection '{collection_name}' already exists in secondary database.")


def copy_views_if_not_exists():
    """
    Copy view definitions from the primary to the secondary database if they don't exist.
    """
    for view in primary_db.list_collections(filter={"type": "view"}):
        view_name = view['name']
        if view_name not in secondary_db.list_collection_names():
            logging.info(f"Creating view '{view_name}' in secondary database...")

            # Get the view pipeline and options (including collation)
            view_pipeline = view['options']['pipeline']
            view_collation = view['options'].get('collation', {})

            # Check if the collation exists and if it contains the 'locale' field
            if 'locale' not in view_collation and view_collation:
                logging.warning(f"Skipping collation for view '{view_name}' due to missing locale.")
                view_collation = None  # Skip the collation if locale is missing

            # Create the view with or without collation
            if view_collation:
                secondary_db.create_collection(view_name, viewOn=view['options']['viewOn'], pipeline=view_pipeline, collation=view_collation)
                logging.info(f"View '{view_name}' created with collation: {view_collation}")
            else:
                secondary_db.create_collection(view_name, viewOn=view['options']['viewOn'], pipeline=view_pipeline)
                logging.info(f"View '{view_name}' created without collation.")

        else:
            logging.info(f"View '{view_name}' already exists in secondary database.")

def validate_collection_copy(collection_name):
    """
    Validate that the document counts between the primary and secondary collections match.
    """
    primary_count = primary_db[collection_name].count_documents({})
    secondary_count = secondary_db[collection_name].count_documents({})

    if primary_count == secondary_count:
        logging.info(f"Collection '{collection_name}' validated successfully.")
    else:
        logging.warning(f"Collection '{collection_name}' validation failed: primary_count={primary_count}, secondary_count={secondary_count}")


def is_replica_set(mongo_client):
    """
    Check if the MongoDB instance is part of a replica set.
    """
    try:
        status = mongo_client.admin.command("replSetGetStatus")
        return "ok" in status and status["ok"] == 1.0
    except OperationFailure as e:
        logging.warning(f"Not a replica set: {e}")
        return False


# Function to start database-wide change stream
def start_db_change_stream():
    # Check if the primary database is part of a replica set before starting change streams
    if is_replica_set(primary_client):
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

        except PyMongoError as e:
            logging.error(f"Error in change stream: {e}")
    else:
        logging.info("The primary database is not part of a replica set. Change streams are not supported.")


def copy_all_collections_concurrently():
    """
    Copy all collections from the primary to the secondary database concurrently.
    """
    collections = primary_db.list_collection_names(filter={"type": "collection"})
    with ThreadPoolExecutor(max_workers=4) as executor:
        executor.map(copy_collection_if_not_exists, collections)


if __name__ == "__main__":
    # Copy collections if they do not exist (concurrently)
    copy_all_collections_concurrently()

    # Copy views if they do not exist
    copy_views_if_not_exists()

    # Validate collections after copying
    for collection_name in primary_db.list_collection_names(filter={"type": "collection"}):
        validate_collection_copy(collection_name)

    # Start the change stream in a separate thread (only if the database is part of a replica set)
    change_stream_thread = threading.Thread(target=start_db_change_stream, daemon=True)
    change_stream_thread.start()

    # Keep the main thread alive
    try:
        while True:
            pass
    except KeyboardInterrupt:
        logging.info("Shutting down synchronization service...")
from pymongo import MongoClient
import logging
import math

# Global Configuration and Variables
MONGO_URI_1 = 'mongodb://localhost:27017/'
MONGO_URI_2 = 'mongodb://localhost:27018/'

DB1_NAME = 'training'
DB2_NAME = 'training'

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def connect_to_mongo(uri):
    """
    Connect to a MongoDB instance.

    :param uri: MongoDB URI string.
    :return: MongoClient instance.
    """
    logging.info(f"Connecting to MongoDB instance at {uri}...")
    return MongoClient(uri)


def fetch_documents_from_collection(db, collection_name):
    """
    Fetch all documents from a MongoDB collection.

    :param db: Database object from MongoDB client.
    :param collection_name: Name of the collection to fetch documents from.
    :return: List of documents in the collection.
    """
    logging.info(f"Fetching documents from the collection: {collection_name}")
    collection = db[collection_name]
    documents = list(collection.find())
    logging.info(f"Number of documents fetched from {db.name}.{collection_name}: {len(documents)}")
    return documents


def is_nan_or_none(value):
    """
    Check if a value is either NaN or None.

    :param value: The value to check.
    :return: True if the value is NaN or None, False otherwise.
    """
    return value is None or (isinstance(value, float) and math.isnan(value))


def compare_collection_counts(db1, db2, collection_name):
    """
    Compare the document counts between two collections.

    :param db1: First MongoDB database.
    :param db2: Second MongoDB database.
    :param collection_name: Name of the collection to compare.
    :return: True if counts match, False otherwise.
    """
    count1 = db1[collection_name].count_documents({})
    count2 = db2[collection_name].count_documents({})

    if count1 != count2:
        logging.warning(f"Collection '{collection_name}' has different document counts: db1={count1} vs db2={count2}")
        return False
    else:
        logging.info(f"Collection '{collection_name}' document counts match: {count1}")
        return True


def compare_indexes(db1, db2, collection_name):
    """
    Compare indexes between two collections.

    :param db1: First MongoDB database.
    :param db2: Second MongoDB database.
    :param collection_name: Name of the collection to compare indexes.
    :return: None
    """
    indexes_db1 = db1[collection_name].index_information()
    indexes_db2 = db2[collection_name].index_information()

    if indexes_db1 != indexes_db2:
        logging.warning(f"Indexes differ in collection '{collection_name}': db1={indexes_db1} vs db2={indexes_db2}")
    else:
        logging.info(f"Indexes match in collection '{collection_name}'")


def compare_nested_fields(value1, value2):
    """
    Recursively compare nested fields in the case of embedded documents or arrays.

    :param value1: Value from the first document (could be a dict or list).
    :param value2: Value from the second document (could be a dict or list).
    :return: List of differences.
    """
    nested_differences = []

    if isinstance(value1, dict) and isinstance(value2, dict):
        for key in value1:
            if key in value2:
                nested_differences.extend(compare_nested_fields(value1[key], value2[key]))
            else:
                nested_differences.append(f"Field '{key}' is missing in the second nested document.")

        for key in value2:
            if key not in value1:
                nested_differences.append(f"Field '{key}' is missing in the first nested document.")

    elif isinstance(value1, list) and isinstance(value2, list):
        if len(value1) != len(value2):
            nested_differences.append(
                f"Array lengths differ: db1 has {len(value1)} elements, db2 has {len(value2)} elements.")
        else:
            for i, (item1, item2) in enumerate(zip(value1, value2)):
                nested_differences.extend(compare_nested_fields(item1, item2))
    else:
        if value1 != value2:
            nested_differences.append(f"Values differ: db1={value1} vs db2={value2}")

    return nested_differences


def compare_fields(doc1, doc2):
    """
    Compare two documents field by field, including nested fields.

    :param doc1: First document.
    :param doc2: Second document.
    :return: List of field differences.
    """
    differences = []

    # Compare fields that exist in doc1 but not in doc2
    for key in doc1:
        if key not in doc2:
            differences.append(f"Field '{key}' is missing in doc2.")
        else:
            value1 = doc1[key]
            value2 = doc2[key]

            # Handle None or NaN values
            if is_nan_or_none(value1) and is_nan_or_none(value2):
                continue  # Skip if both are None or NaN

            # Handle nested documents or arrays
            nested_differences = compare_nested_fields(value1, value2)
            if nested_differences:
                differences.append(f"Differences in field '{key}': {nested_differences}")

    # Compare fields that exist in doc2 but not in doc1
    for key in doc2:
        if key not in doc1:
            differences.append(f"Field '{key}' is missing in doc1.")

    return differences


def compare_collections(docs1, docs2):
    """
    Compare two lists of documents and their fields.

    :param docs1: List of documents from the first collection.
    :param docs2: List of documents from the second collection.
    :return: List of document differences.
    """
    logging.info("Comparing documents from both collections...")
    differences = []

    docs2_dict = {doc['_id']: doc for doc in docs2}  # Convert doc2 list to a dict for faster lookup by _id

    for doc1 in docs1:
        doc1_id = doc1['_id']

        if doc1_id not in docs2_dict:
            differences.append(f"Document with _id={doc1_id} is missing in the second collection.")
        else:
            doc2 = docs2_dict[doc1_id]
            field_differences = compare_fields(doc1, doc2)
            if field_differences:
                differences.append(f"Differences in document with _id={doc1_id}: {field_differences}")

    return differences


def check_for_duplicates(db, collection_name, unique_field="_id"):
    """
    Check for duplicate entries in a collection based on a unique field.

    :param db: MongoDB database.
    :param collection_name: Name of the collection.
    :param unique_field: Field to check for uniqueness (default is '_id').
    :return: None
    """
    duplicates = db[collection_name].aggregate([
        {"$group": {"_id": f"${unique_field}", "count": {"$sum": 1}}},
        {"$match": {"count": {"$gt": 1}}}
    ])

    dup_list = list(duplicates)
    if dup_list:
        logging.warning(
            f"Duplicate documents found in collection '{collection_name}' for field '{unique_field}': {dup_list}")
    else:
        logging.info(f"No duplicates found in collection '{collection_name}' for field '{unique_field}'.")


def compare_all_collections(db1, db2):
    """
    Compare all collections between two MongoDB databases.

    :param db1: First MongoDB database.
    :param db2: Second MongoDB database.
    :return: None
    """
    # Fetch all collections from both databases
    collections_db1 = db1.list_collection_names()
    collections_db2 = db2.list_collection_names()

    logging.info(f"Collections in {db1.name}: {collections_db1}")
    logging.info(f"Collections in {db2.name}: {collections_db2}")

    # Compare collections present in both databases
    common_collections = set(collections_db1).intersection(set(collections_db2))
    if not common_collections:
        logging.warning("No common collections found between the two databases.")
        return

    for collection_name in common_collections:
        logging.info(f"Comparing collection: {collection_name}")

        # Compare document counts
        if not compare_collection_counts(db1, db2, collection_name):
            continue  # Skip comparison if counts don't match

        # Compare indexes
        compare_indexes(db1, db2, collection_name)

        # Fetch documents from both collections
        docs1 = fetch_documents_from_collection(db1, collection_name)
        docs2 = fetch_documents_from_collection(db2, collection_name)

        # Compare documents from both collections
        differences = compare_collections(docs1, docs2)

        if differences:
            logging.info(f"Differences found in collection '{collection_name}':")
            for diff in differences:
                logging.info(diff)
        else:
            logging.info(f"No differences found in collection '{collection_name}'.")

        # Check for duplicates
        check_for_duplicates(db1, collection_name)
        check_for_duplicates(db2, collection_name)


def main():
    # Connect to MongoDB instances
    client1 = connect_to_mongo(MONGO_URI_1)
    client2 = connect_to_mongo(MONGO_URI_2)

    # Access databases
    db1 = client1[DB1_NAME]
    db2 = client2[DB2_NAME]

    # Compare all collections across both databases
    compare_all_collections(db1, db2)


# Entry point
if __name__ == "__main__":
    main()

# MongoDB Database Comparison Tool

## Overview

This tool is designed to compare two MongoDB databases after a migration or between different versions of the database (e.g., MongoDB 4.4 vs MongoDB 7.0). It performs a comprehensive comparison of all collections, documents, indexes, and data types to ensure that the databases are consistent. The tool also includes additional checks, such as for duplicate documents and recursive comparison of nested fields (e.g., embedded documents or arrays).

## Features

1. **Compare Document Counts**: Verifies that the number of documents in each collection between the two databases is identical.
2. **Compare Document Fields**: Compares field-by-field differences between documents in both databases, including handling for nested documents and arrays.
3. **Handle `None` and `NaN` Values**: Ignores differences where both values are either `None` or `NaN`, preventing false-positive differences.
4. **Compare Indexes**: Ensures that the indexes in each collection are consistent between the two databases.
5. **Duplicate Check**: Identifies duplicate documents within collections based on a unique field (default is `_id`).
6. **Comprehensive Field Comparison**: Handles various data types (strings, numbers, arrays, nested documents, etc.) and reports differences in values and types.
7. **Logging**: Logs all differences found and key comparisons, ensuring the user can track any inconsistencies in the migration or synchronization process.

## Prerequisites

- **Python**: You need Python 3.x installed on your system.
- **MongoDB**: The tool requires access to two MongoDB instances (databases) for comparison.
- **Required Python Libraries**: The tool uses the `pymongo` and `math` libraries. If you don’t have `pymongo` installed, you can install it via pip:
  ```bash
  pip install pymongo
  ```

## How It Works

This tool works by performing the following comparisons:

### 1. **Document Count Comparison**
   - **Purpose**: Ensures that the number of documents in each collection matches between the two databases.
   - **Action**: Logs a warning if the document counts do not match, and skips further comparison for that collection.

### 2. **Index Comparison**
   - **Purpose**: Ensures that the indexes in both collections are the same, which is critical for performance and uniqueness constraints.
   - **Action**: Logs differences in indexes if they are found.

### 3. **Document Field Comparison**
   - **Purpose**: Compares individual documents in each collection based on their `_id`. It checks field-level differences, including nested fields and arrays.
   - **Details**:
     - If a field exists in one document but not the other, it logs the missing field.
     - If a field exists in both documents but has different values, the difference is logged.
     - If both values are `None` or `NaN`, the difference is ignored.
   - **Recursive Nested Field Comparison**: For embedded documents and arrays, the tool recursively compares the fields and elements.

### 4. **Duplicate Document Check**
   - **Purpose**: Ensures that no duplicate documents exist within the collections based on a unique field (default is `_id`).
   - **Action**: Logs any duplicates found in the collections.

### 5. **Logging**
   - Logs detailed differences for:
     - Missing documents
     - Different values between fields
     - Missing fields in documents
     - Differences in data types (e.g., string vs integer)
   - Outputs the results for each collection compared.

## Usage

### 1. **Configure MongoDB URIs**
   In the code, you need to specify the MongoDB URIs for the two databases you want to compare. The URIs should point to the two MongoDB instances:
   
   ```python
   MONGO_URI_1 = 'mongodb://localhost:27017/'
   MONGO_URI_2 = 'mongodb://localhost:27018/'
   ```

### 2. **Configure Database Names**
   Set the names of the databases to be compared:
   
   ```python
   DB1_NAME = 'training'
   DB2_NAME = 'training'
   ```

### 3. **Run the Script**
   Run the Python script using the command line:
   ```bash
   python compare_mongo_databases.py
   ```

### 4. **View Logs**
   The script will log detailed information about the comparison, including:
   - Missing or extra documents in either database.
   - Differences in field values between documents.
   - Differences in data types for matching fields.
   - Differences in index configurations.
   - Duplicate document detection.

### Sample Output

When running the script, you will see log entries similar to the following:

```bash
INFO - Connecting to MongoDB instance at mongodb://localhost:27017/...
INFO - Connecting to MongoDB instance at mongodb://localhost:27018/...
INFO - Collections in training: ['users', 'orders']
INFO - Collections in training: ['users', 'orders']
INFO - Comparing collection: users
INFO - Collection 'users' document counts match: 500
INFO - Indexes match in collection 'users'
INFO - Differences in document with _id=12345: ["Field 'age' differs: db1=30 vs db2=31", "Field 'address.city' is missing in doc2."]
INFO - No duplicates found in collection 'users' for field '_id'.
INFO - Comparing collection: orders
INFO - Collection 'orders' has different document counts: db1=200 vs db2=198
INFO - Skipping comparison of 'orders' due to document count mismatch.
```

## What Comparisons Are Performed

Here is a summary of the comparisons performed by the tool:

1. **Document Count**: Checks if the number of documents in each collection matches.
2. **Field-by-Field Document Comparison**:
   - Compares fields in documents with the same `_id`.
   - Detects missing fields, differing values, and differing data types.
   - Recursively compares nested documents and arrays.
3. **Indexes**: Compares the indexes between the two collections and logs any differences.
4. **Duplicates**: Checks for duplicate documents in each collection based on a unique field (default is `_id`).

## Customization

You can customize several parts of the script:

1. **Unique Field for Duplicate Check**: By default, the script checks for duplicates using the `_id` field. You can change this to another field by modifying the `unique_field` parameter in the `check_for_duplicates` function.
   
2. **Handling of Specific Fields**: If there are specific fields you don't want to compare or fields that need custom comparison logic, you can modify the `compare_fields` function.

3. **Database URIs and Names**: Modify the MongoDB URIs and database names according to your environment.

## Conclusion

This MongoDB comparison tool is a comprehensive solution to compare MongoDB databases after migration or between different instances. It ensures that all collections, documents, fields, and indexes are consistent between the databases, and it helps to catch discrepancies like missing documents, differing field values, or performance-impacting index issues. The logging mechanism makes it easy to review the differences and resolve issues.4
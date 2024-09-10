### **MongoDB Synchronization Script: Overview and Details**

#### **Purpose:**
This script is designed to automatically synchronize data between two MongoDB databases. It copies collections that do not exist in the secondary database and then uses MongoDB Change Streams to keep the data synchronized in real-time. This is particularly useful in scenarios where you need to maintain a backup or have a distributed setup where data consistency across multiple databases is required.

---

### **Inputs:**

1. **MongoDB Connection URIs:**
   - `PRIMARY_MONGO_URI`: The connection string for the primary MongoDB instance (e.g., `"mongodb://localhost:27017"`).
   - `SECONDARY_MONGO_URI`: The connection string for the secondary MongoDB instance (e.g., `"mongodb://localhost:27018"`).

2. **Database Name:**
   - `DATABASE_NAME`: The name of the database that you want to synchronize between the primary and secondary MongoDB instances (e.g., `'training'`).

3. **Logging Level (Optional):**
   - `LOGGING_LEVEL`: The logging verbosity level. This can be set to `logging.INFO` for standard logs or `logging.DEBUG` for more detailed logs.

---

### **Outputs:**

1. **Console Logs:**
   - The script outputs log messages that indicate:
     - When collections are being copied over from the primary to the secondary database.
     - When new documents are inserted, updated, replaced, or deleted in the secondary database as part of the synchronization process.
     - Any errors encountered during the synchronization process.
   - Example log output:
     ```
     2024-09-01 10:00:00 - INFO - Copying collection 'users' to secondary database...
     2024-09-01 10:00:01 - INFO - Collection 'users' copied successfully.
     2024-09-01 10:01:00 - INFO - Database change stream started...
     2024-09-01 10:01:05 - INFO - Inserted new document into 'users': {'_id': ObjectId('...'), 'name': 'John Doe', 'age': 30}
     2024-09-01 10:01:10 - INFO - Updated document in 'users': ObjectId('...') with changes: {'$set': {'age': 31}}
     ```

2. **Data Synchronization:**
   - **Initial Copy:** Any collections in the primary database that do not exist in the secondary database are copied over with all their documents.
   - **Real-time Sync:** Subsequent changes (inserts, updates, deletes) in the primary database are automatically reflected in the secondary database.

---

### **Code Workflow:**

1. **Establishing Connections:**
   - The script establishes connections to both the primary and secondary MongoDB databases using the provided connection URIs.

2. **Copying Missing Collections:**
   - The script iterates through all collections in the primary database.
   - For each collection, it checks if it exists in the secondary database. If it does not, the script copies all documents from the primary collection to the secondary collection.

3. **Starting the Change Stream:**
   - After ensuring all collections are copied, the script sets up a MongoDB Change Stream on the primary database.
   - The change stream monitors for real-time changes (inserts, updates, deletes) in any collection within the database.

4. **Synchronizing Data:**
   - When a change is detected in the primary database, the script applies the same change to the corresponding collection in the secondary database.
   - It handles insertions, updates, replacements, and deletions, ensuring data consistency between the databases.

5. **Logging:**
   - The script logs key actions and events, such as when collections are copied, when documents are synchronized, and any errors encountered.

---

### **Testing Between MongoDB Versions 4.4 and 7.0:**

#### **Test Setup:**

1. **Environment:**
   - The script was tested with:
     - **Primary Database:** MongoDB version 4.4.
     - **Secondary Database:** MongoDB version 7.0.
   - Both instances were running locally on different ports (`27017` and `27018`).

2. **Test Cases:**
   - **Initial Sync:**
     - Verified that collections from the MongoDB 4.4 instance were correctly copied to the MongoDB 7.0 instance.
     - Ensured that all documents were accurately transferred.
   - **Change Stream Synchronization:**
     - Tested inserting new documents in MongoDB 4.4 and confirmed they appeared in MongoDB 7.0.
     - Tested updating documents in MongoDB 4.4 and verified the updates were reflected in MongoDB 7.0.
     - Tested deleting documents in MongoDB 4.4 and checked that they were removed from MongoDB 7.0.

3. **Results:**
   - The script successfully synchronized collections and documents between MongoDB 4.4 and MongoDB 7.0 without any compatibility issues.
   - The change stream correctly monitored and applied changes from the primary to the secondary database, ensuring real-time data consistency.

---

### **Conclusion:**

This script provides an efficient and reliable method for synchronizing data between two MongoDB databases across different versions. It handles both the initial data transfer and ongoing synchronization, ensuring that the secondary database remains up-to-date with minimal latency. The added logging functionality offers transparency and ease of monitoring, making the script suitable for production environments where data consistency is critical.

[youtube link](https://youtu.be/nj63gjB2S6U)
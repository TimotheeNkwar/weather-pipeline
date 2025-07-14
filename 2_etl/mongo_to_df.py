import pandas as pd
from pymongo import MongoClient
from config import MONGO_URI, MONGO_DB, MONGO_COLLECTION


def read_mongo():
    """
    Retrieves all documents from the MongoDB collection and converts them into a DataFrame.

    Returns:
        pd.DataFrame: DataFrame containing all documents, or empty if no data.
    """
    try:
        # Connect to MongoDB
        client = MongoClient(MONGO_URI)
        db = client[MONGO_DB]
        collection = db[MONGO_COLLECTION]

        # Retrieve all documents, sorted by fetched_at (newest to oldest)
        cursor = collection.find().sort("fetched_at", -1)
        data = list(cursor)

        # Check if data was retrieved
        if not data:
            print("⚠️ No data found in the MongoDB collection.")
            return pd.DataFrame()

        # Convert to DataFrame
        df = pd.DataFrame(data)

        # Drop unnecessary MongoDB columns
        df.drop(columns=["_id"], inplace=True, errors="ignore")

        return df

    except Exception as e:
        print(f"❌ Error retrieving data: {str(e)}")
        return pd.DataFrame()

    finally:
        # Close the connection
        client.close()


if __name__ == "__main__":
    try:
        df = read_mongo()
        if not df.empty:
            print("✅ Data retrieved successfully:")
            print(df.head())
        else:
            print("⚠️ No results to display.")
    except Exception as e:
        print(f"❌ Error in main execution: {str(e)}")

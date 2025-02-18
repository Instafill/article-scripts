from bson import ObjectId
import os
import pymongo
from pymongo import UpdateOne

# Load environment variables
MONGO_DB = "hipa"
MONGO_HOST = "10.0.1.4"
MONGO_USER = "hipa-rw"
MONGO_PASS = "RTBhwMqTLdyopd96Cjhep"

CONTENT = "content"
SUGGESTIONS = "suggestions"
SYSTEM_SUGGESTION_TYPES = "system_suggestion_types"
BLOGS = "blogs"
ARTICLES = "articles"
CLUSTERS = "clusters"
SITES = 'sites'
USERS = 'users'
TEAMS = 'teams'
AUDITS = 'audits'

INTEGRATION_WEBSITES = 'integration_websites'
INTEGRATION_POSTS = 'integration_posts'
INTEGRATION_SYNC_METADATA = 'integration_authorize_matadata'

GITHUB_ACCESS_TOKENS = 'github_access_tokens'
GITHUB_PAGES_REPOS = 'github_pages_repos'

SUPPORTED_INTEGRATIONS = 'supported_integrations'

# MongoDB Client using pymongo
def get_mongodb():
    client = pymongo.MongoClient(
        host=MONGO_HOST,
        username=MONGO_USER,
        password=MONGO_PASS,
        authSource=MONGO_DB
    )
    try:
        client.server_info()  # This checks if the connection is successful
        print("MongoDB connection successful")
    except Exception as e:
        print(f"MongoDB connection error: {e}")
        raise
    
    return client[MONGO_DB]

db = get_mongodb()

def document_exists(collection: str, form_id: str) -> bool:
    return bool(get_document(collection, ObjectId(form_id)))

def delete_document(collection_name: str, id: ObjectId):
    collection = db[collection_name]
    collection.delete_one({"_id": id})

def delete_documents(collection_name: str, query: dict):
    collection = db[collection_name]
    collection.delete_many(query)

def insert_document(collection_name: str, document: object) -> ObjectId:
    collection = db[collection_name]
    result = collection.insert_one(document)
    return result.inserted_id

def update_document(collection_name: str, id: ObjectId, set: dict, unset: dict = None):
    update = {'$set': set}
    if unset:
        update['$unset'] = unset
    
    collection = db[collection_name]
    return collection.update_one({"_id": id}, update)

def update_documents(collection_name: str, filter: dict, update: dict):
    collection = db[collection_name]
    collection.update_many(
        filter,
        {"$set": update}
    )

def bulk_write_documents(collection_name: str, operations: list[UpdateOne]):
    result = None
    collection = db[collection_name]
    if operations:  
        result = collection.bulk_write(operations)
    
    return result

def update_one(collection_name: str, id: ObjectId, update_parameter: dict):
    collection = db[collection_name]
    collection.update_one(
        {"_id": id},
        update_parameter
    )

def get_document(collection_name: str, id: ObjectId | str, projection: dict = None):
    collection = db[collection_name]
    return collection.find_one({"_id": id}, projection)

def insert_many(collection_name: str, items):
    collection = db[collection_name]
    return collection.insert_many(items)

def find_document(collection_name: str, query: dict, projection: dict = None, sort = None):
    collection = db[collection_name]
    return collection.find_one(query, projection, sort=sort)

def find_documents(collection_name: str, query: dict, projection: dict = None):
    collection = db[collection_name]
    return collection.find(query, projection)

def find_last_document(collection_name: str, query: dict, projection: dict = None):
    collection = db[collection_name]
    last_doc = collection.find_one(query, projection, sort=[('_id', pymongo.DESCENDING)])
    return last_doc

def count_documents(collection_name: str, query: dict = {}):
    collection = db[collection_name]
    return collection.count_documents(query)

def list_documents(collection_name: str, query=None, projection=None):
    collection = db[collection_name]
    return list(collection.find(query, projection))

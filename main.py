from dotenv import load_dotenv,find_dotenv
load_dotenv(find_dotenv())
import os
import pprint
from pymongo import MongoClient

password = os.environ.get("MONGODB_PWD")



connection_string = f"mongodb+srv://Akshaygp:{password}@cluster0.4k1lcs8.mongodb.net/?retryWrites=true&w=majority"
client = MongoClient(connection_string)

dbs = client.list_database_names()
#print(dbs)

test_db = client.test
collection = test_db.list_collection_names()
print(collection)

def insert_test_doc():
    collection = test_db.test
    test_document = {
        "name":"Akshay",
        "type":"test"
    }
    insert_id = collection.insert_one(test_document).inserted_id
    print(insert_id)

#insert_test_doc()

### Creating the new database, collection and inserting the values directly through python

production = client.production
person_collection = production.person_collection

def create_documents():
    first_names = ["Akshay","Shivu","Amar","Rahul"]
    last_names = ["Smith","ABD","Kohli","Maxwell"]
    ages = [21,22,34,56]

    docs = []

    for first_name,last_name,age in zip(first_names,last_names,ages):
        doc = {"first_name":first_name,"last_name":last_name,"age":age}
        docs.append(doc)

    person_collection.insert_many(docs)

#create_documents()
printer = pprint.PrettyPrinter()

def find_all_people():
    people = person_collection.find()

    for person in people:
        printer.pprint(person)

#find_all_people()


def find_akshay():
    akshay = person_collection.find_one({"first_name":"Akshay"})
    printer.pprint(akshay)

#find_akshay()

def count_all_people():
    count = person_collection.count_documents(filter={})
    print("Number od people",count)

#count_all_people()

def get_person_by_id(person_id):
    from bson.objectid import ObjectId

    _id = ObjectId(person_id)
    person = person_collection.find_one({"_id": _id})
    printer.pprint(person)


#get_person_by_id("64bb7fcd93923a566ef6a26f")

def get_age_range(min_age,max_age):
    query = {"$and":[
        {"age":{"$gte":min_age}},
        {"age":{"$lte":max_age}}
    ]}

    people = person_collection.find(query).sort("age")
    for person in people:
        printer.pprint(person)

#get_age_range(20,30)


def project_columns():
    columns = {"_id":0,"first_name":1,"last_name":1}
    people = person_collection.find({},columns)

    for person in people:
        printer.pprint(person)

#project_columns()

def update_person_by_id(person_id):

    from bson.objectid import ObjectId
    _id = ObjectId(person_id)

    # all_updates = {
    #     "$set":{"new_field": True},
    #     "$inc":{"age":1},
    #     "$rename":{"first_name":"first","last_name":"last"}
    # }

    person_collection.update_one({"_id":_id},{"$unset":{"new_filed":""}})

#update_person_by_id("64bb7fcd93923a566ef6a270")

def replace_one(person_id):
    from bson.objectid import ObjectId
    _id = ObjectId(person_id)

    new_doc = {
        "first_name":"new first name",
        "last_name":"new last name",
        "age": 100
    }

    person_collection.replace_one({"_id":_id},new_doc)

#replace_one("64bb7fcd93923a566ef6a270")


def delete_doc_by_id(person_id):
    from bson.objectid import ObjectId
    _id = ObjectId(person_id)

    person_collection.delete_one({"_id":_id})

#delete_doc_by_id("64bb7fcd93923a566ef6a270")

address = {
    "_id":"64bb7fcd93923a566ef6a270",
    "street":"Bay street",
    "number":2706,
    "city":"San Francisco",
    "country":"America",
    "zip":"94107"
}

def add_address_embed(person_id,address):
    from bson.objectid import ObjectId
    _id = ObjectId(person_id)
    

    person_collection.update_one({"_id":_id},{"$addToSet":{"addresses":address}})


#add_address_embed("64bb7fcd93923a566ef6a26e",address)


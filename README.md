### Mongojet

[![CI](https://github.com/romis2012/mongojet/actions/workflows/python-ci.yml/badge.svg)](https://github.com/romis2012/mongojet/actions/workflows/python-ci.yml)
[![Coverage Status](https://codecov.io/gh/romis2012/mongojet/branch/master/graph/badge.svg)](https://codecov.io/gh/romis2012/mongojet)
[![PyPI version](https://badge.fury.io/py/mongojet.svg)](https://pypi.python.org/pypi/mongojet)
[![versions](https://img.shields.io/pypi/pyversions/mongojet.svg)](https://github.com/romis2012/mongojet)

Async (asyncio) MongoDB client for Python. 
It uses [Rust MongoDB driver](https://github.com/mongodb/mongo-rust-driver) and [tokio](https://github.com/tokio-rs/tokio) under the hood.
Mongojet is 2-4x faster than Motor in high concurrency scenarios.

## Requirements
- Python >= 3.8
- pymongo>=4.6.2 (only `bson` package is required)


## Installation
```
pip install mongojet
```

## Usage

Mongojet has an API similar to PyMongo/Motor (but not exactly the same)

### Creating a Client
Typically, you should create a single instance of Client per application/process.
All client options should be passed via [MongoDB connection string](https://www.mongodb.com/docs/manual/reference/connection-string/).
```python
from mongojet import create_client, ReadPreference

client = await create_client('mongodb://localhost:27017/test_database?maxPoolSize=16')
```

### Getting a Database
default database
```python
db = client.get_default_database()
```
database with specific name
```python
db = client.get_database('test_database')
```
database with specific name and options
```python
db = client.get_database('test_database', read_preference=ReadPreference(mode='secondaryPreferred'))
```

### Getting a Collection
```python
collection = db['test_collection']
```
with options
```python
collection = db.get_collection('test_collection', read_preference=ReadPreference(mode='secondary'))
```

### Inserting documents
`insert_one`
```python
document = {'key': 'value'}
result = await collection.insert_one(document)
print(result)
#> {'inserted_id': ObjectId('...')}
```
`insert_many`
```python
documents = [{'i': i} for i in range(1000)]
result = await collection.insert_many(documents)
print(len(result['inserted_ids']))
#> 1000
```
### Find documents

`find_one` (to get a single document)
```python
document = await collection.find_one({'i': 1})
print(document)
#> {'_id': ObjectId('...'), 'i': 1}
```

`find` (to get cursor which is an async iterator)
```python
cursor = await collection.find({'i': {'$gt': 5}}, sort={'i': -1}, limit=10)
```
you can iterate over the cursor using the `async for` loop
```python
async for document in cursor:
    print(document)
```
or collect cursor to list of documents using `to_list` method
```python
documents = await cursor.to_list()
```

`find_many` (to get list of documents in single batch)
```python
documents = await collection.find_many({'i': {'$gt': 5}}, sort={'i': -1}, limit=10)
```

### Counting documents
```python
n = await collection.count_documents({'i': {'$gte': 500}})
print(n)
#> 500
```

### Aggregating documents
```python
cursor = await collection.aggregate(pipeline=[
    {'$match': {'i': {'$gte': 10}}},
    {'$sort': {'i': 1}},
    {'$limit': 10},
])
documents = await cursor.to_list()
print(documents)
```

### Updating documents

`replace_one`
```python
result = await collection.replace_one(filter={'i': 5}, replacement={'i': 5000})
print(result)
#> {'matched_count': 1, 'modified_count': 1, 'upserted_id': None}
```

`update_one`
```python
result = await collection.update_one(filter={'i': 5}, update={'$set': {'i': 5000}}, upsert=True)
print(result)
#> {'matched_count': 0, 'modified_count': 0, 'upserted_id': ObjectId('...')}
```

`update_many`
```python
result = await collection.update_many(filter={'i': {'$gte': 100}}, update={'$set': {'i': 0}})
print(result)
#> {'matched_count': 900, 'modified_count': 900, 'upserted_id': None}
```

### Deleting documents
`delete_one`
```python
result = await collection.delete_one(filter={'i': 5})
print(result)
#> {'deleted_count': 1}
```
`delete_many`
```python
result = await collection.delete_many(filter={'i': {'$gt': 5}})
print(result)
#> {'deleted_count': 94}
```

### Working with GridFS
```python
bucket = db.gridfs_bucket(bucket_name="images")

with open('/path/to/my/awesome/image.png', mode='rb') as file:
    data = file.read()
    result = await bucket.put(data, filename='image.png', content_type='image/png')
    file_id = result['file_id']

with open('/path/to/my/awesome/image_copy.png', mode='wb') as file:
    data = await bucket.get_by_id(file_id)
    file.write(data)

await bucket.delete(file_id)
```


# pylint:disable=redefined-outer-name

import asyncio
import pytest
import pytest_asyncio
from motor.motor_asyncio import AsyncIOMotorClient
from motor.core import AgnosticDatabase as MotorDatabase
from pymongo import AsyncMongoClient
from pymongo.asynchronous.database import AsyncDatabase


from mongojet import create_client, Database as MongojetDatabase

DB_URL = 'mongodb://127.0.0.1:27017/benchmarks?maxPoolSize=16'
READ_COLLECTION_NAME = 'collection_to_read'
UPDATE_COLLECTION_NAME = 'collection_to_update'

DOC_NUM = 128


@pytest.fixture(scope="session")
def event_loop():
    loop = asyncio.new_event_loop()
    try:
        yield loop
    finally:
        loop.close()


@pytest_asyncio.fixture(scope='session', autouse=True)
async def init_db(event_loop):
    client = await create_client(DB_URL)
    db = client.get_default_database()
    collection = db[READ_COLLECTION_NAME]
    await collection.delete_many({})
    for i in range(DOC_NUM):
        await collection.insert_one({'a': i})

    yield

    await db.drop()
    await client.close()


@pytest_asyncio.fixture(scope='session')
async def mongojet_client():
    c = await create_client(DB_URL)
    yield c
    await c.close()


@pytest_asyncio.fixture(scope='session')
async def motor_client(event_loop):
    c = AsyncIOMotorClient(DB_URL, io_loop=event_loop)
    yield c
    c.close()


@pytest_asyncio.fixture(scope='session')
async def pymongo_client():
    c = AsyncMongoClient(DB_URL)
    yield c
    await c.close()


@pytest_asyncio.fixture(scope='session')
async def mongojet_db(mongojet_client):
    return mongojet_client.get_default_database()


@pytest_asyncio.fixture(scope='session')
async def motor_db(motor_client):
    return motor_client.get_default_database()


@pytest_asyncio.fixture(scope='session')
async def pymongo_db(pymongo_client: AsyncMongoClient):
    return pymongo_client.get_default_database()


@pytest_asyncio.fixture(scope='session')
async def mongojet_read_collection(mongojet_db: MongojetDatabase):
    return mongojet_db[READ_COLLECTION_NAME]


@pytest_asyncio.fixture(scope='session')
async def motor_read_collection(motor_db: MotorDatabase):
    return motor_db[READ_COLLECTION_NAME]


@pytest_asyncio.fixture(scope='session')
async def pymongo_read_collection(pymongo_db: AsyncDatabase):
    return pymongo_db[READ_COLLECTION_NAME]


@pytest_asyncio.fixture(scope='function')
async def mongojet_update_collection(mongojet_db: MongojetDatabase):
    c = mongojet_db[UPDATE_COLLECTION_NAME]
    await c.delete_many({})
    return c


@pytest_asyncio.fixture(scope='function')
async def motor_update_collection(motor_db: MotorDatabase):
    c = motor_db[UPDATE_COLLECTION_NAME]
    await c.delete_many({})
    return c


@pytest_asyncio.fixture(scope='function')
async def pymongo_update_collection(pymongo_db: AsyncDatabase):
    c = pymongo_db[UPDATE_COLLECTION_NAME]
    await c.delete_many({})
    return c


# https://github.com/ionelmc/pytest-benchmark/issues/66
@pytest_asyncio.fixture
def aio_benchmark(benchmark, event_loop):
    def _wrapper(func, *args, **kwargs):
        if asyncio.iscoroutinefunction(func):

            def run_coro(*pos, **kwd):
                # return asyncio.get_event_loop().run_until_complete(func(*pos, **kwd))
                return event_loop.run_until_complete(func(*pos, **kwd))

            return benchmark(run_coro, *args, **kwargs)
        else:
            return benchmark(func, *args, **kwargs)

    return _wrapper

import pytest_asyncio

from mongojet import create_client


@pytest_asyncio.fixture(scope='session')
async def client():
    c = await create_client('mongodb://127.0.0.1:27117/test_db?replicaSet=rs1')
    yield c
    await c.close()


@pytest_asyncio.fixture(scope='session')
async def db(client):
    db = client.get_default_database()
    await db.drop()
    return db

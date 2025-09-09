import asyncio

import pytest
from motor.core import AgnosticCollection as MotorCollection
from pymongo.asynchronous.collection import AsyncCollection

from mongojet import Collection as MongojetCollection

ITERATIONS = 1
CONCURENCIES = [
    32,
    64,
    128,
    256,
    512,
    1024,
]


async def find(
    collection: MongojetCollection | MotorCollection | AsyncCollection,
    iterations: int,
    concurrency: int,
):
    async def fn():
        for i in range(iterations):
            await collection.find_one({'a': i})

    tasks = [fn() for _ in range(concurrency)]
    await asyncio.gather(*tasks)


async def insert(
    collection: MongojetCollection | MotorCollection | AsyncCollection,
    iterations: int,
    concurrency: int,
):
    async def fn():
        for i in range(iterations):
            await collection.insert_one({'a': i})

    tasks = [fn() for _ in range(concurrency)]
    await asyncio.gather(*tasks)


async def cursor_mongojet(
    collection: MongojetCollection,
    iterations: int,
    concurrency: int,
):
    async def fn():
        for _ in range(iterations):
            cur = await collection.aggregate(pipeline=[{'$match': {'a': {'$gt': 10}}}])
            async for _ in cur:
                pass

    tasks = [fn() for _ in range(concurrency)]
    await asyncio.gather(*tasks)


async def cursor_motor(
    collection: MotorCollection,
    iterations: int,
    concurrency: int,
):
    async def fn():
        for _ in range(iterations):
            cur = collection.aggregate(pipeline=[{'$match': {'a': {'$gt': 10}}}])
            async for _ in cur:
                pass

    tasks = [fn() for _ in range(concurrency)]
    await asyncio.gather(*tasks)


async def cursor_pymongo(
    collection: AsyncCollection,
    iterations: int,
    concurrency: int,
):
    async def fn():
        for _ in range(iterations):
            cur = await collection.aggregate(pipeline=[{'$match': {'a': {'$gt': 10}}}])
            async for _ in cur:
                pass

    tasks = [fn() for _ in range(concurrency)]
    await asyncio.gather(*tasks)


@pytest.mark.find
@pytest.mark.parametrize('concurrency', CONCURENCIES)
def test_find_one_mongojet(aio_benchmark, mongojet_read_collection, concurrency):
    aio_benchmark(find, mongojet_read_collection, ITERATIONS, concurrency)


@pytest.mark.find
@pytest.mark.parametrize('concurrency', CONCURENCIES)
def test_find_one_motor(aio_benchmark, motor_read_collection, concurrency):
    aio_benchmark(find, motor_read_collection, ITERATIONS, concurrency)


@pytest.mark.find
@pytest.mark.parametrize('concurrency', CONCURENCIES)
def test_find_one_pymongo(aio_benchmark, pymongo_read_collection, concurrency):
    aio_benchmark(find, pymongo_read_collection, ITERATIONS, concurrency)


@pytest.mark.cursor
@pytest.mark.parametrize('concurrency', CONCURENCIES)
def test_cursor_mongojet(aio_benchmark, mongojet_read_collection, concurrency):
    aio_benchmark(cursor_mongojet, mongojet_read_collection, ITERATIONS, concurrency)


@pytest.mark.cursor
@pytest.mark.parametrize('concurrency', CONCURENCIES)
def test_cursor_motor(aio_benchmark, motor_read_collection, concurrency):
    aio_benchmark(cursor_motor, motor_read_collection, ITERATIONS, concurrency)


@pytest.mark.cursor
@pytest.mark.parametrize('concurrency', CONCURENCIES)
def test_cursor_pymongo(aio_benchmark, pymongo_read_collection, concurrency):
    aio_benchmark(cursor_pymongo, pymongo_read_collection, ITERATIONS, concurrency)


@pytest.mark.insert
@pytest.mark.parametrize('concurrency', CONCURENCIES)
def test_insert_mongojet(aio_benchmark, mongojet_update_collection, concurrency):
    aio_benchmark(insert, mongojet_update_collection, ITERATIONS, concurrency)


@pytest.mark.insert
@pytest.mark.parametrize('concurrency', CONCURENCIES)
def test_insert_motor(aio_benchmark, motor_update_collection, concurrency):
    aio_benchmark(insert, motor_update_collection, ITERATIONS, concurrency)


@pytest.mark.insert
@pytest.mark.parametrize('concurrency', CONCURENCIES)
def test_insert_pymongo(aio_benchmark, pymongo_update_collection, concurrency):
    aio_benchmark(insert, pymongo_update_collection, ITERATIONS, concurrency)


# python -m pytest ./benchmarks -m find -v -s
# python -m pytest ./benchmarks -m insert -v -s
# python -m pytest ./benchmarks -m cursor -v -s

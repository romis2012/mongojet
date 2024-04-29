import itertools
import re

import pytest
from bson import SON

from mongojet import Database


@pytest.mark.asyncio
async def test_find_one(db: Database):
    collection = db['test_find_one']
    inserted_data = {'a': 1, 'b': 2, 'c': 3}
    res = await collection.insert_one(inserted_data)
    inserted_id = res['inserted_id']
    inserted_doc = {'_id': inserted_id, **inserted_data}

    doc = await collection.find_one({'_id': inserted_id})
    assert doc == inserted_doc

    doc = await collection.find_one({'_id': inserted_id}, projection={'a': 1})
    assert 'a' in doc
    assert 'b' not in doc


@pytest.mark.asyncio
async def test_distinct(db: Database):
    collection = db['test_distinct']
    await collection.insert_many([{'foo': 'bar', 'a': 1} for _ in range(3)])
    await collection.insert_many([{'foo': 'baz', 'a': 2} for _ in range(3)])
    await collection.insert_many([{'foo': 'qux', 'a': 3} for _ in range(3)])

    assert await collection.count_documents() == 9

    res = await collection.distinct('foo')
    assert sorted(res) == sorted(['bar', 'baz', 'qux'])

    res = await collection.distinct('a', filter={'foo': 'bar'})
    assert sorted(res) == [1]


@pytest.mark.asyncio
async def test_find_many_sort(db: Database):
    collection = db['test_find_many_sort']
    values = [i for i in range(5)]
    await collection.insert_many([{'a': i} for i in values])

    docs = await collection.find_many(sort={'a': 1})
    assert sorted(values) == [doc['a'] for doc in docs]

    docs = await collection.find_many(sort={'a': -1})
    assert sorted(values, reverse=True) == [doc['a'] for doc in docs]

    docs = await collection.find_many(sort=SON([('a', -1)]))
    assert sorted(values, reverse=True) == [doc['a'] for doc in docs]


@pytest.mark.asyncio
async def test_find_many_skip_take(db: Database):
    collection = db['test_find_many_skip_take']

    total = 10
    skip = 3
    limit = 5

    values = [i for i in range(total)]
    await collection.insert_many([{'a': i} for i in values])

    docs = await collection.find_many(sort={'a': 1}, skip=skip, limit=limit)
    assert sorted(values)[skip : skip + limit] == [doc['a'] for doc in docs]


@pytest.mark.asyncio
async def test_find_many_filter(db: Database):
    collection = db['test_find_many_filter']

    values = [i for i in range(10)]
    await collection.insert_many([{'a': i} for i in values])

    docs = await collection.find_many(filter={'a': {'$gt': 5}}, sort={'a': 1})
    assert sorted([v for v in values if v > 5]) == [doc['a'] for doc in docs]


@pytest.mark.asyncio
async def test_find_sort(db: Database):
    collection = db['test_find_sort']
    values = [i for i in range(5)]
    await collection.insert_many([{'a': i} for i in values])

    docs = await collection.find(sort={'a': 1})
    assert sorted(values) == [doc['a'] async for doc in docs]

    docs = await collection.find(sort={'a': -1})
    assert sorted(values, reverse=True) == [doc['a'] async for doc in docs]

    docs = await collection.find(sort=SON([('a', -1)]))
    assert sorted(values, reverse=True) == [doc['a'] async for doc in docs]


@pytest.mark.asyncio
async def test_find_skip_take(db: Database):
    collection = db['test_find_skip_take']

    total = 10
    skip = 3
    limit = 5

    values = [i for i in range(total)]
    await collection.insert_many([{'a': i} for i in values])

    docs = await collection.find(sort={'a': 1}, skip=skip, limit=limit)
    assert sorted(values)[skip : skip + limit] == [doc['a'] async for doc in docs]


@pytest.mark.asyncio
async def test_find_filter(db: Database):
    collection = db['test_find_filter']

    values = [i for i in range(10)]
    await collection.insert_many([{'a': i} for i in values])

    docs = await collection.find(filter={'a': {'$gt': 5}}, sort={'a': 1})
    assert sorted([v for v in values if v > 5]) == [doc['a'] async for doc in docs]


@pytest.mark.asyncio
async def test_find_filter_by_regex(db: Database):
    collection = db['test_find_filter_by_regex']

    values = ['Foo', 'Bar', 'Baz', 'Qux', 'Fred']
    await collection.insert_many([{'a': i} for i in values])

    docs = await collection.find(filter={'a': {'$regex': '^Ba'}})
    assert len([doc['a'] async for doc in docs]) == 2

    docs = await collection.find(filter={'a': re.compile(r'^ba', re.IGNORECASE)})
    assert len([doc['a'] async for doc in docs]) == 2


@pytest.mark.asyncio
async def test_aggregate_project(db: Database):
    collection = db['test_aggregate_project']
    await collection.insert_one({'a': 1, 'b': 2})
    cur = await collection.aggregate(
        pipeline=[{'$project': {'_id': False, 'b': False}}]
    )
    assert (await cur.to_list())[0] == {'a': 1}


@pytest.mark.asyncio
async def test_aggregate_group(db: Database):
    collection = db['test_aggregate_group']
    docs = [{'name': c, 'value': i} for c in 'abcd' for i in range(5)]

    await collection.insert_many(docs)

    cur = await collection.aggregate(
        pipeline=[
            {'$match': {'value': {'$gt': 1}}},
            {'$group': {'_id': '$name', 'total': {'$sum': '$value'}}},
            {'$project': {'name': '$_id', 'total': '$total', '_id': 0}},
            {'$sort': {'name': 1}},
        ]
    )
    data = await cur.to_list()

    docs = [doc for doc in docs if doc['value'] > 1]
    docs = sorted(docs, key=lambda doc: doc['name'])
    docs = [
        {'name': name, 'total': sum(g['value'] for g in group)}
        for name, group in itertools.groupby(docs, key=lambda doc: doc['name'])
    ]

    assert docs == data

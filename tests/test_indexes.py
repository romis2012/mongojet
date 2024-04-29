from typing import Optional

import pytest
from mongojet import Database, IndexModel, IndexModelDef, Collection, OperationFailure


async def get_index_by_name(c: Collection, name: str) -> Optional[IndexModelDef]:
    indexes = await c.list_indexes()
    indexes = [i for i in indexes if i['name'] == name]
    if indexes:
        return indexes[0]
    return None


@pytest.mark.asyncio
async def test_create_index(db: Database):
    col_name = 'test_create_index'
    collection = db[col_name]

    #
    res = await collection.create_index('field_name')
    index = await get_index_by_name(collection, res['index_name'])
    assert index['key'] == {'field_name': 1}

    #
    res = await collection.create_index({'field_name': -1})
    index = await get_index_by_name(collection, res['index_name'])
    assert index['key'] == {'field_name': -1}

    #
    res = await collection.create_index({'field1': 1, 'field2': -1})
    index = await get_index_by_name(collection, res['index_name'])
    assert index['key'] == {'field1': 1, 'field2': -1}

    #
    res = await collection.create_index([('field11', 1), ('field22', -1)])
    index = await get_index_by_name(collection, res['index_name'])
    assert index['key'] == {'field11': 1, 'field22': -1}

    ###
    await collection.drop_indexes()
    indexes = await collection.list_indexes()
    assert len(indexes) == 1  # {'_id': 1}


@pytest.mark.asyncio
async def test_create_index_with_options(db: Database):
    col_name = 'test_create_index_with_options'
    collection = db[col_name]

    key = 'key'
    unique = True
    expireAfterSeconds = 3600  # noqa
    background = True
    sparse = True

    res = await collection.create_index(
        key,
        unique=unique,
        expireAfterSeconds=expireAfterSeconds,
        background=background,
        sparse=sparse,
    )

    index = await get_index_by_name(collection, name=res['index_name'])
    assert index['key'] == {key: 1}
    assert index['unique'] == unique
    assert index['background'] == background
    assert index['expireAfterSeconds'] == expireAfterSeconds
    assert index['sparse'] == sparse


@pytest.mark.asyncio
async def test_create_partial_index(db: Database):
    col_name = 'test_create_partial_index'
    collection = db[col_name]

    key = {'key': 1}
    partialFilterExpression = {'field1': {'$gt': 0}}  # noqa

    res = await collection.create_index(
        key,
        partialFilterExpression=partialFilterExpression,
    )

    index = await get_index_by_name(collection, name=res['index_name'])
    assert index['key'] == key
    assert index['partialFilterExpression'] == partialFilterExpression


@pytest.mark.asyncio
async def test_create_text_index(db: Database):
    col_name = 'test_create_text_index'
    collection = db[col_name]

    key = [('title', 'text'), ('desc', 'text')]
    default_language = 'russian'
    weights = {'title': 1, 'desc': 2}
    language_override = 'language_field'

    res = await collection.create_index(
        key,
        default_language=default_language,
        language_override=language_override,
        weights=weights,
    )

    index = await get_index_by_name(collection, res['index_name'])
    assert index['default_language'] == default_language
    assert index['language_override'] == language_override
    assert index['weights'] == weights


@pytest.mark.asyncio
async def test_create_2d_index(db: Database):
    col_name = 'test_create_2d_index'
    collection = db[col_name]
    key = {'location': '2d'}
    bits = 32
    min = -75  # noqa
    max = 60  # noqa
    res = await collection.create_index(
        key,
        bits=bits,
        min=min,
        max=max,
    )

    index = await get_index_by_name(collection, res['index_name'])
    assert index['key'] == key
    assert index['bits'] == bits
    assert index['min'] == min
    assert index['max'] == max


@pytest.mark.asyncio
async def test_create_2dsphere_index(db: Database):
    col_name = 'test_create_2dsphere_index'
    collection = db[col_name]

    key = {'location': '2dsphere'}
    version = 3
    res = await collection.create_index(key, sphere2dIndexVersion=version)

    index = await get_index_by_name(collection, res['index_name'])
    assert index['key'] == key
    assert index['2dsphereIndexVersion'] == version


@pytest.mark.asyncio
async def test_create_hashed_index(db: Database):
    col_name = 'test_create_hashed_index'
    collection = db[col_name]

    key = {'key': 'hashed'}
    res = await collection.create_index(
        key,
    )

    index = await get_index_by_name(collection, res['index_name'])
    assert index['key'] == key


@pytest.mark.asyncio
async def test_create_indexes(db: Database):
    col_name = 'test_create_indexes'
    collection = db[col_name]

    index1 = IndexModel(
        keys={'key': 1},
        name='idx1',
        unique=True,
        expireAfterSeconds=3600,
        background=True,
    )

    index2 = IndexModel(
        keys={'key1': 1, 'key2': -1},
        name='idx2',
    )

    res = await collection.create_indexes([index1, index2])
    assert 'idx1' in res['index_names']
    assert 'idx2' in res['index_names']

    idx1 = await get_index_by_name(collection, name='idx1')
    assert idx1['key'] == {'key': 1}
    assert idx1['unique'] is True
    assert idx1['expireAfterSeconds'] == 3600
    assert idx1['background'] is True


@pytest.mark.asyncio
async def test_create_index_error(db: Database):
    col_name = 'test_create_index_error'
    collection = db[col_name]

    key = {'key1': 1, 'key2': -1}

    await collection.create_index(key, name='idx1')

    with pytest.raises(OperationFailure):
        await collection.create_index(key, name='idx2')

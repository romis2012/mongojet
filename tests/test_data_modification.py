import pytest
from bson import ObjectId
from mongojet import Database, DuplicateKeyError, WriteError


@pytest.mark.asyncio
async def test_insert_one(db: Database):
    col_name = 'test_insert_one'
    collection = db[col_name]

    _id = 100
    doc = {'_id': _id}
    res = await collection.insert_one(doc)
    assert res['inserted_id'] == _id
    assert await collection.find({'_id': _id}) is not None
    assert await collection.count_documents({}) == 1

    doc = {'foo': 'bar'}
    res = await collection.insert_one(doc)
    assert isinstance(res['inserted_id'], ObjectId)
    assert res['inserted_id'] == doc['_id']
    assert await collection.find({'_id': res['inserted_id']}) is not None
    assert await collection.count_documents() == 2


@pytest.mark.asyncio
async def test_insert_many(db: Database):
    collection = db['test_insert_many']
    count = 8

    docs = [{'_id': i} for i in range(count)]
    res = await collection.insert_many(docs)
    inserted_ids = res['inserted_ids']

    for doc in docs:
        _id = doc['_id']
        assert isinstance(_id, int)
        assert _id in inserted_ids
        assert await collection.count_documents({'_id': _id}) == 1

    docs = [{'foo': 'bar'} for _ in range(count)]
    res = await collection.insert_many(docs)
    inserted_ids = res['inserted_ids']

    for doc in docs:
        _id = doc['_id']
        assert isinstance(_id, ObjectId)
        assert _id in inserted_ids
        assert await collection.count_documents({'_id': _id}) == 1

    assert await collection.count_documents() == count * 2


@pytest.mark.asyncio
async def test_insert_error(db: Database):
    collection = db['test_insert_error']
    await collection.create_index('foo', unique=True)

    await collection.insert_one({'foo': 'bar'})
    with pytest.raises(DuplicateKeyError):
        await collection.insert_one({'foo': 'bar'})


@pytest.mark.asyncio
async def test_insert_with_validation(db: Database):
    coll_name = 'test_insert_with_validation'
    await db.create_collection(coll_name, validator={'a': {'$exists': True}})
    collection = db[coll_name]

    await collection.insert_one({'a': 1})
    with pytest.raises(WriteError):
        await collection.insert_one({'b': 1})

    await collection.insert_one({'b': 1}, bypass_document_validation=True)


@pytest.mark.asyncio
async def test_replace_one(db: Database):
    collection = db['test_replace_one']
    with pytest.raises(ValueError):
        await collection.replace_one({}, {'$set': {'a': 1}})

    await collection.insert_one({'foo': 'bar'})

    res = await collection.replace_one(
        filter={'foo': 'bar'},
        replacement={'foo': 'baz'},
    )
    assert res['matched_count'] == 1
    assert res['modified_count'] == 1
    assert res['upserted_id'] is None

    assert await collection.count_documents({'foo': 'bar'}) == 0
    assert await collection.count_documents({'foo': 'baz'}) == 1

    res = await collection.replace_one(
        filter={'foo': 'bar'},
        replacement={'foo': 'baz'},
        upsert=True,
    )
    assert res['matched_count'] == 0
    assert res['modified_count'] == 0
    assert res['upserted_id'] is not None

    assert await collection.count_documents({'foo': 'baz'}) == 2


@pytest.mark.asyncio
async def test_update_one(db: Database):
    collection = db['test_update_one']
    await collection.insert_one({'foo': 'bar'})

    with pytest.raises(ValueError):
        await collection.update_one({}, {'a': 1})

    res = await collection.update_one(
        filter={'foo': 'bar'},
        update={'$set': {'foo': 'baz'}},
    )
    assert res['matched_count'] == 1
    assert res['modified_count'] == 1
    assert res['upserted_id'] is None

    assert await collection.count_documents({'foo': 'bar'}) == 0
    assert await collection.count_documents({'foo': 'baz'}) == 1

    res = await collection.update_one(
        filter={'foo': 'bar'},
        update={'$set': {'foo': 'baz'}},
        upsert=True,
    )
    assert res['matched_count'] == 0
    assert res['modified_count'] == 0
    assert res['upserted_id'] is not None

    assert await collection.count_documents({'foo': 'baz'}) == 2


@pytest.mark.asyncio
async def test_update_many(db: Database):
    collection = db['test_update_many']
    await collection.insert_one({'a': 1})
    await collection.insert_one({'a': 2})
    await collection.insert_one({'a': 3})
    await collection.insert_one({'a': 4})
    await collection.insert_one({'a': 5})

    res = await collection.update_many(
        filter={'a': {'$gte': 3}},
        update={'$set': {'a': 0}},
    )
    assert res['matched_count'] == 3
    assert res['modified_count'] == 3
    assert res['upserted_id'] is None

    assert await collection.count_documents({'a': 0}) == 3

    res = await collection.update_many(
        filter={'a': 10},
        update={'$set': {'a': 0}},
        upsert=True,
    )
    assert res['matched_count'] == 0
    assert res['modified_count'] == 0
    assert res['upserted_id'] is not None

    assert await collection.count_documents({'a': 0}) == 4
    assert await collection.count_documents() == 6


@pytest.mark.asyncio
async def test_find_and_modify(db: Database):
    collection = db['test_find_and_modify']
    res = await collection.insert_one({'foo': 'bar'})
    inserted_id = res['inserted_id']

    doc = await collection.find_one_and_update(
        filter={'_id': inserted_id},
        update={'$set': {'foo': 'baz'}},
        return_document='before',
    )
    assert doc == {'_id': inserted_id, 'foo': 'bar'}
    assert await collection.find_one({'_id': inserted_id}) == {
        '_id': inserted_id,
        'foo': 'baz',
    }

    doc = await collection.find_one_and_update(
        filter={'_id': inserted_id},
        update={'$set': {'foo': 'qux'}},
        return_document='after',
    )
    assert doc == {'_id': inserted_id, 'foo': 'qux'}
    assert await collection.find_one({'_id': inserted_id}) == {
        '_id': inserted_id,
        'foo': 'qux',
    }

    doc = await collection.find_one_and_replace(
        filter={'_id': inserted_id},
        replacement={'foo': 'fred'},
        return_document='before',
    )
    assert doc == {'_id': inserted_id, 'foo': 'qux'}
    assert await collection.find_one({'_id': inserted_id}) == {
        '_id': inserted_id,
        'foo': 'fred',
    }

    doc = await collection.find_one_and_replace(
        filter={'_id': inserted_id},
        replacement={'foo': 'thud'},
        return_document='after',
    )
    assert doc == {'_id': inserted_id, 'foo': 'thud'}
    assert await collection.find_one({'_id': inserted_id}) == {
        '_id': inserted_id,
        'foo': 'thud',
    }

    doc = await collection.find_one_and_delete(filter={'_id': inserted_id})
    assert doc == {'_id': inserted_id, 'foo': 'thud'}
    assert await collection.count_documents() == 0


@pytest.mark.asyncio
async def test_delete_one(db: Database):
    collection = db['test_delete_one']
    await collection.insert_one({'a': 1})
    await collection.insert_one({'a': 2})
    await collection.insert_one({'a': 3})

    res = await collection.delete_one(filter={'a': 0})
    assert res['deleted_count'] == 0

    res = await collection.delete_one(filter={'a': 1})
    assert res['deleted_count'] == 1
    assert await collection.count_documents() == 2


@pytest.mark.asyncio
async def test_delete_many(db: Database):
    collection = db['test_delete_many']
    await collection.insert_many([{'a': i} for i in range(5)])

    res = await collection.delete_many(filter={'a': {'$gte': 3}})
    assert res['deleted_count'] == 2
    assert await collection.count_documents() == 3

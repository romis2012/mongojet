import pytest

from mongojet import Database, DuplicateKeyError


@pytest.mark.asyncio
async def test_transaction(db: Database):
    collection = db['test_transaction']
    await collection.create_index('a', unique=True)

    session = await db.client.start_session()

    async with await session.start_transaction():
        await collection.insert_one({'a': 1}, session=session)
        await collection.insert_one({'a': 2}, session=session)

    assert await collection.count_documents() == 2

    with pytest.raises(DuplicateKeyError):
        async with await session.start_transaction():
            await collection.insert_one({'a': 3}, session=session)
            await collection.insert_one({'a': 3}, session=session)

    assert await collection.count_documents() == 2


@pytest.mark.asyncio
async def test_transaction_find(db: Database):
    collection = db['test_transaction_find']
    session = await db.client.start_session()

    async with await session.start_transaction():
        values = [i for i in range(5)]
        await collection.insert_many([{'a': i} for i in values], session=session)

        cur = await collection.find(sort={'a': 1}, session=session)
        assert sorted(values) == [doc['a'] async for doc in cur]

        cur = await collection.find(sort={'a': 1})
        assert [] == [doc['a'] async for doc in cur]


@pytest.mark.asyncio
async def test_transaction_aggregate(db: Database):
    collection = db['test_transaction_aggregate']
    session = await db.client.start_session()

    async with await session.start_transaction():
        values = [i for i in range(5)]
        await collection.insert_many([{'a': i} for i in values], session=session)

        cur = await collection.aggregate(
            pipeline=[{"$sort": {'a': 1}}],
            session=session,
        )
        assert sorted(values) == [doc['a'] async for doc in cur]

        cur = await collection.aggregate(pipeline=[{"$sort": {'a': 1}}])
        assert [] == [doc['a'] async for doc in cur]

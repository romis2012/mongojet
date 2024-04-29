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

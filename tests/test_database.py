import pytest
from mongojet import Client, ReadConcern, WriteConcern, ReadPreference, Database


def test_get_database(client: Client):
    db_name = 'db_name'
    db1 = client.get_database(db_name)
    db2 = client[db_name]
    db3 = client.db_name

    assert db1.name == db2.name == db3.name == db_name
    assert db1.client is db2.client is db3.client


def test_database_options(client: Client):
    read_concern = ReadConcern(level='local')
    write_concern = WriteConcern(w='majority', wtimeout=360, j=True)
    read_preference = ReadPreference(
        mode='secondaryPreferred',
        tagSets=[{'one': 'two'}, {'three': 'four'}],
        maxStalenessSeconds=600,
        hedge={'enabled': True},
    )

    db_name = 'db_name'

    db = client.get_database(
        db_name,
        read_concern=read_concern,
        write_concern=write_concern,
        read_preference=read_preference,
    )

    assert db.name == db_name
    assert db.read_concern == read_concern
    assert db.write_concern == write_concern
    assert db.read_preference == read_preference


@pytest.mark.asyncio
async def test_create_collection(db: Database):
    col_name = 'test_create_collection'
    capped = True
    size = 524288000
    max = 300000  # noqa
    await db.create_collection(col_name, capped=capped, size=size, max=max)

    coll_specs = await db.list_collections(filter={'name': col_name})
    options = coll_specs[0]['options']

    assert options['capped'] == capped
    assert options['size'] == size
    assert options['max'] == max

    collection = db[col_name]
    await collection.drop()
    coll_specs = await db.list_collections(filter={'name': col_name})
    assert len(coll_specs) == 0

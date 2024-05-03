import pytest
from bson import ObjectId

from mongojet import Database, NoFile, FileExists, GridfsBucket


@pytest.mark.asyncio
async def test_gridfs(db: Database):
    bucket: GridfsBucket = db.gridfs_bucket(bucket_name="files")

    file_id = ObjectId()
    file_name = 'some name'
    file_data = b'file content'

    res = await bucket.put(data=file_data, filename=file_name, file_id=file_id)
    assert res['file_id'] == file_id

    data = await bucket.get_by_id(file_id=file_id)
    assert data == file_data

    data = await bucket.get_by_name(filename=file_name)
    assert data == file_data

    with pytest.raises(NoFile):
        await bucket.get_by_id(file_id=ObjectId())

    with pytest.raises(NoFile):
        await bucket.get_by_name(filename='qqqqqqqqqqq')

    with pytest.raises(FileExists):
        await bucket.put(data=file_data, filename=file_name, file_id=file_id)

    await bucket.delete(file_id=file_id)
    with pytest.raises(NoFile):
        await bucket.get_by_id(file_id=file_id)

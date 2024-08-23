from typing import Any, Optional

from bson import CodecOptions

from .mongojet import PyMongoError, NoFile, FileExists, DuplicateKeyError
from ._codec import Codec
from ._types import GridFsPutResult


class GridfsBucket:
    def __init__(self, core_bucket, codec_options: CodecOptions):
        self._core_bucket = core_bucket
        self._codec = Codec(codec_options)

    async def put(
        self,
        data: bytes,
        filename: Optional[str] = None,
        file_id: Optional[Any] = None,
        **metadata: Any,
    ) -> GridFsPutResult:

        options = {}
        if filename:
            options['filename'] = filename
        if file_id:
            options['file_id'] = file_id

        try:
            result = await self._core_bucket.put(
                data,
                self._codec.encode(options),
                self._codec.encode(metadata),
            )
        except DuplicateKeyError as e:
            raise FileExists(e) from e
        else:
            return self._codec.decode(result)

    async def get_by_id(self, file_id: Any) -> bytes:
        options = {'file_id': file_id}
        return await self._core_bucket.get_by_id(self._codec.encode(options))

    async def get_by_name(self, filename: Any) -> bytes:
        options = {'filename': filename}
        return await self._core_bucket.get_by_name(self._codec.encode(options))

    async def delete(self, file_id: Any) -> None:
        options = {'file_id': file_id}
        await self._core_bucket.delete(self._codec.encode(options))

import collections
from typing import TypeVar, AsyncIterator, Sequence

from bson import CodecOptions

from ._codec import Codec

T = TypeVar('T')


class Cursor(AsyncIterator[T]):
    def __init__(self, core_cursor, codec_options: CodecOptions):
        self._core_cursor = core_cursor
        self._codec = Codec(options=codec_options)
        self._buff = collections.deque()

    def __aiter__(self):
        return self

    async def __anext__(self) -> T:
        if not self._buff:
            data = await self._core_cursor.next_batch()
            if not data:
                raise StopAsyncIteration

            # "empty" mongodb document is a sequence of bytes
            # b'\x05\x00\x00\x00\x00'
            # that decodes into empty dict
            # bson.encode({}) == b'\x05\x00\x00\x00\x00'
            # bson.decode(b'\x05\x00\x00\x00\x00') == {}
            doc = self._codec.decode(data)
            if not doc:
                raise StopAsyncIteration

            self._buff.extend(list(doc.values()))

        return self._buff.popleft()

    async def to_list(self, length=None) -> Sequence[T]:
        if length is not None:
            raise ValueError(
                'Only None value is supported for partial compatibility with Motor API'
            )
        data = await self._core_cursor.collect()
        doc = self._codec.decode(data)
        if not doc:
            return []

        return list(doc.values())

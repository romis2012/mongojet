import collections
from typing import TypeVar, AsyncIterator, Sequence

from bson import CodecOptions

from ._codec import Codec

T = TypeVar('T')


class Cursor(AsyncIterator[T]):
    def __init__(self, core_cursor, codec_options: CodecOptions, batch_size=128):
        self._core_cursor = core_cursor
        self._codec = Codec(options=codec_options)
        self._batch_size = batch_size
        self._buff = collections.deque()

    def __aiter__(self):
        return self

    # async def __anext__(self) -> T:
    #     data = await self._core_cursor.next()
    #     return self._codec.decode(data)

    async def __anext__(self) -> T:
        if not self._buff:
            docs = await self._core_cursor.next_batch(self._batch_size)
            if not docs:
                raise StopAsyncIteration
            else:
                self._buff.extend([self._codec.decode(doc) for doc in docs])

        return self._buff.popleft()

    async def to_list(self, length=None) -> Sequence[T]:
        # warnings.warn(
        #     'to_list is deprecated, iterate over cursor directly instead',
        #     DeprecationWarning,
        #     stacklevel=2,
        # )

        if length is not None:
            raise ValueError(
                'Only None value is supported for partial compatibility with Motor API'
            )

        return [self._codec.decode(doc) for doc in await self._core_cursor.collect()]

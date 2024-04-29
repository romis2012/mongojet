from __future__ import annotations

from bson import CodecOptions

try:
    from typing import Unpack
except ImportError:
    from typing_extensions import Unpack

from ._types import TransactionOptions
from ._codec import Codec


class ClientSession:
    def __init__(self, core_session, codec_options: CodecOptions):
        self._core_session = core_session
        self._codec = Codec(options=codec_options)

    async def start_transaction(
        self,
        **options: Unpack[TransactionOptions],
    ) -> _TransactionContext:
        await self._core_session.start_transaction(self._codec.encode(options))
        return _TransactionContext(self)

    async def commit_transaction(self):
        await self._core_session.commit_transaction()

    async def abort_transaction(self):
        await self._core_session.abort_transaction()

    @property
    def core_session(self):
        return self._core_session


class _TransactionContext:
    def __init__(self, session: ClientSession):
        self._session = session

    async def __aenter__(self) -> _TransactionContext:
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        # TODO: check if session in_transaction?
        if exc_val is None:
            await self._session.commit_transaction()
        else:
            await self._session.abort_transaction()

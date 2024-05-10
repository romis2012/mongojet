from typing import Optional

from bson import CodecOptions

from .mongojet import core_create_client  # noqa

try:
    from typing import Unpack
except ImportError:
    from typing_extensions import Unpack

from ._database import Database
from ._types import DatabaseOptions, SessionOptions
from ._codec import Codec
from ._session import ClientSession


async def create_client(url: str, tz_aware: bool = True) -> 'Client':
    core_client = await core_create_client(url=url)
    return Client(core_client, codec_options=CodecOptions(tz_aware=tz_aware))


class Client:
    def __init__(self, core_client, codec_options: CodecOptions) -> None:
        self._codec_options = codec_options
        self._codec = Codec(options=codec_options)
        self._core_client = core_client

    def get_default_database(
        self,
        codec_options: Optional[CodecOptions] = None,
        **options: Unpack[DatabaseOptions],
    ) -> Database:
        default_database = self._core_client.default_database_name
        if default_database is None:
            raise ValueError('No default database name defined or provided.')

        if options:
            core_database = self._core_client.get_database_with_options(
                default_database,
                self._codec.encode(options),
            )
        else:
            core_database = self._core_client.get_default_database()

        return Database(
            core_database,
            codec_options=codec_options or self._codec_options,
            client=self,
        )

    def get_database(
        self,
        name: Optional[str] = None,
        codec_options: Optional[CodecOptions] = None,
        **options: Unpack[DatabaseOptions],
    ) -> Database:
        if name is None:
            return self.get_default_database(codec_options=codec_options, **options)

        if options:
            core_database = self._core_client.get_database_with_options(
                name,
                self._codec.encode(options),
            )
        else:
            core_database = self._core_client.get_database(name)

        return Database(
            core_database,
            codec_options=codec_options or self._codec_options,
            client=self,
        )

    async def start_session(self, **options: Unpack[SessionOptions]) -> ClientSession:
        core_session = await self._core_client.start_session(
            self._codec.encode(options),
        )
        return ClientSession(
            core_session,
            codec_options=self._codec_options,
        )

    async def close(self, immediate=True):
        if immediate:
            await self._core_client.shutdown_immediate()
        else:
            await self._core_client.shutdown()

    def __getitem__(self, name: str) -> Database:
        return self.get_database(name)

    def __getattr__(self, name: str) -> Database:
        if name.startswith("_"):  # pragma: no cover
            raise AttributeError(
                f"Client has no attribute {name!r}. To access the {name}"
                f" database, use client[{name!r}]."
            )
        return self.__getitem__(name)

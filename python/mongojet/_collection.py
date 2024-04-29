from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, Optional, List, Union, TYPE_CHECKING

try:
    from typing import Unpack
except ImportError:
    from typing_extensions import Unpack

from bson import CodecOptions, ObjectId

from ._session import ClientSession
from ._cursor import Cursor
from ._codec import Codec
from ._types import (
    Document,
    FindOptions,
    FindOneOptions,
    AggregateOptions,
    UpdateOptions,
    InsertOneOptions,
    InsertManyOptions,
    InsertOneResult,
    UpdateResult,
    InsertManyResult,
    DeleteResult,
    IndexKeys,
    IndexModel,
    CreateIndexOptions,
    CreateIndexResult,
    CreateIndexesResult,
    DropIndexOptions,
    ListIndexesOptions,
    IndexModelDef,
    ReplaceOptions,
    DeleteOptions,
    CountOptions,
    EstimatedCountOptions,
    FindOneAndUpdateOptions,
    FindOneAndReplaceOptions,
    FindOneAndDeleteOptions,
    DropCollectionOptions,
    ReadPreference,
    WriteConcern,
    ReadConcern,
    DistinctOptions,
)

if TYPE_CHECKING:
    from ._database import Database


# noinspection PyShadowingBuiltins
class Collection:
    def __init__(
        self,
        core_collection,
        codec_options: CodecOptions,
        database: Database,
    ):
        self._database = database
        self._codec_options = codec_options
        self._codec = Codec(options=codec_options)
        self._core_collection = core_collection

    async def find_one(
        self,
        filter: Optional[Union[Document, str]] = None,
        session: Optional[ClientSession] = None,
        **options: Unpack[FindOneOptions],
    ) -> Document:
        if filter is not None and not isinstance(filter, Mapping):
            filter = {'_id': filter}

        filter = self._codec.encode(filter)
        options = self._codec.encode(options)

        if session is None:
            result = await self._core_collection.find_one(filter, options)
        else:
            result = await self._core_collection.find_one_with_session(
                session.core_session,
                filter,
                options,
            )

        return self._codec.decode(result)

    async def find_one_and_update(
        self,
        filter: Document,
        update: Union[Document, Sequence[Document]],
        session: Optional[ClientSession] = None,
        **options: Unpack[FindOneAndUpdateOptions],
    ) -> Document:
        filter = self._codec.encode(filter, optional=False)

        if isinstance(update, Sequence):
            update = ([self._codec.encode(doc) for doc in update],)
        else:
            update = self._codec.encode(update, optional=False)

        options = self._codec.encode(options)

        if session is None:
            result = await self._core_collection.find_one_and_update(
                filter,
                update,
                options,
            )
        else:
            result = await self._core_collection.find_one_and_update_with_session(
                session.core_session,
                filter,
                update,
                options,
            )

        return self._codec.decode(result)

    async def find_one_and_replace(
        self,
        filter: Document,
        replacement: Document,
        session: Optional[ClientSession] = None,
        **options: Unpack[FindOneAndReplaceOptions],
    ) -> Document:
        filter = self._codec.encode(filter, optional=False)
        replacement = self._codec.encode(replacement, optional=False)
        options = self._codec.encode(options)

        if session is None:
            result = await self._core_collection.find_one_and_replace(
                filter,
                replacement,
                options,
            )
        else:
            result = await self._core_collection.find_one_and_replace_with_session(
                session.core_session,
                filter,
                replacement,
                options,
            )

        return self._codec.decode(result)

    async def find_one_and_delete(
        self,
        filter: Document,
        session: Optional[ClientSession] = None,
        **options: Unpack[FindOneAndDeleteOptions],
    ) -> Document:
        filter = self._codec.encode(filter, optional=False)
        options = self._codec.encode(options)

        if session is None:
            result = await self._core_collection.find_one_and_delete(
                filter,
                options,
            )
        else:
            result = await self._core_collection.find_one_and_delete_with_session(
                session.core_session,
                filter,
                options,
            )

        return self._codec.decode(result)

    async def find(
        self,
        filter: Optional[Document] = None,
        session: Optional[ClientSession] = None,
        **options: Unpack[FindOptions],
    ) -> Cursor[Document]:
        filter = self._codec.encode(filter)
        options = self._codec.encode(options)

        if session is None:
            cur = await self._core_collection.find(filter, options)
        else:
            cur = await self._core_collection.find_with_session(
                session.core_session,
                filter,
                options,
            )
        return Cursor(cur, codec_options=self._codec_options)

    async def find_many(
        self,
        filter: Optional[Document] = None,
        session: Optional[ClientSession] = None,
        **options: Unpack[FindOptions],
    ) -> List[Document]:
        filter = self._codec.encode(filter)
        options = self._codec.encode(options)

        if session is None:
            result = await self._core_collection.find_many(filter, options)
        else:
            result = await self._core_collection.find_many_with_session(
                session.core_session,
                filter,
                options,
            )

        return [self._codec.decode(d) for d in result]

    async def aggregate(
        self,
        pipeline: Sequence[Document],
        session: Optional[ClientSession] = None,
        **options: Unpack[AggregateOptions],
    ) -> Cursor[Document]:
        pipeline = [self._codec.encode(doc) for doc in pipeline]
        options = self._codec.encode(options)

        if session is None:
            cur = await self._core_collection.aggregate(pipeline, options)
        else:
            cur = await self._core_collection.aggregate_with_session(
                session.core_session,
                pipeline,
                options,
            )

        return Cursor(cur, codec_options=self._codec_options)

    async def update_one(
        self,
        filter: Document,
        update: Union[Document, Sequence[Document]],
        session: Optional[ClientSession] = None,
        **options: Unpack[UpdateOptions],
    ) -> UpdateResult:
        filter = self._codec.encode(filter, optional=False)

        if isinstance(update, Sequence):
            update = ([self._codec.encode(doc) for doc in update],)
        else:
            update = self._codec.encode(update, optional=False)

        options = self._codec.encode(options)

        if session is None:
            result = await self._core_collection.update_one(
                filter,
                update,
                options,
            )
        else:
            result = await self._core_collection.update_one_with_session(
                session.core_session,
                filter,
                update,
                options,
            )
        return self._codec.decode(result)

    async def update_many(
        self,
        filter: Document,
        update: Union[Document, Sequence[Document]],
        session: Optional[ClientSession] = None,
        **options: Unpack[UpdateOptions],
    ) -> UpdateResult:
        filter = self._codec.encode(filter, optional=False)

        if isinstance(update, Sequence):
            update = ([self._codec.encode(doc) for doc in update],)
        else:
            update = self._codec.encode(update, optional=False)

        options = self._codec.encode(options)

        if session is None:
            result = await self._core_collection.update_many(
                filter,
                update,
                options,
            )
        else:
            result = await self._core_collection.update_many_with_session(
                session.core_session,
                filter,
                update,
                options,
            )
        return self._codec.decode(result)

    async def insert_one(
        self,
        document: Document,
        session: Optional[ClientSession] = None,
        **options: Unpack[InsertOneOptions],
    ) -> InsertOneResult:

        if '_id' not in document:
            document['_id'] = ObjectId()

        document = self._codec.encode(document, optional=False)
        options = self._codec.encode(options)

        if session is None:
            result = await self._core_collection.insert_one(
                document,
                options,
            )
        else:
            result = await self._core_collection.insert_one_with_session(
                session.core_session,
                document,
                options,
            )

        return self._codec.decode(result)

    async def insert_many(
        self,
        documents: List[Document],
        session: Optional[ClientSession] = None,
        **options: Unpack[InsertManyOptions],
    ) -> InsertManyResult:

        for document in documents:
            if '_id' not in document:
                document['_id'] = ObjectId()

        documents = [self._codec.encode(doc) for doc in documents]
        options = self._codec.encode(options)

        if session is None:
            result = await self._core_collection.insert_many(
                documents,
                options,
            )
        else:
            result = await self._core_collection.insert_many_with_session(
                session.core_session,
                documents,
                options,
            )

        return self._codec.decode(result)

    async def replace_one(
        self,
        filter: Document,
        replacement: Document,
        session: Optional[ClientSession] = None,
        **options: Unpack[ReplaceOptions],
    ) -> UpdateResult:
        filter = self._codec.encode(filter, optional=False)
        replacement = self._codec.encode(replacement, optional=False)
        options = self._codec.encode(options)

        if session is None:
            result = await self._core_collection.replace_one(
                filter,
                replacement,
                options,
            )
        else:
            result = await self._core_collection.replace_one_with_session(
                session.core_session,
                filter,
                replacement,
                options,
            )

        return self._codec.decode(result)

    async def delete_one(
        self,
        filter: Document,
        session: Optional[ClientSession] = None,
        **options: Unpack[DeleteOptions],
    ) -> DeleteResult:
        filter = self._codec.encode(filter, optional=False)
        options = self._codec.encode(options)

        if session is None:
            result = await self._core_collection.delete_one(
                filter,
                options,
            )
        else:
            result = await self._core_collection.delete_one_with_session(
                session.core_session,
                filter,
                options,
            )

        return self._codec.decode(result)

    async def delete_many(
        self,
        filter: Document,
        session: Optional[ClientSession] = None,
        **options: Unpack[DeleteOptions],
    ) -> DeleteResult:
        filter = self._codec.encode(filter, optional=False)
        options = self._codec.encode(options)

        if session is None:
            result = await self._core_collection.delete_many(
                filter,
                options,
            )
        else:
            result = await self._core_collection.delete_many_with_session(
                session.core_session,
                filter,
                options,
            )

        return self._codec.decode(result)

    async def count_documents(
        self,
        filter: Optional[Document] = None,
        session: Optional[ClientSession] = None,
        **options: Unpack[CountOptions],
    ) -> int:
        filter = self._codec.encode(filter)
        options = self._codec.encode(options)

        if session is None:
            result = await self._core_collection.count_documents(
                filter,
                options,
            )
        else:
            result = await self._core_collection.count_documents_with_session(
                session.core_session,
                filter,
                options,
            )

        return result

    async def estimated_document_count(
        self,
        **options: Unpack[EstimatedCountOptions],
    ) -> int:
        result = await self._core_collection.estimated_document_count(
            self._codec.encode(options),
        )
        return result

    async def distinct(
        self,
        field_name: str,
        filter: Optional[Document] = None,
        session: Optional[ClientSession] = None,
        **options: Unpack[DistinctOptions],
    ) -> List[Any]:
        filter = self._codec.encode(filter)
        options = self._codec.encode(options)

        if session is None:
            data = await self._core_collection.distinct(
                field_name,
                filter,
                options,
            )
        else:
            data = await self._core_collection.distinct_with_session(
                session.core_session,
                field_name,
                filter,
                options,
            )

        result = self._codec.decode(data)
        return result['values']

    async def create_index(
        self,
        keys: IndexKeys,
        session: Optional[ClientSession] = None,
        **kwargs: Any,  # IndexModelDef (wo "key") and CreateIndexOptions keys
    ) -> CreateIndexResult:

        options = {}
        if "maxTimeMS" in kwargs:
            options["maxTimeMS"] = int(kwargs.pop("maxTimeMS"))
        if "comment" in kwargs:
            options["comment"] = kwargs.pop("comment")
        if "writeConcern" in kwargs:
            options["commitQuorum"] = kwargs.pop("commitQuorum")
        if "commitQuorum" in kwargs:
            options["commitQuorum"] = kwargs.pop("commitQuorum")

        model = IndexModel(keys, **kwargs)

        if session is None:
            result = await self._core_collection.create_index(
                self._codec.encode(model.document),
                self._codec.encode(options),
            )
        else:
            result = await self._core_collection.create_index_with_session(
                session.core_session,
                self._codec.encode(model.document),
                self._codec.encode(options),
            )

        return self._codec.decode(result)

    async def create_indexes(
        self,
        indexes: Sequence[IndexModel],
        session: Optional[ClientSession] = None,
        **kwargs: Unpack[CreateIndexOptions],
    ) -> CreateIndexesResult:

        indexes = [self._codec.encode(idx.document) for idx in indexes]
        options = self._codec.encode(kwargs)

        if session is None:
            result = await self._core_collection.create_indexes(
                indexes,
                options,
            )
        else:
            result = await self._core_collection.create_indexes_with_session(
                session.core_session,
                indexes,
                options,
            )

        return self._codec.decode(result)

    async def drop_index(
        self,
        name: str,
        session: Optional[ClientSession] = None,
        **kwargs: Unpack[DropIndexOptions],
    ) -> None:

        options = self._codec.encode(kwargs)

        if session is None:
            await self._core_collection.drop_index(
                name,
                options,
            )
        else:
            await self._core_collection.drop_index_with_session(
                session.core_session,
                name,
                options,
            )

    async def drop_indexes(
        self,
        session: Optional[ClientSession] = None,
        **kwargs: Unpack[DropIndexOptions],
    ) -> None:

        options = self._codec.encode(kwargs)

        if session is None:
            await self._core_collection.drop_indexes(
                options,
            )
        else:
            await self._core_collection.drop_indexes_with_session(
                session.core_session,
                options,
            )

    async def list_indexes(
        self,
        session: Optional[ClientSession] = None,
        **kwargs: Unpack[ListIndexesOptions],
    ) -> List[IndexModelDef]:

        options = self._codec.encode(kwargs)

        if session is None:
            docs = await self._core_collection.list_indexes(
                options,
            )
        else:
            docs = await self._core_collection.list_indexes_with_session(
                session.core_session,
                options,
            )

        return [self._codec.decode(doc) for doc in docs]

    async def drop(
        self,
        session: Optional[ClientSession] = None,
        **kwargs: Unpack[DropCollectionOptions],
    ) -> None:

        options = self._codec.encode(kwargs)

        if session is None:
            await self._core_collection.drop(
                options,
            )
        else:
            await self._core_collection.drop_with_session(
                session.core_session,
                options,
            )

    @property
    def read_preference(self) -> Optional[ReadPreference]:
        data = self._core_collection.read_preference()
        return self._codec.decode(data)

    @property
    def write_concern(self) -> Optional[WriteConcern]:
        data = self._core_collection.write_concern()
        return self._codec.decode(data)

    @property
    def read_concern(self) -> Optional[ReadConcern]:
        data = self._core_collection.read_concern()
        return self._codec.decode(data)

    @property
    def name(self) -> str:
        return self._core_collection.name

    @property
    def full_name(self) -> str:
        return self._core_collection.full_name

    @property
    def database(self) -> Database:
        return self._database

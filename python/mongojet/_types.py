from enum import IntEnum
from typing import (
    TypedDict,
    Literal,
    Optional,
    Sequence,
    Mapping,
    Any,
    Union,
    Tuple,
    Dict,
)

try:
    from typing import Required
except ImportError:
    from typing_extensions import Required

Document = Dict[str, Any]

ReadConcernLevel = Literal[
    'local',
    'majority',
    'linearizable',
    'available',
    'snapshot',
]

ReadPreferenceMode = Literal[
    'primary',
    'secondary',
    'primaryPreferred',
    'secondaryPreferred',
    'nearest',
]


class ReadConcern(TypedDict):
    level: ReadConcernLevel


class WriteConcern(TypedDict, total=False):
    w: Optional[Union[int, Literal['majority']]]
    wtimeout: Optional[int]
    j: Optional[bool]


class HedgedReadOptions(TypedDict):
    enabled: bool


class ReadPreference(TypedDict, total=False):
    mode: Required[ReadPreferenceMode]
    tagSets: Optional[Sequence[Mapping[str, Any]]]
    maxStalenessSeconds: Optional[int]
    hedge: Optional[HedgedReadOptions]


class DatabaseOptions(TypedDict, total=False):
    read_concern: Optional[ReadConcern]
    write_concern: Optional[WriteConcern]
    read_preference: Optional[ReadPreference]


class CollectionOptions(TypedDict, total=False):
    read_concern: Optional[ReadConcern]
    write_concern: Optional[WriteConcern]
    read_preference: Optional[ReadPreference]


CursorType = Literal[
    'tailable',
    'nonTailable',
    'tailableAwait',
]


class CollationStrength(IntEnum):
    PRIMARY = 1
    SECONDARY = 2
    TERTIARY = 3
    QUATERNARY = 4
    IDENTICAL = 5


CollationCaseFirst = Literal['upper', 'lower', 'off']
CollationAlternate = Literal['non-ignorable', 'shifted']
CollationMaxVariable = Literal['punct', 'space']


class Collation(TypedDict, total=False):
    locale: str
    strength: Optional[CollationStrength]
    caseLevel: Optional[bool]
    caseFirst: Optional[CollationCaseFirst]
    numericOrdering: Optional[bool]
    alternate: Optional[CollationAlternate]
    maxVariable: Optional[CollationMaxVariable]
    normalization: Optional[bool]
    backwards: Optional[bool]


class FindOptions(TypedDict, total=False):
    sort: Optional[Document]
    projection: Optional[Document]
    skip: Optional[int]
    limit: Optional[int]
    cursor_type: Optional[CursorType]
    no_cursor_timeout: Optional[bool]
    allow_partial_results: Optional[bool]
    batch_size: Optional[int]
    max_time_ms: Optional[int]
    allow_disk_use: Optional[bool]
    max: Optional[Document]
    min: Optional[Document]
    hint: Optional[Union[str, Document]]
    collation: Optional[Collation]
    comment: Optional[Union[str, Document]]
    max_await_time_ms: Optional[int]
    max_scan: Optional[int]
    read_concern: Optional[ReadConcern]
    read_preference: Optional[ReadPreference]
    return_key: Optional[bool]
    show_record_id: Optional[bool]
    let: Optional[Document]


class FindOneOptions(TypedDict, total=False):
    sort: Optional[Document]
    projection: Optional[Document]
    skip: Optional[int]
    allow_partial_results: Optional[bool]
    max_time_ms: Optional[int]
    max: Optional[Document]
    min: Optional[Document]
    hint: Optional[Union[str, Document]]
    collation: Optional[Collation]
    comment: Optional[Union[str, Document]]
    max_scan: Optional[int]
    read_concern: Optional[ReadConcern]
    read_preference: Optional[ReadPreference]
    return_key: Optional[bool]
    show_record_id: Optional[bool]
    let: Optional[Document]


class FindOneAndUpdateOptions(TypedDict, total=False):
    sort: Optional[Document]
    projection: Optional[Document]
    upsert: Optional[bool]
    return_document: Optional[Literal['after', 'before']]
    array_filters: Optional[Sequence[Document]]
    hint: Optional[Union[str, Document]]
    collation: Optional[Collation]
    bypass_document_validation: Optional[bool]
    max_time_ms: Optional[int]
    write_concern: Optional[WriteConcern]
    let: Optional[Document]
    comment: Optional[Any]


class FindOneAndReplaceOptions(TypedDict, total=False):
    sort: Optional[Document]
    projection: Optional[Document]
    upsert: Optional[bool]
    return_document: Optional[Literal['after', 'before']]
    hint: Optional[Union[str, Document]]
    collation: Optional[Collation]
    bypass_document_validation: Optional[bool]
    max_time_ms: Optional[int]
    write_concern: Optional[WriteConcern]
    let: Optional[Document]
    comment: Optional[Any]


class FindOneAndDeleteOptions(TypedDict, total=False):
    sort: Optional[Document]
    projection: Optional[Document]
    hint: Optional[Union[str, Document]]
    collation: Optional[Collation]
    max_time_ms: Optional[int]
    write_concern: Optional[WriteConcern]
    let: Optional[Document]
    comment: Optional[Any]


class AggregateOptions(TypedDict, total=False):
    bypass_document_validation: Optional[bool]
    batch_size: Optional[int]
    max_time_ms: Optional[int]
    allow_disk_use: Optional[bool]
    hint: Optional[Union[str, Document]]
    collation: Optional[Collation]
    comment: Optional[Union[str, Document]]
    max_await_time_ms: Optional[int]
    read_concern: Optional[ReadConcern]
    read_preference: Optional[ReadPreference]
    write_concern: Optional[WriteConcern]
    let: Optional[Document]


class UpdateOptions(TypedDict, total=False):
    upsert: Optional[bool]
    bypass_document_validation: Optional[bool]
    collation: Optional[Collation]
    array_filters: Optional[Sequence[Document]]
    hint: Optional[Union[str, Document]]
    write_concern: Optional[WriteConcern]
    let: Optional[Document]
    comment: Optional[Any]


class ReplaceOptions(TypedDict, total=False):
    upsert: Optional[bool]
    bypass_document_validation: Optional[bool]
    collation: Optional[Collation]
    hint: Optional[Union[str, Document]]
    write_concern: Optional[WriteConcern]
    let: Optional[Document]
    comment: Optional[Any]


class InsertOneOptions(TypedDict, total=False):
    bypass_document_validation: Optional[bool]
    write_concern: Optional[WriteConcern]
    comment: Optional[Any]


class InsertManyOptions(TypedDict, total=False):
    ordered: Optional[bool]
    bypass_document_validation: Optional[bool]
    write_concern: Optional[WriteConcern]
    comment: Optional[Any]


class DeleteOptions(TypedDict, total=False):
    collation: Optional[Collation]
    hint: Optional[Union[str, Document]]
    write_concern: Optional[WriteConcern]
    let: Optional[Document]
    comment: Optional[Any]


class CountOptions(TypedDict, total=False):
    skip: Optional[int]
    limit: Optional[int]
    max_time_ms: Optional[int]
    hint: Optional[Union[str, Document]]
    collation: Optional[Collation]
    read_preference: Optional[ReadPreference]
    read_concern: Optional[ReadConcern]
    comment: Optional[Any]


class EstimatedCountOptions(TypedDict, total=False):
    max_time_ms: Optional[int]
    read_preference: Optional[ReadPreference]
    read_concern: Optional[ReadConcern]
    comment: Optional[Any]


class DistinctOptions(TypedDict, total=False):
    max_time_ms: Optional[int]
    read_preference: Optional[ReadPreference]
    read_concern: Optional[ReadConcern]
    collation: Optional[Collation]
    comment: Optional[Any]


class TransactionOptions(TypedDict, total=False):
    read_concern: Optional[ReadConcern]
    write_concern: Optional[WriteConcern]
    read_preference: Optional[ReadPreference]
    max_commit_time_ms: Optional[int]


class SessionOptions(TypedDict, total=False):
    causal_consistency: Optional[bool]
    default_transaction_options: Optional[TransactionOptions]
    snapshot: Optional[bool]


class UpdateResult(TypedDict):
    matched_count: int
    modified_count: int
    upserted_id: Any


class InsertOneResult(TypedDict):
    inserted_id: Any


class InsertManyResult(TypedDict):
    inserted_ids: Sequence[Any]


class DeleteResult(TypedDict):
    deleted_count: int


IndexModelDef = TypedDict(
    'IndexModelDef',
    {
        'key': Required[Document],
        'name': Optional[str],
        'unique': Optional[bool],
        'background': Optional[bool],
        'expireAfterSeconds': Optional[int],
        'sparse': Optional[bool],
        'storageEngine': Optional[Document],
        'v': Optional[int],
        'default_language': Optional[str],
        'language_override': Optional[str],
        'textIndexVersion': Optional[int],
        'weights': Optional[Document],
        '2dsphereIndexVersion': Optional[int],
        'bits': Optional[int],
        'min': Optional[int],
        'max': Optional[int],
        'bucketSize': Optional[int],
        'partialFilterExpression': Optional[Document],
        'collation': Optional[Collation],
        'wildcardProjection': Optional[Document],
        'hidden': Optional[bool],
        'clustered': Optional[bool],
    },
)


CommitQuorum = Union[
    Literal['votingMembers', 'majority'],
    str,  # replica set tag name
    int,  # nodes
]


class CreateIndexOptions(TypedDict, total=False):
    maxTimeMS: Optional[int]
    comment: Optional[Any]  # Document | str
    writeConcern: Optional[WriteConcern]
    commitQuorum: Optional[CommitQuorum]


class DropIndexOptions(TypedDict, total=False):
    maxTimeMS: Optional[int]
    comment: Optional[Any]  # Document | str
    writeConcern: Optional[WriteConcern]


class ListIndexesOptions(TypedDict, total=False):
    maxTimeMS: Optional[int]
    comment: Optional[Any]  # Document | str
    batchSize: Optional[int]


# class IndexOptions(IndexModelDef, CreateIndexOptions):
#     pass


class CreateIndexResult(TypedDict):
    index_name: str


class CreateIndexesResult(TypedDict):
    index_names: Sequence[str]


IndexList = Union[
    Sequence[Union[str, Tuple[str, Union[int, str, Mapping[str, Any]]]]],
    Mapping[str, Any],
]
Sort = IndexList

IndexKeys = Union[str, IndexList]


class IndexModel:

    __slots__ = ("__document",)

    def __init__(self, keys: IndexKeys, **kwargs: Any) -> None:
        from ._helpers import create_index_model

        self.__document = create_index_model(keys, **kwargs)

    @property
    def document(self) -> IndexModelDef:
        return self.__document


class DropCollectionOptions(TypedDict, total=False):
    write_concern: Optional[WriteConcern]


class IndexOptionDefaults(TypedDict):
    storageEngine: Document


class TimeseriesOptions(TypedDict, total=False):
    timeField: str
    metaField: Optional[str]
    granularity: Optional[Literal['seconds', 'minutes', 'hours']]
    bucketMaxSpanSeconds: Optional[int]
    bucketRoundingSeconds: Optional[int]


class ChangeStreamPreAndPostImages(TypedDict):
    enabled: bool


class ClusteredIndex(TypedDict, total=False):
    key: Document
    unique: bool
    name: Optional[str]
    v: Optional[int]  # currently must be 2 if provided.


class CreateCollectionOptions(TypedDict, total=False):
    capped: Optional[bool]
    size: Optional[int]
    max: Optional[int]
    storageEngine: Optional[Document]
    validator: Optional[Document]
    validationLevel: Optional[Literal['off', 'strict', 'moderate']]
    validationAction: Optional[Literal['error', 'warn']]
    viewOn: Optional[str]
    pipeline: Optional[Sequence[Document]]
    collation: Optional[Collation]
    writeConcern: Optional[WriteConcern]
    indexOptionDefaults: Optional[IndexOptionDefaults]
    timeseries: Optional[TimeseriesOptions]
    expireAfterSeconds: Optional[int]
    changeStreamPreAndPostImages: Optional[ChangeStreamPreAndPostImages]
    clusteredIndex: Optional[ClusteredIndex]
    comment: Optional[Any]


class ListCollectionsOptions(TypedDict, total=False):
    batchSize: Optional[int]
    comment: Optional[Any]


class CollectionSpecification(TypedDict, total=False):
    name: str
    type: Literal['collection', 'view', 'timeseries']
    options: CreateCollectionOptions
    info: dict
    idIndex: Optional[Document]


class RunCommandOptions(TypedDict, total=False):
    read_preference: Optional[ReadPreference]


class GridFsBucketOptions(TypedDict, total=False):
    bucket_name: Optional[str]
    chunk_size_bytes: Optional[int]
    write_concern: Optional[WriteConcern]
    read_concern: Optional[ReadConcern]
    read_preference: Optional[ReadPreference]


# class GridFsPutOptions(TypedDict, total=False):
#     file_id: Optional[Any]
#     filename: Optional[str]


class GridFsPutResult(TypedDict, total=False):
    file_id: Any


class DropDatabaseOptions(TypedDict, total=False):
    write_concern: Optional[WriteConcern]

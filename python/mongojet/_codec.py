import re
from typing import Any, Dict, Optional, Mapping

import bson
from bson import CodecOptions

DEFAULT_CODEC_OPTIONS = CodecOptions(tz_aware=True)


class Codec:
    def __init__(self, options: CodecOptions):
        self._options = options

    def encode(
        self,
        doc: Optional[Mapping[str, Any]],
        optional=True,
    ) -> Optional[bytes]:
        if doc is None:
            return None

        # if optional and doc=={}
        if optional and not doc:
            return None

        return bson.BSON.encode(doc, codec_options=self._options)

    def decode(
        self,
        data: Optional[bytes],
    ) -> Optional[Dict[str, Any]]:
        if data is None:
            return None

        doc = bson.BSON(data).decode(codec_options=self._options)

        return doc


def encode(
    doc: Optional[Mapping[str, Any]],
    codec_options=None,
    amend_keys=False,
    optional=True,
) -> Optional[bytes]:
    """
    DEPRECATED
    """
    # ???
    # if not doc:
    #     return None

    if doc is None:
        return None

    # if optional and doc=={}
    if optional and not doc:
        return None

    if amend_keys:
        doc = dict_keys_to_camel_case(doc)

    return bson.BSON.encode(
        doc,
        codec_options=codec_options or DEFAULT_CODEC_OPTIONS,
    )


def decode(
    data: Optional[bytes],
    codec_options=None,
    amend_keys=False,
) -> Optional[Dict[str, Any]]:
    if data is None:
        return None

    doc = bson.BSON(data).decode(
        codec_options=codec_options or DEFAULT_CODEC_OPTIONS,
    )

    if amend_keys:
        doc = dict_keys_to_snake_case(doc)

    return doc


def to_camel_case(s: str) -> str:
    s = ''.join(word.capitalize() for word in s.split('_'))
    return s[:1].lower() + s[1:]


re_caps = re.compile(r'(?<!^)(?=[A-Z])')


def to_snake_case(s: str) -> str:
    return re_caps.sub('_', s).lower()


def dict_keys_to_camel_case(d: Mapping[str, Any]) -> dict:
    return {to_camel_case(k): v for k, v in d.items()}


def dict_keys_to_snake_case(d: Mapping[str, Any]) -> dict:
    return {to_snake_case(k): v for k, v in d.items()}

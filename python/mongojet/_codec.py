from typing import Any, Dict, Optional, Mapping

import bson
from bson import CodecOptions


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

        # return bson.BSON.encode(doc, codec_options=self._options)
        return bson.encode(doc, codec_options=self._options)

    def decode(
        self,
        data: Optional[bytes],
    ) -> Optional[Dict[str, Any]]:
        if data is None:
            return None

        # doc = bson.BSON(data).decode(codec_options=self._options)
        doc = bson.decode(data, codec_options=self._options)

        return doc

"""
Copied from `pymongo.helpers`
"""

from __future__ import annotations

from collections import abc
from typing import Union, Sequence, Mapping, Any, Optional, TYPE_CHECKING

from bson import SON

if TYPE_CHECKING:
    from ._types import IndexList, IndexKeys, IndexModelDef

ASCENDING = 1
"""Ascending sort order."""
DESCENDING = -1
"""Descending sort order."""

# _Sort = Union[
#     Sequence[Union[str, Tuple[str, Union[int, str, Mapping[str, Any]]]]],
#     Mapping[str, Any],
# ]
# _Hint = Union[str, _Sort]


def create_index_model(key_or_list: IndexKeys, **kwargs: Any) -> 'IndexModelDef':
    # fix arg types
    if 'expireAfterSeconds' in kwargs:
        kwargs['expireAfterSeconds'] = int(kwargs.pop('expireAfterSeconds'))

    # fix key names
    if 'sphere2dIndexVersion' in kwargs:
        kwargs['2dsphereIndexVersion'] = kwargs.pop('sphere2dIndexVersion')

    keys = _index_list(key_or_list)

    if kwargs.get('name') is None:
        kwargs['name'] = _gen_index_name(keys)

    index_model = {'key': _index_document(keys), **kwargs}

    return index_model  # type:ignore


def _gen_index_name(keys: IndexList) -> str:
    """Generate an index name from the set of fields it is over."""
    return "_".join(["{}_{}".format(*item) for item in keys])


def _index_list(
    key_or_list: IndexKeys, direction: Optional[Union[int, str]] = None
) -> Sequence[tuple[str, Union[int, str, Mapping[str, Any]]]]:
    """Helper to generate a list of (key, direction) pairs.

    Takes such a list, or a single key, or a single key and direction.
    """
    if direction is not None:
        if not isinstance(key_or_list, str):
            raise TypeError("Expected a string and a direction")
        return [(key_or_list, direction)]
    else:
        if isinstance(key_or_list, str):
            return [(key_or_list, ASCENDING)]
        elif isinstance(key_or_list, abc.ItemsView):
            return list(key_or_list)  # type: ignore[arg-type]
        elif isinstance(key_or_list, abc.Mapping):
            return list(key_or_list.items())
        elif not isinstance(key_or_list, (list, tuple)):
            raise TypeError(
                "if no direction is specified, key_or_list must be an instance of list"
            )
        values: list[tuple[str, int]] = []
        for item in key_or_list:
            if isinstance(item, str):
                item = (item, ASCENDING)  # noqa: PLW2901
            values.append(item)
        return values


def _index_document(index_list: IndexList) -> SON[str, Any]:
    """Helper to generate an index specifying document.

    Takes a list of (key, direction) pairs.
    """
    if not isinstance(index_list, (list, tuple, abc.Mapping)):
        raise TypeError(
            "must use a dictionary or a list of (key, direction) pairs, not: "
            + repr(index_list)
        )
    if not len(index_list):
        raise ValueError("key_or_list must not be empty")

    index: SON[str, Any] = SON()

    if isinstance(index_list, abc.Mapping):
        for key in index_list:
            value = index_list[key]
            _validate_index_key_pair(key, value)
            index[key] = value
    else:
        for item in index_list:
            if isinstance(item, str):
                item = (item, ASCENDING)  # noqa: PLW2901
            key, value = item
            _validate_index_key_pair(key, value)
            index[key] = value
    return index


def _validate_index_key_pair(key: Any, value: Any) -> None:
    if not isinstance(key, str):
        raise TypeError("first item in each key pair must be an instance of str")
    if not isinstance(value, (str, int, abc.Mapping)):
        raise TypeError(
            "second item in each key pair must be 1, -1, "
            "'2d', or another valid MongoDB index specifier."
        )

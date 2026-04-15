from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="RestoreRequest")


@_attrs_define
class RestoreRequest:
    """Request body for `restore` (rewind a branch in-place to a specific LSN).

    Exactly one of `lsn` or `snapshot_id` must be provided.
    A backup branch is automatically created before the rewind.

        Attributes:
            lsn (int | None | Unset): LSN to restore to. Must be ≤ the branch's current head LSN.
            snapshot_id (None | str | Unset): Snapshot ID to restore to. The snapshot's LSN will be used.
    """

    lsn: int | None | Unset = UNSET
    snapshot_id: None | str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        lsn: int | None | Unset
        if isinstance(self.lsn, Unset):
            lsn = UNSET
        else:
            lsn = self.lsn

        snapshot_id: None | str | Unset
        if isinstance(self.snapshot_id, Unset):
            snapshot_id = UNSET
        else:
            snapshot_id = self.snapshot_id

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if lsn is not UNSET:
            field_dict["lsn"] = lsn
        if snapshot_id is not UNSET:
            field_dict["snapshot_id"] = snapshot_id

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)

        def _parse_lsn(data: object) -> int | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(int | None | Unset, data)

        lsn = _parse_lsn(d.pop("lsn", UNSET))

        def _parse_snapshot_id(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        snapshot_id = _parse_snapshot_id(d.pop("snapshot_id", UNSET))

        restore_request = cls(
            lsn=lsn,
            snapshot_id=snapshot_id,
        )

        restore_request.additional_properties = d
        return restore_request

    @property
    def additional_keys(self) -> list[str]:
        return list(self.additional_properties.keys())

    def __getitem__(self, key: str) -> Any:
        return self.additional_properties[key]

    def __setitem__(self, key: str, value: Any) -> None:
        self.additional_properties[key] = value

    def __delitem__(self, key: str) -> None:
        del self.additional_properties[key]

    def __contains__(self, key: str) -> bool:
        return key in self.additional_properties

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

T = TypeVar("T", bound="PageSnapshotRecordItemsItem")


@_attrs_define
class PageSnapshotRecordItemsItem:
    """
    Attributes:
        branch_id (str):
        created_at (int):
        lsn (int):
        snapshot_id (str):
    """

    branch_id: str
    created_at: int
    lsn: int
    snapshot_id: str
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        branch_id = self.branch_id

        created_at = self.created_at

        lsn = self.lsn

        snapshot_id = self.snapshot_id

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "branch_id": branch_id,
                "created_at": created_at,
                "lsn": lsn,
                "snapshot_id": snapshot_id,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        branch_id = d.pop("branch_id")

        created_at = d.pop("created_at")

        lsn = d.pop("lsn")

        snapshot_id = d.pop("snapshot_id")

        page_snapshot_record_items_item = cls(
            branch_id=branch_id,
            created_at=created_at,
            lsn=lsn,
            snapshot_id=snapshot_id,
        )

        page_snapshot_record_items_item.additional_properties = d
        return page_snapshot_record_items_item

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

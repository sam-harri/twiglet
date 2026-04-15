from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="BranchInfo")


@_attrs_define
class BranchInfo:
    """
    Attributes:
        branch_id (str):
        created_at (int):
        head_lsn (int):
        fork_lsn (int | None | Unset):
        parent_id (None | str | Unset):
    """

    branch_id: str
    created_at: int
    head_lsn: int
    fork_lsn: int | None | Unset = UNSET
    parent_id: None | str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        branch_id = self.branch_id

        created_at = self.created_at

        head_lsn = self.head_lsn

        fork_lsn: int | None | Unset
        if isinstance(self.fork_lsn, Unset):
            fork_lsn = UNSET
        else:
            fork_lsn = self.fork_lsn

        parent_id: None | str | Unset
        if isinstance(self.parent_id, Unset):
            parent_id = UNSET
        else:
            parent_id = self.parent_id

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "branch_id": branch_id,
                "created_at": created_at,
                "head_lsn": head_lsn,
            }
        )
        if fork_lsn is not UNSET:
            field_dict["fork_lsn"] = fork_lsn
        if parent_id is not UNSET:
            field_dict["parent_id"] = parent_id

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        branch_id = d.pop("branch_id")

        created_at = d.pop("created_at")

        head_lsn = d.pop("head_lsn")

        def _parse_fork_lsn(data: object) -> int | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(int | None | Unset, data)

        fork_lsn = _parse_fork_lsn(d.pop("fork_lsn", UNSET))

        def _parse_parent_id(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        parent_id = _parse_parent_id(d.pop("parent_id", UNSET))

        branch_info = cls(
            branch_id=branch_id,
            created_at=created_at,
            head_lsn=head_lsn,
            fork_lsn=fork_lsn,
            parent_id=parent_id,
        )

        branch_info.additional_properties = d
        return branch_info

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

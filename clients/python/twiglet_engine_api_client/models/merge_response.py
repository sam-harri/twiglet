from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

if TYPE_CHECKING:
    from ..models.branch_info import BranchInfo


T = TypeVar("T", bound="MergeResponse")


@_attrs_define
class MergeResponse:
    """Response returned after a successful merge.

    Attributes:
        branch (BranchInfo):
        objects_merged (int): Number of object records copied from source onto target
    """

    branch: BranchInfo
    objects_merged: int
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        branch = self.branch.to_dict()

        objects_merged = self.objects_merged

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "branch": branch,
                "objects_merged": objects_merged,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.branch_info import BranchInfo

        d = dict(src_dict)
        branch = BranchInfo.from_dict(d.pop("branch"))

        objects_merged = d.pop("objects_merged")

        merge_response = cls(
            branch=branch,
            objects_merged=objects_merged,
        )

        merge_response.additional_properties = d
        return merge_response

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

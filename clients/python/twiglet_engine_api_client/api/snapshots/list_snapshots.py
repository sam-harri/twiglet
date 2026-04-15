from http import HTTPStatus
from typing import Any
from urllib.parse import quote

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.error_envelope import ErrorEnvelope
from ...models.page_snapshot_record import PageSnapshotRecord
from ...types import UNSET, Response, Unset


def _get_kwargs(
    project_id: str,
    branch_id: str,
    *,
    cursor: str | Unset = UNSET,
    limit: int | Unset = UNSET,
) -> dict[str, Any]:

    params: dict[str, Any] = {}

    params["cursor"] = cursor

    params["limit"] = limit

    params = {k: v for k, v in params.items() if v is not UNSET and v is not None}

    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": "/projects/{project_id}/branches/{branch_id}/snapshots".format(
            project_id=quote(str(project_id), safe=""),
            branch_id=quote(str(branch_id), safe=""),
        ),
        "params": params,
    }

    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> ErrorEnvelope | PageSnapshotRecord | None:
    if response.status_code == 200:
        response_200 = PageSnapshotRecord.from_dict(response.json())

        return response_200

    if response.status_code == 404:
        response_404 = ErrorEnvelope.from_dict(response.json())

        return response_404

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[ErrorEnvelope | PageSnapshotRecord]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    project_id: str,
    branch_id: str,
    *,
    client: AuthenticatedClient,
    cursor: str | Unset = UNSET,
    limit: int | Unset = UNSET,
) -> Response[ErrorEnvelope | PageSnapshotRecord]:
    """List snapshots for a branch with cursor-based pagination.

    Args:
        project_id (str):
        branch_id (str):
        cursor (str | Unset):
        limit (int | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[ErrorEnvelope | PageSnapshotRecord]
    """

    kwargs = _get_kwargs(
        project_id=project_id,
        branch_id=branch_id,
        cursor=cursor,
        limit=limit,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    project_id: str,
    branch_id: str,
    *,
    client: AuthenticatedClient,
    cursor: str | Unset = UNSET,
    limit: int | Unset = UNSET,
) -> ErrorEnvelope | PageSnapshotRecord | None:
    """List snapshots for a branch with cursor-based pagination.

    Args:
        project_id (str):
        branch_id (str):
        cursor (str | Unset):
        limit (int | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        ErrorEnvelope | PageSnapshotRecord
    """

    return sync_detailed(
        project_id=project_id,
        branch_id=branch_id,
        client=client,
        cursor=cursor,
        limit=limit,
    ).parsed


async def asyncio_detailed(
    project_id: str,
    branch_id: str,
    *,
    client: AuthenticatedClient,
    cursor: str | Unset = UNSET,
    limit: int | Unset = UNSET,
) -> Response[ErrorEnvelope | PageSnapshotRecord]:
    """List snapshots for a branch with cursor-based pagination.

    Args:
        project_id (str):
        branch_id (str):
        cursor (str | Unset):
        limit (int | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[ErrorEnvelope | PageSnapshotRecord]
    """

    kwargs = _get_kwargs(
        project_id=project_id,
        branch_id=branch_id,
        cursor=cursor,
        limit=limit,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    project_id: str,
    branch_id: str,
    *,
    client: AuthenticatedClient,
    cursor: str | Unset = UNSET,
    limit: int | Unset = UNSET,
) -> ErrorEnvelope | PageSnapshotRecord | None:
    """List snapshots for a branch with cursor-based pagination.

    Args:
        project_id (str):
        branch_id (str):
        cursor (str | Unset):
        limit (int | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        ErrorEnvelope | PageSnapshotRecord
    """

    return (
        await asyncio_detailed(
            project_id=project_id,
            branch_id=branch_id,
            client=client,
            cursor=cursor,
            limit=limit,
        )
    ).parsed

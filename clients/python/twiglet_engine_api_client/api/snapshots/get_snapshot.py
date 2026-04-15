from http import HTTPStatus
from typing import Any
from urllib.parse import quote

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.error_envelope import ErrorEnvelope
from ...models.snapshot_response import SnapshotResponse
from ...types import Response


def _get_kwargs(
    project_id: str,
    branch_id: str,
    snapshot_name: str,
) -> dict[str, Any]:

    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": "/projects/{project_id}/branches/{branch_id}/snapshots/{snapshot_name}".format(
            project_id=quote(str(project_id), safe=""),
            branch_id=quote(str(branch_id), safe=""),
            snapshot_name=quote(str(snapshot_name), safe=""),
        ),
    }

    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> ErrorEnvelope | SnapshotResponse | None:
    if response.status_code == 200:
        response_200 = SnapshotResponse.from_dict(response.json())

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
) -> Response[ErrorEnvelope | SnapshotResponse]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    project_id: str,
    branch_id: str,
    snapshot_name: str,
    *,
    client: AuthenticatedClient,
) -> Response[ErrorEnvelope | SnapshotResponse]:
    """Get a snapshot by name.

    Args:
        project_id (str):
        branch_id (str):
        snapshot_name (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[ErrorEnvelope | SnapshotResponse]
    """

    kwargs = _get_kwargs(
        project_id=project_id,
        branch_id=branch_id,
        snapshot_name=snapshot_name,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    project_id: str,
    branch_id: str,
    snapshot_name: str,
    *,
    client: AuthenticatedClient,
) -> ErrorEnvelope | SnapshotResponse | None:
    """Get a snapshot by name.

    Args:
        project_id (str):
        branch_id (str):
        snapshot_name (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        ErrorEnvelope | SnapshotResponse
    """

    return sync_detailed(
        project_id=project_id,
        branch_id=branch_id,
        snapshot_name=snapshot_name,
        client=client,
    ).parsed


async def asyncio_detailed(
    project_id: str,
    branch_id: str,
    snapshot_name: str,
    *,
    client: AuthenticatedClient,
) -> Response[ErrorEnvelope | SnapshotResponse]:
    """Get a snapshot by name.

    Args:
        project_id (str):
        branch_id (str):
        snapshot_name (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[ErrorEnvelope | SnapshotResponse]
    """

    kwargs = _get_kwargs(
        project_id=project_id,
        branch_id=branch_id,
        snapshot_name=snapshot_name,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    project_id: str,
    branch_id: str,
    snapshot_name: str,
    *,
    client: AuthenticatedClient,
) -> ErrorEnvelope | SnapshotResponse | None:
    """Get a snapshot by name.

    Args:
        project_id (str):
        branch_id (str):
        snapshot_name (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        ErrorEnvelope | SnapshotResponse
    """

    return (
        await asyncio_detailed(
            project_id=project_id,
            branch_id=branch_id,
            snapshot_name=snapshot_name,
            client=client,
        )
    ).parsed

from http import HTTPStatus
from typing import Any
from urllib.parse import quote

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.error_envelope import ErrorEnvelope
from ...models.restore_request import RestoreRequest
from ...models.restore_response import RestoreResponse
from ...types import Response


def _get_kwargs(
    project_id: str,
    branch_id: str,
    *,
    body: RestoreRequest,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/projects/{project_id}/branches/{branch_id}/restore".format(
            project_id=quote(str(project_id), safe=""),
            branch_id=quote(str(branch_id), safe=""),
        ),
    }

    _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> ErrorEnvelope | RestoreResponse | None:
    if response.status_code == 200:
        response_200 = RestoreResponse.from_dict(response.json())

        return response_200

    if response.status_code == 400:
        response_400 = ErrorEnvelope.from_dict(response.json())

        return response_400

    if response.status_code == 404:
        response_404 = ErrorEnvelope.from_dict(response.json())

        return response_404

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[ErrorEnvelope | RestoreResponse]:
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
    body: RestoreRequest,
) -> Response[ErrorEnvelope | RestoreResponse]:
    """Rewind a branch in-place to a previous LSN, preserving its current state in a backup branch.

     The same `branch_id` is kept; its internal node is atomically swapped to a new node
    capped at `lsn`. A backup branch is automatically created as a child of `branch_id`
    pointing at the pre-restore node, so no history is lost.

    The backup branch prevents `branch_id` from being deleted until the backup is
    explicitly removed. Provide exactly one of `lsn` or `snapshot_id`.

    Args:
        project_id (str):
        branch_id (str):
        body (RestoreRequest): Request body for `restore` (rewind a branch in-place to a specific
            LSN).

            Exactly one of `lsn` or `snapshot_id` must be provided.
            A backup branch is automatically created before the rewind.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[ErrorEnvelope | RestoreResponse]
    """

    kwargs = _get_kwargs(
        project_id=project_id,
        branch_id=branch_id,
        body=body,
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
    body: RestoreRequest,
) -> ErrorEnvelope | RestoreResponse | None:
    """Rewind a branch in-place to a previous LSN, preserving its current state in a backup branch.

     The same `branch_id` is kept; its internal node is atomically swapped to a new node
    capped at `lsn`. A backup branch is automatically created as a child of `branch_id`
    pointing at the pre-restore node, so no history is lost.

    The backup branch prevents `branch_id` from being deleted until the backup is
    explicitly removed. Provide exactly one of `lsn` or `snapshot_id`.

    Args:
        project_id (str):
        branch_id (str):
        body (RestoreRequest): Request body for `restore` (rewind a branch in-place to a specific
            LSN).

            Exactly one of `lsn` or `snapshot_id` must be provided.
            A backup branch is automatically created before the rewind.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        ErrorEnvelope | RestoreResponse
    """

    return sync_detailed(
        project_id=project_id,
        branch_id=branch_id,
        client=client,
        body=body,
    ).parsed


async def asyncio_detailed(
    project_id: str,
    branch_id: str,
    *,
    client: AuthenticatedClient,
    body: RestoreRequest,
) -> Response[ErrorEnvelope | RestoreResponse]:
    """Rewind a branch in-place to a previous LSN, preserving its current state in a backup branch.

     The same `branch_id` is kept; its internal node is atomically swapped to a new node
    capped at `lsn`. A backup branch is automatically created as a child of `branch_id`
    pointing at the pre-restore node, so no history is lost.

    The backup branch prevents `branch_id` from being deleted until the backup is
    explicitly removed. Provide exactly one of `lsn` or `snapshot_id`.

    Args:
        project_id (str):
        branch_id (str):
        body (RestoreRequest): Request body for `restore` (rewind a branch in-place to a specific
            LSN).

            Exactly one of `lsn` or `snapshot_id` must be provided.
            A backup branch is automatically created before the rewind.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[ErrorEnvelope | RestoreResponse]
    """

    kwargs = _get_kwargs(
        project_id=project_id,
        branch_id=branch_id,
        body=body,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    project_id: str,
    branch_id: str,
    *,
    client: AuthenticatedClient,
    body: RestoreRequest,
) -> ErrorEnvelope | RestoreResponse | None:
    """Rewind a branch in-place to a previous LSN, preserving its current state in a backup branch.

     The same `branch_id` is kept; its internal node is atomically swapped to a new node
    capped at `lsn`. A backup branch is automatically created as a child of `branch_id`
    pointing at the pre-restore node, so no history is lost.

    The backup branch prevents `branch_id` from being deleted until the backup is
    explicitly removed. Provide exactly one of `lsn` or `snapshot_id`.

    Args:
        project_id (str):
        branch_id (str):
        body (RestoreRequest): Request body for `restore` (rewind a branch in-place to a specific
            LSN).

            Exactly one of `lsn` or `snapshot_id` must be provided.
            A backup branch is automatically created before the rewind.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        ErrorEnvelope | RestoreResponse
    """

    return (
        await asyncio_detailed(
            project_id=project_id,
            branch_id=branch_id,
            client=client,
            body=body,
        )
    ).parsed

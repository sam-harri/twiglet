from http import HTTPStatus
from typing import Any, cast
from urllib.parse import quote

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.error_envelope import ErrorEnvelope
from ...types import Response


def _get_kwargs(
    project_id: str,
    branch_id: str,
    path: str,
) -> dict[str, Any]:

    _kwargs: dict[str, Any] = {
        "method": "delete",
        "url": "/projects/{project_id}/branches/{branch_id}/objects/{path}".format(
            project_id=quote(str(project_id), safe=""),
            branch_id=quote(str(branch_id), safe=""),
            path=quote(str(path), safe=""),
        ),
    }

    return _kwargs


def _parse_response(*, client: AuthenticatedClient | Client, response: httpx.Response) -> Any | ErrorEnvelope | None:
    if response.status_code == 204:
        response_204 = cast(Any, None)
        return response_204

    if response.status_code == 404:
        response_404 = ErrorEnvelope.from_dict(response.json())

        return response_404

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(*, client: AuthenticatedClient | Client, response: httpx.Response) -> Response[Any | ErrorEnvelope]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    project_id: str,
    branch_id: str,
    path: str,
    *,
    client: AuthenticatedClient,
) -> Response[Any | ErrorEnvelope]:
    """Delete an object from a branch.

     Writes a tombstone at the current LSN, making the object invisible on
    this branch without affecting ancestor branches.

    Args:
        project_id (str):
        branch_id (str):
        path (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | ErrorEnvelope]
    """

    kwargs = _get_kwargs(
        project_id=project_id,
        branch_id=branch_id,
        path=path,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    project_id: str,
    branch_id: str,
    path: str,
    *,
    client: AuthenticatedClient,
) -> Any | ErrorEnvelope | None:
    """Delete an object from a branch.

     Writes a tombstone at the current LSN, making the object invisible on
    this branch without affecting ancestor branches.

    Args:
        project_id (str):
        branch_id (str):
        path (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | ErrorEnvelope
    """

    return sync_detailed(
        project_id=project_id,
        branch_id=branch_id,
        path=path,
        client=client,
    ).parsed


async def asyncio_detailed(
    project_id: str,
    branch_id: str,
    path: str,
    *,
    client: AuthenticatedClient,
) -> Response[Any | ErrorEnvelope]:
    """Delete an object from a branch.

     Writes a tombstone at the current LSN, making the object invisible on
    this branch without affecting ancestor branches.

    Args:
        project_id (str):
        branch_id (str):
        path (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | ErrorEnvelope]
    """

    kwargs = _get_kwargs(
        project_id=project_id,
        branch_id=branch_id,
        path=path,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    project_id: str,
    branch_id: str,
    path: str,
    *,
    client: AuthenticatedClient,
) -> Any | ErrorEnvelope | None:
    """Delete an object from a branch.

     Writes a tombstone at the current LSN, making the object invisible on
    this branch without affecting ancestor branches.

    Args:
        project_id (str):
        branch_id (str):
        path (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | ErrorEnvelope
    """

    return (
        await asyncio_detailed(
            project_id=project_id,
            branch_id=branch_id,
            path=path,
            client=client,
        )
    ).parsed

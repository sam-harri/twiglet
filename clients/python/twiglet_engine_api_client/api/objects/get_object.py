from http import HTTPStatus
from typing import Any, cast
from urllib.parse import quote

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.error_envelope import ErrorEnvelope
from ...types import UNSET, Response, Unset


def _get_kwargs(
    project_id: str,
    branch_id: str,
    path: str,
    *,
    at_lsn: int | Unset = UNSET,
) -> dict[str, Any]:

    params: dict[str, Any] = {}

    params["at_lsn"] = at_lsn

    params = {k: v for k, v in params.items() if v is not UNSET and v is not None}

    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": "/projects/{project_id}/branches/{branch_id}/objects/{path}".format(
            project_id=quote(str(project_id), safe=""),
            branch_id=quote(str(branch_id), safe=""),
            path=quote(str(path), safe=""),
        ),
        "params": params,
    }

    return _kwargs


def _parse_response(*, client: AuthenticatedClient | Client, response: httpx.Response) -> Any | ErrorEnvelope | None:
    if response.status_code == 200:
        response_200 = cast(Any, None)
        return response_200

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
    at_lsn: int | Unset = UNSET,
) -> Response[Any | ErrorEnvelope]:
    """Download an object from a branch.

     Returns the raw object bytes with `Content-Type` and `Content-Length` headers.
    Use `HEAD` on the same path to retrieve these headers without the body.

    Args:
        project_id (str):
        branch_id (str):
        path (str):
        at_lsn (int | Unset):

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
        at_lsn=at_lsn,
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
    at_lsn: int | Unset = UNSET,
) -> Any | ErrorEnvelope | None:
    """Download an object from a branch.

     Returns the raw object bytes with `Content-Type` and `Content-Length` headers.
    Use `HEAD` on the same path to retrieve these headers without the body.

    Args:
        project_id (str):
        branch_id (str):
        path (str):
        at_lsn (int | Unset):

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
        at_lsn=at_lsn,
    ).parsed


async def asyncio_detailed(
    project_id: str,
    branch_id: str,
    path: str,
    *,
    client: AuthenticatedClient,
    at_lsn: int | Unset = UNSET,
) -> Response[Any | ErrorEnvelope]:
    """Download an object from a branch.

     Returns the raw object bytes with `Content-Type` and `Content-Length` headers.
    Use `HEAD` on the same path to retrieve these headers without the body.

    Args:
        project_id (str):
        branch_id (str):
        path (str):
        at_lsn (int | Unset):

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
        at_lsn=at_lsn,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    project_id: str,
    branch_id: str,
    path: str,
    *,
    client: AuthenticatedClient,
    at_lsn: int | Unset = UNSET,
) -> Any | ErrorEnvelope | None:
    """Download an object from a branch.

     Returns the raw object bytes with `Content-Type` and `Content-Length` headers.
    Use `HEAD` on the same path to retrieve these headers without the body.

    Args:
        project_id (str):
        branch_id (str):
        path (str):
        at_lsn (int | Unset):

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
            at_lsn=at_lsn,
        )
    ).parsed

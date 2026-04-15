from http import HTTPStatus
from typing import Any
from urllib.parse import quote

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.branch_response import BranchResponse
from ...models.create_branch_request import CreateBranchRequest
from ...models.error_envelope import ErrorEnvelope
from ...types import Response


def _get_kwargs(
    project_id: str,
    *,
    body: CreateBranchRequest,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/projects/{project_id}/branches".format(
            project_id=quote(str(project_id), safe=""),
        ),
    }

    _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> BranchResponse | ErrorEnvelope | None:
    if response.status_code == 201:
        response_201 = BranchResponse.from_dict(response.json())

        return response_201

    if response.status_code == 404:
        response_404 = ErrorEnvelope.from_dict(response.json())

        return response_404

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[BranchResponse | ErrorEnvelope]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    project_id: str,
    *,
    client: AuthenticatedClient,
    body: CreateBranchRequest,
) -> Response[BranchResponse | ErrorEnvelope]:
    """Fork a new branch from an existing branch.

     The new branch inherits all objects visible on the source branch at the
    current LSN. Writes to the new branch are independent of the source.

    Args:
        project_id (str):
        body (CreateBranchRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[BranchResponse | ErrorEnvelope]
    """

    kwargs = _get_kwargs(
        project_id=project_id,
        body=body,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    project_id: str,
    *,
    client: AuthenticatedClient,
    body: CreateBranchRequest,
) -> BranchResponse | ErrorEnvelope | None:
    """Fork a new branch from an existing branch.

     The new branch inherits all objects visible on the source branch at the
    current LSN. Writes to the new branch are independent of the source.

    Args:
        project_id (str):
        body (CreateBranchRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        BranchResponse | ErrorEnvelope
    """

    return sync_detailed(
        project_id=project_id,
        client=client,
        body=body,
    ).parsed


async def asyncio_detailed(
    project_id: str,
    *,
    client: AuthenticatedClient,
    body: CreateBranchRequest,
) -> Response[BranchResponse | ErrorEnvelope]:
    """Fork a new branch from an existing branch.

     The new branch inherits all objects visible on the source branch at the
    current LSN. Writes to the new branch are independent of the source.

    Args:
        project_id (str):
        body (CreateBranchRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[BranchResponse | ErrorEnvelope]
    """

    kwargs = _get_kwargs(
        project_id=project_id,
        body=body,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    project_id: str,
    *,
    client: AuthenticatedClient,
    body: CreateBranchRequest,
) -> BranchResponse | ErrorEnvelope | None:
    """Fork a new branch from an existing branch.

     The new branch inherits all objects visible on the source branch at the
    current LSN. Writes to the new branch are independent of the source.

    Args:
        project_id (str):
        body (CreateBranchRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        BranchResponse | ErrorEnvelope
    """

    return (
        await asyncio_detailed(
            project_id=project_id,
            client=client,
            body=body,
        )
    ).parsed

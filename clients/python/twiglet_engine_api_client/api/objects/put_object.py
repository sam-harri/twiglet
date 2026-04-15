from http import HTTPStatus
from typing import Any
from urllib.parse import quote

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.error_envelope import ErrorEnvelope
from ...models.put_object_response import PutObjectResponse
from ...types import Response


def _get_kwargs(
    project_id: str,
    branch_id: str,
    path: str,
    *,
    body: str,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "put",
        "url": "/projects/{project_id}/branches/{branch_id}/objects/{path}".format(
            project_id=quote(str(project_id), safe=""),
            branch_id=quote(str(branch_id), safe=""),
            path=quote(str(path), safe=""),
        ),
    }

    _kwargs["content"] = body.payload

    headers["Content-Type"] = "application/octet-stream"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> ErrorEnvelope | PutObjectResponse | None:
    if response.status_code == 200:
        response_200 = PutObjectResponse.from_dict(response.json())

        return response_200

    if response.status_code == 201:
        response_201 = PutObjectResponse.from_dict(response.json())

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
) -> Response[ErrorEnvelope | PutObjectResponse]:
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
    body: str,
) -> Response[ErrorEnvelope | PutObjectResponse]:
    """Write an object to a branch.

     Creates or overwrites the object at the given path. The request body is
    the raw object content. Set `Content-Type` to the object's MIME type.

    Returns `201 Created` for a new object and `200 OK` for an overwrite.

    Args:
        project_id (str):
        branch_id (str):
        path (str):
        body (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[ErrorEnvelope | PutObjectResponse]
    """

    kwargs = _get_kwargs(
        project_id=project_id,
        branch_id=branch_id,
        path=path,
        body=body,
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
    body: str,
) -> ErrorEnvelope | PutObjectResponse | None:
    """Write an object to a branch.

     Creates or overwrites the object at the given path. The request body is
    the raw object content. Set `Content-Type` to the object's MIME type.

    Returns `201 Created` for a new object and `200 OK` for an overwrite.

    Args:
        project_id (str):
        branch_id (str):
        path (str):
        body (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        ErrorEnvelope | PutObjectResponse
    """

    return sync_detailed(
        project_id=project_id,
        branch_id=branch_id,
        path=path,
        client=client,
        body=body,
    ).parsed


async def asyncio_detailed(
    project_id: str,
    branch_id: str,
    path: str,
    *,
    client: AuthenticatedClient,
    body: str,
) -> Response[ErrorEnvelope | PutObjectResponse]:
    """Write an object to a branch.

     Creates or overwrites the object at the given path. The request body is
    the raw object content. Set `Content-Type` to the object's MIME type.

    Returns `201 Created` for a new object and `200 OK` for an overwrite.

    Args:
        project_id (str):
        branch_id (str):
        path (str):
        body (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[ErrorEnvelope | PutObjectResponse]
    """

    kwargs = _get_kwargs(
        project_id=project_id,
        branch_id=branch_id,
        path=path,
        body=body,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    project_id: str,
    branch_id: str,
    path: str,
    *,
    client: AuthenticatedClient,
    body: str,
) -> ErrorEnvelope | PutObjectResponse | None:
    """Write an object to a branch.

     Creates or overwrites the object at the given path. The request body is
    the raw object content. Set `Content-Type` to the object's MIME type.

    Returns `201 Created` for a new object and `200 OK` for an overwrite.

    Args:
        project_id (str):
        branch_id (str):
        path (str):
        body (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        ErrorEnvelope | PutObjectResponse
    """

    return (
        await asyncio_detailed(
            project_id=project_id,
            branch_id=branch_id,
            path=path,
            client=client,
            body=body,
        )
    ).parsed

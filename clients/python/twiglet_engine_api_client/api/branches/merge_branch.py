from http import HTTPStatus
from typing import Any
from urllib.parse import quote

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.error_envelope import ErrorEnvelope
from ...models.merge_request import MergeRequest
from ...models.merge_response import MergeResponse
from ...types import Response


def _get_kwargs(
    project_id: str,
    branch_id: str,
    *,
    body: MergeRequest,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/projects/{project_id}/branches/{branch_id}/merge".format(
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
) -> ErrorEnvelope | MergeResponse | None:
    if response.status_code == 200:
        response_200 = MergeResponse.from_dict(response.json())

        return response_200

    if response.status_code == 404:
        response_404 = ErrorEnvelope.from_dict(response.json())

        return response_404

    if response.status_code == 409:
        response_409 = ErrorEnvelope.from_dict(response.json())

        return response_409

    if response.status_code == 422:
        response_422 = ErrorEnvelope.from_dict(response.json())

        return response_422

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[ErrorEnvelope | MergeResponse]:
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
    body: MergeRequest,
) -> Response[ErrorEnvelope | MergeResponse]:
    """Merge a source branch into its direct parent.

     Replays all object writes that `source_branch_id` made since it was forked
    onto `target_branch_id`. The source branch must be a **direct child** of the
    target and must have **no children** of its own. After a successful merge,
    the source branch is unchanged and may be deleted by the caller.

    All copies are applied atomically (all-or-nothing). Tombstones are
    propagated, so deletes on the source branch will take effect on the target.

    Args:
        project_id (str):
        branch_id (str):
        body (MergeRequest): Request body for merging a source branch into its parent target
            branch.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[ErrorEnvelope | MergeResponse]
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
    body: MergeRequest,
) -> ErrorEnvelope | MergeResponse | None:
    """Merge a source branch into its direct parent.

     Replays all object writes that `source_branch_id` made since it was forked
    onto `target_branch_id`. The source branch must be a **direct child** of the
    target and must have **no children** of its own. After a successful merge,
    the source branch is unchanged and may be deleted by the caller.

    All copies are applied atomically (all-or-nothing). Tombstones are
    propagated, so deletes on the source branch will take effect on the target.

    Args:
        project_id (str):
        branch_id (str):
        body (MergeRequest): Request body for merging a source branch into its parent target
            branch.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        ErrorEnvelope | MergeResponse
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
    body: MergeRequest,
) -> Response[ErrorEnvelope | MergeResponse]:
    """Merge a source branch into its direct parent.

     Replays all object writes that `source_branch_id` made since it was forked
    onto `target_branch_id`. The source branch must be a **direct child** of the
    target and must have **no children** of its own. After a successful merge,
    the source branch is unchanged and may be deleted by the caller.

    All copies are applied atomically (all-or-nothing). Tombstones are
    propagated, so deletes on the source branch will take effect on the target.

    Args:
        project_id (str):
        branch_id (str):
        body (MergeRequest): Request body for merging a source branch into its parent target
            branch.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[ErrorEnvelope | MergeResponse]
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
    body: MergeRequest,
) -> ErrorEnvelope | MergeResponse | None:
    """Merge a source branch into its direct parent.

     Replays all object writes that `source_branch_id` made since it was forked
    onto `target_branch_id`. The source branch must be a **direct child** of the
    target and must have **no children** of its own. After a successful merge,
    the source branch is unchanged and may be deleted by the caller.

    All copies are applied atomically (all-or-nothing). Tombstones are
    propagated, so deletes on the source branch will take effect on the target.

    Args:
        project_id (str):
        branch_id (str):
        body (MergeRequest): Request body for merging a source branch into its parent target
            branch.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        ErrorEnvelope | MergeResponse
    """

    return (
        await asyncio_detailed(
            project_id=project_id,
            branch_id=branch_id,
            client=client,
            body=body,
        )
    ).parsed

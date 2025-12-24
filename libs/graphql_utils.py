"""
GraphQL utilities for Lakeflow connectors.

Provides lightweight GraphQL client built on requests library,
following the same patterns as existing REST API connectors.
"""

import requests
import time
from typing import Any, Optional


class GraphQLClient:
    """
    Lightweight GraphQL client using requests library.

    Provides GraphQL query execution with error handling, retries,
    and rate limiting support.
    """

    def __init__(self, endpoint: str, headers: dict[str, str]):
        """
        Initialize GraphQL client.

        Args:
            endpoint: GraphQL API endpoint URL
            headers: HTTP headers including authorization
        """
        self.endpoint = endpoint
        self.headers = headers

    def execute(
        self, query: str, variables: Optional[dict[str, Any]] = None
    ) -> dict[str, Any]:
        """
        Execute a GraphQL query or mutation.

        Args:
            query: GraphQL query or mutation string
            variables: Optional variables for the query

        Returns:
            Response data as dictionary

        Raises:
            GraphQLError: If GraphQL returns errors
            RuntimeError: If HTTP request fails
        """
        payload = {"query": query}
        if variables:
            payload["variables"] = variables

        response = requests.post(
            self.endpoint, json=payload, headers=self.headers, timeout=30
        )

        if response.status_code != 200:
            raise RuntimeError(
                f"GraphQL HTTP error: {response.status_code} {response.text}"
            )

        result = response.json()

        # GraphQL returns 200 even with errors - check errors in response
        handle_graphql_errors(result)

        return result

    def execute_with_retry(
        self,
        query: str,
        variables: Optional[dict[str, Any]] = None,
        max_retries: int = 3,
        backoff_factor: float = 2.0,
    ) -> dict[str, Any]:
        """
        Execute a GraphQL query with exponential backoff retry.

        Args:
            query: GraphQL query or mutation string
            variables: Optional variables for the query
            max_retries: Maximum number of retry attempts
            backoff_factor: Multiplier for exponential backoff

        Returns:
            Response data as dictionary

        Raises:
            GraphQLError: If GraphQL returns errors after all retries
            RuntimeError: If HTTP request fails after all retries
        """
        last_exception = None

        for attempt in range(max_retries + 1):
            try:
                return self.execute(query, variables)
            except (RuntimeError, requests.exceptions.RequestException) as e:
                last_exception = e
                if attempt < max_retries:
                    wait_time = backoff_factor**attempt
                    time.sleep(wait_time)
                    continue
                # Last attempt failed
                break

        # All retries exhausted
        raise RuntimeError(
            f"GraphQL request failed after {max_retries} retries: {last_exception}"
        )


class GraphQLError(Exception):
    """Exception raised for GraphQL errors."""

    def __init__(self, errors: list[dict[str, Any]]):
        self.errors = errors
        error_messages = [e.get("message", str(e)) for e in errors]
        super().__init__(f"GraphQL errors: {'; '.join(error_messages)}")


def handle_graphql_errors(response: dict[str, Any]) -> None:
    """
    Parse GraphQL response and raise exception if errors exist.

    GraphQL APIs return 200 status code even when there are errors,
    with errors included in the response body.

    Args:
        response: GraphQL response dictionary

    Raises:
        GraphQLError: If response contains errors
    """
    if "errors" in response and response["errors"]:
        raise GraphQLError(response["errors"])


def extract_cursor(response: dict[str, Any], path: list[str]) -> Optional[str]:
    """
    Extract cursor value from nested GraphQL response.

    Helper function to navigate nested response structure and
    extract pagination cursor.

    Args:
        response: GraphQL response dictionary
        path: List of keys to navigate (e.g., ["data", "items", "cursor"])

    Returns:
        Cursor value as string, or None if not found

    Example:
        cursor = extract_cursor(response, ["data", "boards", "pageInfo", "endCursor"])
    """
    current = response
    for key in path:
        if not isinstance(current, dict) or key not in current:
            return None
        current = current[key]
    return str(current) if current is not None else None


def build_pagination_variables(
    page: int = 1, limit: int = 50, cursor: Optional[str] = None
) -> dict[str, Any]:
    """
    Build common pagination variables for GraphQL queries.

    Args:
        page: Page number (for page-based pagination)
        limit: Number of items per page
        cursor: Cursor value (for cursor-based pagination)

    Returns:
        Variables dictionary for GraphQL query

    Example:
        variables = build_pagination_variables(page=2, limit=25)
        # Returns: {"page": 2, "limit": 25}
    """
    variables = {"page": page, "limit": limit}
    if cursor:
        variables["cursor"] = cursor
    return variables

import json
from typing import Iterator, Dict, List, Optional, Tuple, Union

import requests
from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    StringType,
    BooleanType,
)


class LakeflowConnect:
    """
    Monday.com connector for Lakeflow Connect.

    This connector uses Monday.com's GraphQL API (v2) to fetch data from
    boards, items, and users.
    """

    SUPPORTED_TABLES = ["boards", "items", "users"]
    API_URL = "https://api.monday.com/v2"
    DEFAULT_PAGE_SIZE = 50

    def __init__(self, options: Dict[str, str]) -> None:
        """
        Initialize the Monday.com connector with connection-level options.

        Expected options:
            - api_token: Personal API token for Monday.com authentication.
        """
        api_token = options.get("api_token")
        if not api_token:
            raise ValueError("Monday.com connector requires 'api_token' in options")

        self._session = requests.Session()
        self._session.headers.update({
            "Authorization": api_token,
            "Content-Type": "application/json",
        })

    def list_tables(self) -> List[str]:
        """
        List names of all tables supported by this connector.
        """
        return self.SUPPORTED_TABLES.copy()

    def _validate_table(self, table_name: str) -> None:
        """Validate that the table name is supported."""
        if table_name not in self.SUPPORTED_TABLES:
            raise ValueError(
                f"Table '{table_name}' is not supported. "
                f"Supported tables: {self.SUPPORTED_TABLES}"
            )

    def _execute_query(self, query: str, variables: dict = None) -> dict:
        """
        Execute a GraphQL query against Monday.com API.

        Args:
            query: GraphQL query string
            variables: Optional variables for the query

        Returns:
            The 'data' portion of the response

        Raises:
            RuntimeError: If the API returns errors
        """
        payload = {"query": query}
        if variables:
            payload["variables"] = variables

        response = self._session.post(self.API_URL, json=payload, timeout=60)
        response.raise_for_status()

        result = response.json()

        if "errors" in result:
            error_messages = [e.get("message", str(e)) for e in result["errors"]]
            raise RuntimeError(f"Monday.com API errors: {error_messages}")

        return result.get("data", {})

    def get_table_schema(
        self, table_name: str, table_options: Dict[str, str]
    ) -> StructType:
        """
        Fetch the schema of a table.
        """
        self._validate_table(table_name)

        schema_map = {
            "boards": self._get_boards_schema,
            "items": self._get_items_schema,
            "users": self._get_users_schema,
        }

        return schema_map[table_name]()

    def _get_boards_schema(self) -> StructType:
        """Return the boards table schema."""
        return StructType([
            StructField("id", LongType(), False),
            StructField("name", StringType(), True),
            StructField("description", StringType(), True),
            StructField("state", StringType(), True),
            StructField("board_kind", StringType(), True),
            StructField("workspace_id", LongType(), True),
            StructField("created_at", StringType(), True),
            StructField("updated_at", StringType(), True),
            StructField("url", StringType(), True),
            StructField("items_count", LongType(), True),
            StructField("permissions", StringType(), True),
        ])

    def _get_items_schema(self) -> StructType:
        """Return the items table schema."""
        return StructType([
            StructField("id", LongType(), False),
            StructField("name", StringType(), True),
            StructField("state", StringType(), True),
            StructField("created_at", StringType(), True),
            StructField("updated_at", StringType(), True),
            StructField("creator_id", LongType(), True),
            StructField("board_id", LongType(), True),
            StructField("group_id", StringType(), True),
            StructField("column_values", StringType(), True),
            StructField("url", StringType(), True),
        ])

    def _get_users_schema(self) -> StructType:
        """Return the users table schema."""
        return StructType([
            StructField("id", LongType(), False),
            StructField("name", StringType(), True),
            StructField("email", StringType(), True),
            StructField("enabled", BooleanType(), True),
            StructField("url", StringType(), True),
            StructField("created_at", StringType(), True),
            StructField("is_admin", BooleanType(), True),
            StructField("is_guest", BooleanType(), True),
            StructField("is_view_only", BooleanType(), True),
            StructField("title", StringType(), True),
            StructField("location", StringType(), True),
            StructField("phone", StringType(), True),
            StructField("mobile_phone", StringType(), True),
            StructField("time_zone_identifier", StringType(), True),
        ])

    def read_table_metadata(
        self, table_name: str, table_options: Dict[str, str]
    ) -> dict:
        """
        Fetch the metadata of a table.
        """
        self._validate_table(table_name)

        metadata = {
            "boards": {
                "primary_keys": ["id"],
                "ingestion_type": "snapshot",
            },
            "items": {
                "primary_keys": ["id"],
                "ingestion_type": "snapshot",
            },
            "users": {
                "primary_keys": ["id"],
                "ingestion_type": "snapshot",
            },
        }

        return metadata[table_name]

    def read_table(
        self, table_name: str, start_offset: dict, table_options: Dict[str, str]
    ) -> (Iterator[dict], dict):
        """
        Read the records of a table and return an iterator of records and an offset.
        """
        self._validate_table(table_name)

        read_map = {
            "boards": self._read_boards,
            "items": self._read_items,
            "users": self._read_users,
        }

        return read_map[table_name](start_offset, table_options)

    def _read_boards(
        self, start_offset: dict, table_options: Dict[str, str]
    ) -> (Iterator[dict], dict):
        """
        Read boards using page-based pagination.
        """
        page_size = int(table_options.get("page_size", self.DEFAULT_PAGE_SIZE))
        state_filter = table_options.get("state", "all")

        query = """
        query($limit: Int, $page: Int, $state: State) {
            boards(limit: $limit, page: $page, state: $state) {
                id
                name
                description
                state
                board_kind
                workspace_id
                created_at
                updated_at
                url
                items_count
                permissions
            }
        }
        """

        def generate_records():
            page = 1
            while True:
                variables = {
                    "limit": page_size,
                    "page": page,
                    "state": state_filter,
                }

                data = self._execute_query(query, variables)
                boards = data.get("boards", [])

                if not boards:
                    break

                for board in boards:
                    record = self._transform_board(board)
                    yield record

                if len(boards) < page_size:
                    break

                page += 1

        return generate_records(), {}

    def _transform_board(self, board: dict) -> dict:
        """Transform a board record for output."""
        return {
            "id": self._to_long(board.get("id")),
            "name": board.get("name"),
            "description": board.get("description"),
            "state": board.get("state"),
            "board_kind": board.get("board_kind"),
            "workspace_id": self._to_long(board.get("workspace_id")),
            "created_at": board.get("created_at"),
            "updated_at": board.get("updated_at"),
            "url": board.get("url"),
            "items_count": self._to_long(board.get("items_count")),
            "permissions": board.get("permissions"),
        }

    def _read_items(
        self, start_offset: dict, table_options: Dict[str, str]
    ) -> (Iterator[dict], dict):
        """
        Read items using cursor-based pagination.

        Required table_options:
            - board_ids: Comma-separated list of board IDs to fetch items from.
                         If not provided, will auto-discover all boards.
        """
        page_size = int(table_options.get("page_size", 100))
        board_ids_str = table_options.get("board_ids", "")

        if board_ids_str:
            board_ids = [bid.strip() for bid in board_ids_str.split(",") if bid.strip()]
        else:
            board_ids = self._discover_board_ids()

        if not board_ids:
            return iter([]), None

        def generate_records():
            for board_id in board_ids:
                yield from self._read_items_for_board(board_id, page_size)

        return generate_records(), {}

    def _discover_board_ids(self) -> List[str]:
        """Discover all board IDs in the account."""
        query = """
        query($limit: Int, $page: Int) {
            boards(limit: $limit, page: $page, state: active) {
                id
            }
        }
        """

        board_ids = []
        page = 1
        page_size = 100

        while True:
            variables = {"limit": page_size, "page": page}
            data = self._execute_query(query, variables)
            boards = data.get("boards", [])

            if not boards:
                break

            board_ids.extend(str(b["id"]) for b in boards)

            if len(boards) < page_size:
                break

            page += 1

        return board_ids

    def _read_items_for_board(self, board_id: str, page_size: int) -> Iterator[dict]:
        """Read all items for a specific board using cursor pagination."""
        initial_query = """
        query($boardIds: [ID!], $limit: Int) {
            boards(ids: $boardIds) {
                items_page(limit: $limit) {
                    cursor
                    items {
                        id
                        name
                        state
                        created_at
                        updated_at
                        creator_id
                        board {
                            id
                        }
                        group {
                            id
                        }
                        column_values {
                            id
                            text
                            value
                        }
                        url
                    }
                }
            }
        }
        """

        next_page_query = """
        query($cursor: String!, $limit: Int!) {
            next_items_page(cursor: $cursor, limit: $limit) {
                cursor
                items {
                    id
                    name
                    state
                    created_at
                    updated_at
                    creator_id
                    board {
                        id
                    }
                    group {
                        id
                    }
                    column_values {
                        id
                        text
                        value
                    }
                    url
                }
            }
        }
        """

        variables = {"boardIds": [board_id], "limit": page_size}
        data = self._execute_query(initial_query, variables)

        boards_data = data.get("boards", [])
        if not boards_data:
            return

        items_page = boards_data[0].get("items_page", {})
        items = items_page.get("items", [])
        cursor = items_page.get("cursor")

        for item in items:
            yield self._transform_item(item)

        while cursor:
            variables = {"cursor": cursor, "limit": page_size}
            data = self._execute_query(next_page_query, variables)

            next_items_page = data.get("next_items_page", {})
            items = next_items_page.get("items", [])
            cursor = next_items_page.get("cursor")

            for item in items:
                yield self._transform_item(item)

    def _transform_item(self, item: dict) -> dict:
        """Transform an item record for output."""
        board = item.get("board") or {}
        group = item.get("group") or {}
        column_values = item.get("column_values", [])

        column_values_json = json.dumps(column_values) if column_values else None

        return {
            "id": self._to_long(item.get("id")),
            "name": item.get("name"),
            "state": item.get("state"),
            "created_at": item.get("created_at"),
            "updated_at": item.get("updated_at"),
            "creator_id": self._to_long(item.get("creator_id")),
            "board_id": self._to_long(board.get("id")),
            "group_id": group.get("id"),
            "column_values": column_values_json,
            "url": item.get("url"),
        }

    def _read_users(
        self, start_offset: dict, table_options: Dict[str, str]
    ) -> (Iterator[dict], dict):
        """
        Read users using page-based pagination.
        """
        page_size = int(table_options.get("page_size", self.DEFAULT_PAGE_SIZE))
        kind_filter = table_options.get("kind", "all")

        query = """
        query($limit: Int, $page: Int, $kind: UserKind) {
            users(limit: $limit, page: $page, kind: $kind) {
                id
                name
                email
                enabled
                url
                created_at
                is_admin
                is_guest
                is_view_only
                title
                location
                phone
                mobile_phone
                time_zone_identifier
            }
        }
        """

        def generate_records():
            page = 1
            while True:
                variables = {
                    "limit": page_size,
                    "page": page,
                    "kind": kind_filter,
                }

                data = self._execute_query(query, variables)
                users = data.get("users", [])

                if not users:
                    break

                for user in users:
                    record = self._transform_user(user)
                    yield record

                if len(users) < page_size:
                    break

                page += 1

        return generate_records(), {}

    def _transform_user(self, user: dict) -> dict:
        """Transform a user record for output."""
        return {
            "id": self._to_long(user.get("id")),
            "name": user.get("name"),
            "email": user.get("email"),
            "enabled": user.get("enabled"),
            "url": user.get("url"),
            "created_at": user.get("created_at"),
            "is_admin": user.get("is_admin"),
            "is_guest": user.get("is_guest"),
            "is_view_only": user.get("is_view_only"),
            "title": user.get("title"),
            "location": user.get("location"),
            "phone": user.get("phone"),
            "mobile_phone": user.get("mobile_phone"),
            "time_zone_identifier": user.get("time_zone_identifier"),
        }

    @staticmethod
    def _to_long(value) -> Optional[int]:
        """Convert a value to a long integer, or None if not convertible."""
        if value is None:
            return None
        try:
            return int(value)
        except (ValueError, TypeError):
            return None

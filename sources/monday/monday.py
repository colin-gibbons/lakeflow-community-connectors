import json
from datetime import datetime, timedelta
from typing import Iterator, Dict, List, Optional, Tuple, Union, Set

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

    SUPPORTED_TABLES = ["boards", "items", "users", "workspaces", "teams", "groups", "tags", "updates", "activity_logs"]
    API_URL = "https://api.monday.com/v2"
    DEFAULT_PAGE_SIZE = 50
    DEFAULT_ACTIVITY_LOG_LIMIT = 1000
    LOOKBACK_SECONDS = 60  # Lookback window to avoid missing updates

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
            "workspaces": self._get_workspaces_schema,
            "teams": self._get_teams_schema,
            "groups": self._get_groups_schema,
            "tags": self._get_tags_schema,
            "updates": self._get_updates_schema,
            "activity_logs": self._get_activity_logs_schema,
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

    def _get_workspaces_schema(self) -> StructType:
        """Return the workspaces table schema."""
        return StructType([
            StructField("id", LongType(), False),
            StructField("name", StringType(), True),
            StructField("description", StringType(), True),
            StructField("kind", StringType(), True),
            StructField("state", StringType(), True),
            StructField("created_at", StringType(), True),
            StructField("is_default_workspace", BooleanType(), True),
        ])

    def _get_teams_schema(self) -> StructType:
        """Return the teams table schema."""
        return StructType([
            StructField("id", LongType(), False),
            StructField("name", StringType(), True),
            StructField("picture_url", StringType(), True),
            StructField("owner_ids", StringType(), True),  # JSON array of owner user IDs
            StructField("member_ids", StringType(), True),  # JSON array of member user IDs
        ])

    def _get_groups_schema(self) -> StructType:
        """Return the groups table schema."""
        return StructType([
            StructField("id", StringType(), False),  # Group IDs are strings
            StructField("title", StringType(), True),
            StructField("color", StringType(), True),
            StructField("position", StringType(), True),
            StructField("archived", BooleanType(), True),
            StructField("deleted", BooleanType(), True),
            StructField("board_id", LongType(), True),  # Parent board ID
        ])

    def _get_tags_schema(self) -> StructType:
        """Return the tags table schema."""
        return StructType([
            StructField("id", LongType(), False),
            StructField("name", StringType(), True),
            StructField("color", StringType(), True),
            StructField("board_id", LongType(), True),  # Parent board ID
        ])

    def _get_updates_schema(self) -> StructType:
        """Return the updates table schema."""
        return StructType([
            StructField("id", LongType(), False),
            StructField("body", StringType(), True),  # HTML content
            StructField("text_body", StringType(), True),  # Plain text
            StructField("created_at", StringType(), True),
            StructField("updated_at", StringType(), True),
            StructField("creator_id", LongType(), True),
            StructField("item_id", LongType(), True),
        ])

    def _get_activity_logs_schema(self) -> StructType:
        """Return the activity_logs table schema."""
        return StructType([
            StructField("id", StringType(), False),
            StructField("event", StringType(), True),
            StructField("entity", StringType(), True),
            StructField("data", StringType(), True),  # JSON string
            StructField("user_id", LongType(), True),
            StructField("account_id", StringType(), True),
            StructField("created_at", StringType(), True),  # Converted from 17-digit Unix
            StructField("board_id", LongType(), True),  # Parent board ID
        ])

    def read_table_metadata(
        self, table_name: str, table_options: Dict[str, str]
    ) -> dict:
        """
        Fetch the metadata of a table.

        CDC is supported for boards and items via Activity Logs API.
        Users table only supports snapshot mode.
        """
        self._validate_table(table_name)

        metadata = {
            "boards": {
                "primary_keys": ["id"],
                "cursor_field": "updated_at",
                "ingestion_type": "cdc",
            },
            "items": {
                "primary_keys": ["id"],
                "cursor_field": "updated_at",
                "ingestion_type": "cdc",
            },
            "users": {
                "primary_keys": ["id"],
                "ingestion_type": "snapshot",
            },
            "workspaces": {
                "primary_keys": ["id"],
                "ingestion_type": "snapshot",
            },
            "teams": {
                "primary_keys": ["id"],
                "ingestion_type": "snapshot",
            },
            "groups": {
                "primary_keys": ["id", "board_id"],
                "ingestion_type": "snapshot",
            },
            "tags": {
                "primary_keys": ["id", "board_id"],
                "ingestion_type": "snapshot",
            },
            "updates": {
                "primary_keys": ["id"],
                "ingestion_type": "snapshot",
            },
            "activity_logs": {
                "primary_keys": ["id", "board_id"],
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
            "workspaces": self._read_workspaces,
            "teams": self._read_teams,
            "groups": self._read_groups,
            "tags": self._read_tags,
            "updates": self._read_updates,
            "activity_logs": self._read_activity_logs,
        }

        return read_map[table_name](start_offset, table_options)

    def _read_boards(
        self, start_offset: dict, table_options: Dict[str, str]
    ) -> (Iterator[dict], dict):
        """
        Read boards with CDC support via Activity Logs.

        First sync (no cursor): Full snapshot of all boards.
        Subsequent syncs: Only boards changed since cursor timestamp.
        """
        page_size = int(table_options.get("page_size", self.DEFAULT_PAGE_SIZE))
        state_filter = table_options.get("state", "all")

        # Check for cursor - if none, do full snapshot
        cursor = start_offset.get("cursor") if start_offset else None

        if not cursor:
            # First sync - full snapshot
            return self._read_boards_snapshot(page_size, state_filter)
        else:
            # Incremental sync via activity logs
            return self._read_boards_cdc(cursor, page_size, state_filter)

    def _read_boards_snapshot(
        self, page_size: int, state_filter: str
    ) -> (Iterator[dict], dict):
        """Read all boards for a full snapshot sync."""
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

        max_updated_at = "1970-01-01T00:00:00Z"
        all_records = []

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
                all_records.append(record)

                # Track max updated_at for cursor
                board_updated = record.get("updated_at", "")
                if board_updated and board_updated > max_updated_at:
                    max_updated_at = board_updated

            if len(boards) < page_size:
                break

            page += 1

        # Apply lookback to avoid missing near-simultaneous updates
        if max_updated_at != "1970-01-01T00:00:00Z":
            new_cursor = self._apply_lookback(max_updated_at)
        else:
            new_cursor = None

        return iter(all_records), {"cursor": new_cursor} if new_cursor else {}

    def _read_boards_cdc(
        self, cursor: str, page_size: int, state_filter: str
    ) -> (Iterator[dict], dict):
        """Read only changed boards since cursor using Activity Logs."""
        # First, get all board IDs to query activity logs
        all_board_ids = self._discover_board_ids()

        if not all_board_ids:
            return iter([]), {"cursor": cursor}

        # Query activity logs for board changes since cursor
        logs = self._query_activity_logs(
            all_board_ids, from_timestamp=cursor, entity_filter="board"
        )

        if not logs:
            # No changes - return same cursor
            return iter([]), {"cursor": cursor}

        # Extract changed board IDs from logs
        changed_board_ids = self._extract_board_ids_from_logs(logs)

        # Get max timestamp from logs for new cursor
        max_timestamp = self._get_max_timestamp(logs, cursor)

        if not changed_board_ids:
            # Activity but no board IDs found - advance cursor
            new_cursor = self._apply_lookback(max_timestamp)
            return iter([]), {"cursor": new_cursor}

        # Fetch full board details for changed boards
        boards = self._fetch_boards_by_ids(list(changed_board_ids))

        # Update max timestamp from actual board records
        for board in boards:
            board_updated = board.get("updated_at", "")
            if board_updated and board_updated > max_timestamp:
                max_timestamp = board_updated

        # Apply lookback for next cursor
        new_cursor = self._apply_lookback(max_timestamp)

        return iter(boards), {"cursor": new_cursor}

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
        Read items with CDC support via Activity Logs.

        First sync (no cursor): Full snapshot of all items.
        Subsequent syncs: Only items changed since cursor timestamp.

        Required table_options:
            - board_ids: Comma-separated list of board IDs to fetch items from.
                         If not provided, will auto-discover all boards.
        """
        page_size = int(table_options.get("page_size", 100))
        board_ids_str = table_options.get("board_ids", "")

        # Get board IDs
        if board_ids_str:
            board_ids = [bid.strip() for bid in board_ids_str.split(",") if bid.strip()]
        else:
            board_ids = self._discover_board_ids()

        if not board_ids:
            return iter([]), {}

        # Check for cursor - if none, do full snapshot
        cursor = start_offset.get("cursor") if start_offset else None

        if not cursor:
            # First sync - full snapshot
            return self._read_items_snapshot(board_ids, page_size)
        else:
            # Incremental sync via activity logs
            return self._read_items_cdc(board_ids, cursor, page_size)

    def _read_items_snapshot(
        self, board_ids: List[str], page_size: int
    ) -> (Iterator[dict], dict):
        """Read all items for a full snapshot sync."""
        max_updated_at = "1970-01-01T00:00:00Z"

        def generate_records():
            nonlocal max_updated_at
            for board_id in board_ids:
                for item in self._read_items_for_board(board_id, page_size):
                    # Track max updated_at for cursor
                    item_updated = item.get("updated_at", "")
                    if item_updated and item_updated > max_updated_at:
                        max_updated_at = item_updated
                    yield item

        records = list(generate_records())

        # Apply lookback to avoid missing near-simultaneous updates
        if max_updated_at != "1970-01-01T00:00:00Z":
            cursor = self._apply_lookback(max_updated_at)
        else:
            cursor = None

        return iter(records), {"cursor": cursor} if cursor else {}

    def _read_items_cdc(
        self, board_ids: List[str], cursor: str, page_size: int
    ) -> (Iterator[dict], dict):
        """Read only changed items since cursor using Activity Logs."""
        # Query activity logs for item changes since cursor
        logs = self._query_activity_logs(
            board_ids, from_timestamp=cursor, entity_filter="pulse"
        )

        if not logs:
            # No changes - return same cursor
            return iter([]), {"cursor": cursor}

        # Extract changed item IDs from logs
        changed_item_ids = self._extract_item_ids_from_logs(logs)

        # Get max timestamp from logs for new cursor
        max_timestamp = self._get_max_timestamp(logs, cursor)

        if not changed_item_ids:
            # Activity but no item IDs found - advance cursor
            new_cursor = self._apply_lookback(max_timestamp)
            return iter([]), {"cursor": new_cursor}

        # Fetch full item details for changed items
        items = self._fetch_items_by_ids(list(changed_item_ids), page_size)

        # Update max timestamp from actual item records
        for item in items:
            item_updated = item.get("updated_at", "")
            if item_updated and item_updated > max_timestamp:
                max_timestamp = item_updated

        # Apply lookback for next cursor
        new_cursor = self._apply_lookback(max_timestamp)

        return iter(items), {"cursor": new_cursor}

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

    def _read_workspaces(
        self, start_offset: dict, table_options: Dict[str, str]
    ) -> (Iterator[dict], dict):
        """
        Read workspaces using page-based pagination.
        """
        page_size = int(table_options.get("page_size", self.DEFAULT_PAGE_SIZE))
        state_filter = table_options.get("state", "all")
        kind_filter = table_options.get("kind")

        query = """
        query($limit: Int, $page: Int, $state: State, $kind: WorkspaceKind) {
            workspaces(limit: $limit, page: $page, state: $state, kind: $kind) {
                id
                name
                description
                kind
                state
                created_at
                is_default_workspace
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
                if kind_filter:
                    variables["kind"] = kind_filter

                data = self._execute_query(query, variables)
                workspaces = data.get("workspaces", [])

                if not workspaces:
                    break

                for workspace in workspaces:
                    yield self._transform_workspace(workspace)

                if len(workspaces) < page_size:
                    break

                page += 1

        return generate_records(), {}

    def _transform_workspace(self, workspace: dict) -> dict:
        """Transform a workspace record for output."""
        return {
            "id": self._to_long(workspace.get("id")),
            "name": workspace.get("name"),
            "description": workspace.get("description"),
            "kind": workspace.get("kind"),
            "state": workspace.get("state"),
            "created_at": workspace.get("created_at"),
            "is_default_workspace": workspace.get("is_default_workspace"),
        }

    def _read_teams(
        self, start_offset: dict, table_options: Dict[str, str]
    ) -> (Iterator[dict], dict):
        """
        Read teams. Note: Teams API doesn't have pagination parameters,
        so we fetch all teams in one query.
        """
        query = """
        query {
            teams {
                id
                name
                picture_url
                owners {
                    id
                }
                users {
                    id
                }
            }
        }
        """

        def generate_records():
            data = self._execute_query(query)
            teams = data.get("teams", [])

            for team in teams:
                yield self._transform_team(team)

        return generate_records(), {}

    def _transform_team(self, team: dict) -> dict:
        """Transform a team record for output."""
        owners = team.get("owners") or []
        users = team.get("users") or []

        # Extract owner and member IDs as JSON arrays
        owner_ids = json.dumps([self._to_long(o.get("id")) for o in owners if o.get("id")])
        member_ids = json.dumps([self._to_long(u.get("id")) for u in users if u.get("id")])

        return {
            "id": self._to_long(team.get("id")),
            "name": team.get("name"),
            "picture_url": team.get("picture_url"),
            "owner_ids": owner_ids,
            "member_ids": member_ids,
        }

    def _read_groups(
        self, start_offset: dict, table_options: Dict[str, str]
    ) -> (Iterator[dict], dict):
        """
        Read groups from boards. Groups are nested within boards,
        so we need to query boards and extract their groups.

        Table options:
            - board_ids: Optional comma-separated list of board IDs.
                         If not provided, discovers all boards.
        """
        board_ids_str = table_options.get("board_ids", "")

        # Get board IDs
        if board_ids_str:
            board_ids = [bid.strip() for bid in board_ids_str.split(",") if bid.strip()]
        else:
            board_ids = self._discover_board_ids()

        if not board_ids:
            return iter([]), {}

        query = """
        query($boardIds: [ID!]) {
            boards(ids: $boardIds) {
                id
                groups {
                    id
                    title
                    color
                    position
                    archived
                    deleted
                }
            }
        }
        """

        def generate_records():
            # Process boards in batches to avoid query complexity limits
            batch_size = 25
            for i in range(0, len(board_ids), batch_size):
                batch_ids = board_ids[i:i + batch_size]

                variables = {"boardIds": batch_ids}
                data = self._execute_query(query, variables)

                for board in data.get("boards", []):
                    board_id = board.get("id")
                    groups = board.get("groups", [])

                    for group in groups:
                        yield self._transform_group(group, board_id)

        return generate_records(), {}

    def _transform_group(self, group: dict, board_id: str) -> dict:
        """Transform a group record for output."""
        return {
            "id": group.get("id"),  # Group IDs are strings
            "title": group.get("title"),
            "color": group.get("color"),
            "position": group.get("position"),
            "archived": group.get("archived"),
            "deleted": group.get("deleted"),
            "board_id": self._to_long(board_id),
        }

    def _read_tags(
        self, start_offset: dict, table_options: Dict[str, str]
    ) -> (Iterator[dict], dict):
        """
        Read tags from boards. Tags are nested within boards,
        so we need to query boards and extract their tags.

        Table options:
            - board_ids: Optional comma-separated list of board IDs.
                         If not provided, discovers all boards.
        """
        board_ids_str = table_options.get("board_ids", "")

        # Get board IDs
        if board_ids_str:
            board_ids = [bid.strip() for bid in board_ids_str.split(",") if bid.strip()]
        else:
            board_ids = self._discover_board_ids()

        if not board_ids:
            return iter([]), {}

        query = """
        query($boardIds: [ID!]) {
            boards(ids: $boardIds) {
                id
                tags {
                    id
                    name
                    color
                }
            }
        }
        """

        def generate_records():
            # Process boards in batches to avoid query complexity limits
            batch_size = 25
            for i in range(0, len(board_ids), batch_size):
                batch_ids = board_ids[i:i + batch_size]

                variables = {"boardIds": batch_ids}
                data = self._execute_query(query, variables)

                for board in data.get("boards", []):
                    board_id = board.get("id")
                    tags = board.get("tags", [])

                    for tag in tags:
                        yield self._transform_tag(tag, board_id)

        return generate_records(), {}

    def _transform_tag(self, tag: dict, board_id: str) -> dict:
        """Transform a tag record for output."""
        return {
            "id": self._to_long(tag.get("id")),
            "name": tag.get("name"),
            "color": tag.get("color"),
            "board_id": self._to_long(board_id),
        }

    def _read_updates(
        self, start_offset: dict, table_options: Dict[str, str]
    ) -> (Iterator[dict], dict):
        """
        Read updates (comments) using page-based pagination.
        """
        page_size = int(table_options.get("page_size", self.DEFAULT_PAGE_SIZE))

        query = """
        query($limit: Int, $page: Int) {
            updates(limit: $limit, page: $page) {
                id
                body
                text_body
                created_at
                updated_at
                creator_id
                item_id
            }
        }
        """

        def generate_records():
            page = 1
            while True:
                variables = {
                    "limit": page_size,
                    "page": page,
                }

                data = self._execute_query(query, variables)
                updates = data.get("updates", [])

                if not updates:
                    break

                for update in updates:
                    yield self._transform_update(update)

                if len(updates) < page_size:
                    break

                page += 1

        return generate_records(), {}

    def _transform_update(self, update: dict) -> dict:
        """Transform an update record for output."""
        return {
            "id": self._to_long(update.get("id")),
            "body": update.get("body"),
            "text_body": update.get("text_body"),
            "created_at": update.get("created_at"),
            "updated_at": update.get("updated_at"),
            "creator_id": self._to_long(update.get("creator_id")),
            "item_id": self._to_long(update.get("item_id")),
        }

    def _read_activity_logs(
        self, start_offset: dict, table_options: Dict[str, str]
    ) -> (Iterator[dict], dict):
        """
        Read activity logs from boards. Activity logs are nested within boards.

        Table options:
            - board_ids: Optional comma-separated list of board IDs.
                         If not provided, discovers all boards.
        """
        board_ids_str = table_options.get("board_ids", "")

        # Get board IDs
        if board_ids_str:
            board_ids = [bid.strip() for bid in board_ids_str.split(",") if bid.strip()]
        else:
            board_ids = self._discover_board_ids()

        if not board_ids:
            return iter([]), {}

        query = """
        query($boardIds: [ID!], $limit: Int) {
            boards(ids: $boardIds) {
                id
                activity_logs(limit: $limit) {
                    id
                    event
                    entity
                    data
                    user_id
                    account_id
                    created_at
                }
            }
        }
        """

        def generate_records():
            # Process boards in batches to avoid query complexity limits
            batch_size = 10
            for i in range(0, len(board_ids), batch_size):
                batch_ids = board_ids[i:i + batch_size]

                variables = {
                    "boardIds": batch_ids,
                    "limit": self.DEFAULT_ACTIVITY_LOG_LIMIT,
                }

                try:
                    data = self._execute_query(query, variables)
                except RuntimeError:
                    # Activity logs may not be available for some boards
                    continue

                for board in data.get("boards", []):
                    board_id = board.get("id")
                    logs = board.get("activity_logs", [])

                    for log in logs:
                        yield self._transform_activity_log(log, board_id)

        return generate_records(), {}

    def _transform_activity_log(self, log: dict, board_id: str) -> dict:
        """Transform an activity log record for output."""
        return {
            "id": log.get("id"),
            "event": log.get("event"),
            "entity": log.get("entity"),
            "data": log.get("data"),
            "user_id": self._to_long(log.get("user_id")),
            "account_id": log.get("account_id"),
            "created_at": self._convert_activity_timestamp(log.get("created_at", "0")),
            "board_id": self._to_long(board_id),
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

    # =========================================================================
    # Activity Log Methods for CDC Support
    # =========================================================================

    def _convert_activity_timestamp(self, timestamp: str) -> str:
        """
        Convert Monday.com's 17-digit Unix timestamp to ISO8601 format.

        Monday.com activity logs use a 17-digit timestamp (10^-7 seconds).
        Divide by 10,000,000 to get Unix seconds.
        """
        try:
            seconds = int(timestamp) / 10_000_000
            dt = datetime.utcfromtimestamp(seconds)
            return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        except (ValueError, TypeError, OSError):
            return "1970-01-01T00:00:00Z"

    def _apply_lookback(self, timestamp: str) -> str:
        """Apply lookback window to timestamp to avoid missing updates."""
        try:
            dt = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%SZ")
            dt_with_lookback = dt - timedelta(seconds=self.LOOKBACK_SECONDS)
            return dt_with_lookback.strftime("%Y-%m-%dT%H:%M:%SZ")
        except (ValueError, TypeError):
            return timestamp

    def _query_activity_logs(
        self,
        board_ids: List[str],
        from_timestamp: Optional[str] = None,
        entity_filter: Optional[str] = None,
    ) -> List[dict]:
        """
        Query activity logs for specified boards.

        Args:
            board_ids: List of board IDs to query
            from_timestamp: ISO8601 timestamp to filter from (inclusive)
            entity_filter: Filter by entity type ("board" or "pulse" for items)

        Returns:
            List of activity log entries with converted timestamps
        """
        query = """
        query($boardIds: [ID!], $from: ISO8601DateTime, $limit: Int) {
            boards(ids: $boardIds) {
                id
                activity_logs(from: $from, limit: $limit) {
                    id
                    event
                    entity
                    data
                    created_at
                }
            }
        }
        """

        all_logs = []

        # Process boards in batches to avoid query complexity limits
        batch_size = 10
        for i in range(0, len(board_ids), batch_size):
            batch_ids = board_ids[i:i + batch_size]

            variables = {
                "boardIds": batch_ids,
                "limit": self.DEFAULT_ACTIVITY_LOG_LIMIT,
            }
            if from_timestamp:
                variables["from"] = from_timestamp

            try:
                data = self._execute_query(query, variables)
            except RuntimeError:
                # Activity logs may not be available for some boards
                continue

            for board in data.get("boards", []):
                board_id = board.get("id")
                for log in board.get("activity_logs", []):
                    # Convert timestamp and add board_id for context
                    log_entry = {
                        "id": log.get("id"),
                        "event": log.get("event"),
                        "entity": log.get("entity"),
                        "data": log.get("data"),
                        "created_at": self._convert_activity_timestamp(
                            log.get("created_at", "0")
                        ),
                        "board_id": board_id,
                    }

                    # Filter by entity type if specified
                    if entity_filter and log_entry.get("entity") != entity_filter:
                        continue

                    all_logs.append(log_entry)

        return all_logs

    def _extract_item_ids_from_logs(self, logs: List[dict]) -> Set[str]:
        """
        Extract unique item IDs from activity logs.

        Item (pulse) activity logs contain the item ID in the 'data' field
        as a JSON string with a 'pulse_id' or similar field.
        """
        item_ids = set()

        for log in logs:
            if log.get("entity") != "pulse":
                continue

            data_str = log.get("data", "")
            if not data_str:
                continue

            try:
                data = json.loads(data_str) if isinstance(data_str, str) else data_str

                # Try various possible ID field names
                for key in ["pulse_id", "item_id", "id", "pulseId", "itemId"]:
                    if key in data:
                        item_ids.add(str(data[key]))
                        break

                # Also check for board_id + pulse_id combination
                if "board_id" in data and "pulse_id" in data:
                    item_ids.add(str(data["pulse_id"]))

            except (json.JSONDecodeError, TypeError):
                continue

        return item_ids

    def _extract_board_ids_from_logs(self, logs: List[dict]) -> Set[str]:
        """Extract unique board IDs from activity logs for board-level changes."""
        board_ids = set()

        for log in logs:
            if log.get("entity") == "board":
                # The board_id is added during log processing
                board_id = log.get("board_id")
                if board_id:
                    board_ids.add(str(board_id))

        return board_ids

    def _fetch_items_by_ids(
        self, item_ids: List[str], page_size: int = 100
    ) -> List[dict]:
        """Fetch full item details by item IDs."""
        if not item_ids:
            return []

        query = """
        query($itemIds: [ID!], $limit: Int) {
            items(ids: $itemIds, limit: $limit) {
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
        """

        all_items = []

        # Process items in batches (API limit is 100 items per query)
        for i in range(0, len(item_ids), page_size):
            batch_ids = item_ids[i:i + page_size]

            variables = {
                "itemIds": batch_ids,
                "limit": page_size,
            }

            try:
                data = self._execute_query(query, variables)
                items = data.get("items", [])
                all_items.extend(self._transform_item(item) for item in items)
            except RuntimeError:
                # Some items may have been deleted
                continue

        return all_items

    def _fetch_boards_by_ids(self, board_ids: List[str]) -> List[dict]:
        """Fetch full board details by board IDs."""
        if not board_ids:
            return []

        query = """
        query($boardIds: [ID!]) {
            boards(ids: $boardIds) {
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

        variables = {"boardIds": board_ids}

        try:
            data = self._execute_query(query, variables)
            boards = data.get("boards", [])
            return [self._transform_board(board) for board in boards]
        except RuntimeError:
            return []

    def _get_max_timestamp(self, logs: List[dict], current_max: str) -> str:
        """Get the maximum created_at timestamp from activity logs."""
        max_ts = current_max

        for log in logs:
            log_ts = log.get("created_at", "")
            if log_ts and log_ts > max_ts:
                max_ts = log_ts

        return max_ts

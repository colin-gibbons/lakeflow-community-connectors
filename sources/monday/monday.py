import time
from datetime import datetime, timedelta
from typing import Iterator, Any, Optional

from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    StringType,
    BooleanType,
    ArrayType,
    MapType,
)

# Import GraphQL utilities - will be available after merge
try:
    from libs.graphql_utils import GraphQLClient, GraphQLError
except ImportError:
    # For development/testing before merge
    import sys
    import os

    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))
    from libs.graphql_utils import GraphQLClient, GraphQLError


class LakeflowConnect:
    def __init__(self, options: dict[str, str]) -> None:
        """
        Initialize the Monday.com connector with connection-level options.

        Expected options:
            - api_key: Monday.com API key for authentication.
        """
        api_key = options.get("api_key")
        if not api_key:
            raise ValueError("Monday.com connector requires 'api_key' in options")

        self.api_key = api_key

        # Initialize GraphQL client
        self._graphql_client = GraphQLClient(
            endpoint="https://api.monday.com/v2",
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
        )

    def list_tables(self) -> list[str]:
        """
        List names of all tables supported by this connector.

        Priority 1: Core objects
        Priority 2: Relational data
        Priority 3: Extended features
        """
        return [
            # Priority 1: Core objects
            "boards",
            "items",
            "users",
            "teams",
            "workspaces",
            # Priority 2: Relational data
            "updates",
            "columns",
            "groups",
            # Priority 3: Extended
            "tags",
            "activity_logs",
        ]

    def get_table_schema(
        self, table_name: str, table_options: dict[str, str]
    ) -> StructType:
        """
        Fetch the schema of a table.

        Schemas are static and derived from Monday.com GraphQL API documentation.
        Core fields are typed, variable data (like column_values) uses MapType.
        """
        schemas = {
            "boards": self._get_boards_schema(),
            "items": self._get_items_schema(),
            "users": self._get_users_schema(),
            "teams": self._get_teams_schema(),
            "workspaces": self._get_workspaces_schema(),
            "updates": self._get_updates_schema(),
            "columns": self._get_columns_schema(),
            "groups": self._get_groups_schema(),
            "tags": self._get_tags_schema(),
            "activity_logs": self._get_activity_logs_schema(),
        }

        if table_name not in schemas:
            raise ValueError(
                f"Table '{table_name}' is not supported. "
                f"Supported tables: {', '.join(schemas.keys())}"
            )

        return schemas[table_name]

    def read_table_metadata(
        self, table_name: str, table_options: dict[str, str]
    ) -> dict:
        """
        Fetch the metadata of a table.

        Returns primary keys, cursor field for CDC, and ingestion type.
        """
        metadata_map = {
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
            "teams": {
                "primary_keys": ["id"],
                "ingestion_type": "snapshot",
            },
            "workspaces": {
                "primary_keys": ["id"],
                "ingestion_type": "snapshot",
            },
            "updates": {
                "primary_keys": ["id"],
                "cursor_field": "updated_at",
                "ingestion_type": "cdc",
            },
            "columns": {
                "primary_keys": ["id"],
                "ingestion_type": "snapshot",
            },
            "groups": {
                "primary_keys": ["id"],
                "ingestion_type": "snapshot",
            },
            "tags": {
                "primary_keys": ["id"],
                "ingestion_type": "snapshot",
            },
            "activity_logs": {
                "primary_keys": ["id"],
                "cursor_field": "created_at",
                "ingestion_type": "append",
            },
        }

        if table_name not in metadata_map:
            raise ValueError(
                f"Table '{table_name}' metadata not found. "
                f"Supported tables: {', '.join(metadata_map.keys())}"
            )

        return metadata_map[table_name]

    def read_table(
        self, table_name: str, start_offset: dict, table_options: dict[str, str]
    ) -> (Iterator[dict], dict):
        """
        Read the records of a table and return an iterator of records and an offset.

        Supports incremental loading for CDC tables using updated_at cursor.
        Supports pagination for large datasets.
        """
        read_methods = {
            "boards": self._read_boards,
            "items": self._read_items,
            "users": self._read_users,
            "teams": self._read_teams,
            "workspaces": self._read_workspaces,
            "updates": self._read_updates,
            "columns": self._read_columns,
            "groups": self._read_groups,
            "tags": self._read_tags,
            "activity_logs": self._read_activity_logs,
        }

        if table_name not in read_methods:
            raise ValueError(
                f"Table '{table_name}' read method not implemented. "
                f"Supported tables: {', '.join(read_methods.keys())}"
            )

        return read_methods[table_name](start_offset, table_options)

    # Schema Definitions

    def _get_boards_schema(self) -> StructType:
        """Schema for boards table."""
        return StructType(
            [
                StructField("id", StringType(), False),
                StructField("name", StringType(), True),
                StructField("description", StringType(), True),
                StructField("state", StringType(), True),
                StructField("board_kind", StringType(), True),
                StructField("created_at", StringType(), True),
                StructField("updated_at", StringType(), True),
                StructField("workspace_id", StringType(), True),
                StructField("owner_id", StringType(), True),
                StructField(
                    "groups",
                    ArrayType(MapType(StringType(), StringType())),
                    True,
                ),
                StructField(
                    "columns",
                    ArrayType(MapType(StringType(), StringType())),
                    True,
                ),
            ]
        )

    def _get_items_schema(self) -> StructType:
        """Schema for items table."""
        return StructType(
            [
                StructField("id", StringType(), False),
                StructField("name", StringType(), True),
                StructField("board_id", StringType(), True),
                StructField("group_id", StringType(), True),
                StructField("state", StringType(), True),
                StructField("created_at", StringType(), True),
                StructField("updated_at", StringType(), True),
                StructField("creator_id", StringType(), True),
                StructField(
                    "column_values",
                    ArrayType(
                        StructType(
                            [
                                StructField("id", StringType(), True),
                                StructField("column_id", StringType(), True),
                                StructField("type", StringType(), True),
                                StructField("text", StringType(), True),
                                StructField("value", StringType(), True),
                            ]
                        )
                    ),
                    True,
                ),
            ]
        )

    def _get_users_schema(self) -> StructType:
        """Schema for users table."""
        return StructType(
            [
                StructField("id", StringType(), False),
                StructField("name", StringType(), True),
                StructField("email", StringType(), True),
                StructField("url", StringType(), True),
                StructField("title", StringType(), True),
                StructField("created_at", StringType(), True),
                StructField("birthday", StringType(), True),
                StructField("country_code", StringType(), True),
                StructField("location", StringType(), True),
                StructField("time_zone_identifier", StringType(), True),
                StructField("is_guest", BooleanType(), True),
                StructField("is_pending", BooleanType(), True),
                StructField("enabled", BooleanType(), True),
                StructField("photo_thumb", StringType(), True),
            ]
        )

    def _get_teams_schema(self) -> StructType:
        """Schema for teams table."""
        return StructType(
            [
                StructField("id", StringType(), False),
                StructField("name", StringType(), True),
                StructField("picture_url", StringType(), True),
                StructField(
                    "users",
                    ArrayType(MapType(StringType(), StringType())),
                    True,
                ),
            ]
        )

    def _get_workspaces_schema(self) -> StructType:
        """Schema for workspaces table."""
        return StructType(
            [
                StructField("id", StringType(), False),
                StructField("name", StringType(), True),
                StructField("kind", StringType(), True),
                StructField("description", StringType(), True),
                StructField("created_at", StringType(), True),
            ]
        )

    def _get_updates_schema(self) -> StructType:
        """Schema for updates table."""
        return StructType(
            [
                StructField("id", StringType(), False),
                StructField("item_id", StringType(), True),
                StructField("creator_id", StringType(), True),
                StructField("body", StringType(), True),
                StructField("text_body", StringType(), True),
                StructField("created_at", StringType(), True),
                StructField("updated_at", StringType(), True),
            ]
        )

    def _get_columns_schema(self) -> StructType:
        """Schema for columns table."""
        return StructType(
            [
                StructField("id", StringType(), False),
                StructField("title", StringType(), True),
                StructField("type", StringType(), True),
                StructField("description", StringType(), True),
                StructField("board_id", StringType(), True),
                StructField("width", LongType(), True),
                StructField("settings_str", StringType(), True),
            ]
        )

    def _get_groups_schema(self) -> StructType:
        """Schema for groups table."""
        return StructType(
            [
                StructField("id", StringType(), False),
                StructField("title", StringType(), True),
                StructField("color", StringType(), True),
                StructField("position", StringType(), True),
                StructField("board_id", StringType(), True),
            ]
        )

    def _get_tags_schema(self) -> StructType:
        """Schema for tags table."""
        return StructType(
            [
                StructField("id", StringType(), False),
                StructField("name", StringType(), True),
                StructField("color", StringType(), True),
            ]
        )

    def _get_activity_logs_schema(self) -> StructType:
        """Schema for activity_logs table."""
        return StructType(
            [
                StructField("id", StringType(), False),
                StructField("event", StringType(), True),
                StructField("user_id", StringType(), True),
                StructField("account_id", StringType(), True),
                StructField("created_at", StringType(), True),
                StructField("data", StringType(), True),
                StructField("entity", StringType(), True),
            ]
        )

    # Read Methods

    def _read_boards(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> (Iterator[dict], dict):
        """Read boards table with pagination and CDC support."""
        page = 1
        if start_offset and isinstance(start_offset, dict):
            page = start_offset.get("page", 1)

        limit = int(table_options.get("limit", 50))
        max_pages = int(table_options.get("max_pages_per_batch", 50))

        query = """
        query GetBoards($page: Int!, $limit: Int!) {
          boards(page: $page, limit: $limit, order_by: updated_at) {
            id
            name
            description
            state
            board_kind
            created_at
            updated_at
            workspace {
              id
            }
            owner {
              id
            }
            groups {
              id
              title
              color
              position
            }
            columns {
              id
              title
              type
              description
              width
              settings_str
            }
          }
        }
        """

        all_records = []
        pages_fetched = 0

        while pages_fetched < max_pages:
            variables = {"page": page, "limit": limit}

            try:
                response = self._graphql_client.execute(query, variables)
            except GraphQLError as e:
                raise RuntimeError(f"Monday.com API error for boards: {e}")

            boards = response.get("data", {}).get("boards", [])

            if not boards:
                break

            # Transform boards to match schema
            for board in boards:
                record = {
                    "id": str(board.get("id", "")),
                    "name": board.get("name"),
                    "description": board.get("description"),
                    "state": board.get("state"),
                    "board_kind": board.get("board_kind"),
                    "created_at": board.get("created_at"),
                    "updated_at": board.get("updated_at"),
                    "workspace_id": self._extract_id(board.get("workspace")),
                    "owner_id": self._extract_id(board.get("owner")),
                    "groups": self._transform_nested_list(board.get("groups", [])),
                    "columns": self._transform_nested_list(board.get("columns", [])),
                }
                all_records.append(record)

            page += 1
            pages_fetched += 1

            # Rate limiting
            time.sleep(0.2)

        next_offset = {"page": page}
        return iter(all_records), next_offset

    def _read_items(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> (Iterator[dict], dict):
        """Read items table with pagination and CDC support."""
        # Get board IDs from table options
        board_ids_str = table_options.get("board_ids")
        if not board_ids_str:
            raise ValueError("items table requires 'board_ids' in table_options")

        board_ids = [bid.strip() for bid in board_ids_str.split(",")]

        limit = int(table_options.get("limit", 50))
        max_pages = int(table_options.get("max_pages_per_batch", 50))

        # Track offset per board
        if start_offset and isinstance(start_offset, dict):
            current_board_idx = start_offset.get("board_idx", 0)
            page = start_offset.get("page", 1)
        else:
            current_board_idx = 0
            page = 1

        all_records = []
        pages_fetched = 0

        # Iterate through boards
        while current_board_idx < len(board_ids) and pages_fetched < max_pages:
            board_id = board_ids[current_board_idx]

            query = """
            query GetItems($boardId: [Int!]!, $page: Int!, $limit: Int!) {
              boards(ids: $boardId) {
                id
                items_page(limit: $limit, query_params: {page: $page}) {
                  items {
                    id
                    name
                    state
                    created_at
                    updated_at
                    creator {
                      id
                    }
                    group {
                      id
                    }
                    column_values {
                      id
                      column {
                        id
                      }
                      type
                      text
                      value
                    }
                  }
                }
              }
            }
            """

            variables = {"boardId": [int(board_id)], "page": page, "limit": limit}

            try:
                response = self._graphql_client.execute(query, variables)
            except GraphQLError as e:
                raise RuntimeError(f"Monday.com API error for items: {e}")

            boards_data = response.get("data", {}).get("boards", [])
            if not boards_data:
                # Move to next board
                current_board_idx += 1
                page = 1
                continue

            items = boards_data[0].get("items_page", {}).get("items", [])

            if not items:
                # Move to next board
                current_board_idx += 1
                page = 1
                continue

            # Transform items to match schema
            for item in items:
                column_values = []
                for cv in item.get("column_values", []):
                    column_values.append(
                        {
                            "id": str(cv.get("id", "")),
                            "column_id": self._extract_id(cv.get("column")),
                            "type": cv.get("type"),
                            "text": cv.get("text"),
                            "value": cv.get("value"),
                        }
                    )

                record = {
                    "id": str(item.get("id", "")),
                    "name": item.get("name"),
                    "board_id": board_id,
                    "group_id": self._extract_id(item.get("group")),
                    "state": item.get("state"),
                    "created_at": item.get("created_at"),
                    "updated_at": item.get("updated_at"),
                    "creator_id": self._extract_id(item.get("creator")),
                    "column_values": column_values,
                }
                all_records.append(record)

            page += 1
            pages_fetched += 1

            # Rate limiting
            time.sleep(0.2)

        next_offset = {"board_idx": current_board_idx, "page": page}
        return iter(all_records), next_offset

    def _read_users(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> (Iterator[dict], dict):
        """Read users table (snapshot mode)."""
        query = """
        query GetUsers {
          users {
            id
            name
            email
            url
            title
            created_at
            birthday
            country_code
            location
            time_zone_identifier
            is_guest
            is_pending
            enabled
            photo_thumb
          }
        }
        """

        try:
            response = self._graphql_client.execute(query)
        except GraphQLError as e:
            raise RuntimeError(f"Monday.com API error for users: {e}")

        users = response.get("data", {}).get("users", [])

        records = []
        for user in users:
            record = {
                "id": str(user.get("id", "")),
                "name": user.get("name"),
                "email": user.get("email"),
                "url": user.get("url"),
                "title": user.get("title"),
                "created_at": user.get("created_at"),
                "birthday": user.get("birthday"),
                "country_code": user.get("country_code"),
                "location": user.get("location"),
                "time_zone_identifier": user.get("time_zone_identifier"),
                "is_guest": user.get("is_guest"),
                "is_pending": user.get("is_pending"),
                "enabled": user.get("enabled"),
                "photo_thumb": user.get("photo_thumb"),
            }
            records.append(record)

        # Snapshot mode - no meaningful offset
        return iter(records), {}

    def _read_teams(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> (Iterator[dict], dict):
        """Read teams table (snapshot mode)."""
        query = """
        query GetTeams {
          teams {
            id
            name
            picture_url
            users {
              id
              name
              email
            }
          }
        }
        """

        try:
            response = self._graphql_client.execute(query)
        except GraphQLError as e:
            raise RuntimeError(f"Monday.com API error for teams: {e}")

        teams = response.get("data", {}).get("teams", [])

        records = []
        for team in teams:
            record = {
                "id": str(team.get("id", "")),
                "name": team.get("name"),
                "picture_url": team.get("picture_url"),
                "users": self._transform_nested_list(team.get("users", [])),
            }
            records.append(record)

        return iter(records), {}

    def _read_workspaces(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> (Iterator[dict], dict):
        """Read workspaces table (snapshot mode)."""
        query = """
        query GetWorkspaces {
          workspaces {
            id
            name
            kind
            description
            created_at
          }
        }
        """

        try:
            response = self._graphql_client.execute(query)
        except GraphQLError as e:
            raise RuntimeError(f"Monday.com API error for workspaces: {e}")

        workspaces = response.get("data", {}).get("workspaces", [])

        records = []
        for workspace in workspaces:
            record = {
                "id": str(workspace.get("id", "")),
                "name": workspace.get("name"),
                "kind": workspace.get("kind"),
                "description": workspace.get("description"),
                "created_at": workspace.get("created_at"),
            }
            records.append(record)

        return iter(records), {}

    def _read_updates(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> (Iterator[dict], dict):
        """Read updates table with CDC support."""
        # Updates require board IDs or can fetch from items
        board_ids_str = table_options.get("board_ids")
        if not board_ids_str:
            raise ValueError("updates table requires 'board_ids' in table_options")

        board_ids = [bid.strip() for bid in board_ids_str.split(",")]

        # Track offset per board
        if start_offset and isinstance(start_offset, dict):
            current_board_idx = start_offset.get("board_idx", 0)
            page = start_offset.get("page", 1)
        else:
            current_board_idx = 0
            page = 1

        limit = int(table_options.get("limit", 50))
        max_pages = int(table_options.get("max_pages_per_batch", 50))

        all_records = []
        pages_fetched = 0

        # Iterate through boards to get updates
        while current_board_idx < len(board_ids) and pages_fetched < max_pages:
            board_id = board_ids[current_board_idx]

            query = """
            query GetUpdates($boardId: [Int!]!, $page: Int!, $limit: Int!) {
              boards(ids: $boardId) {
                updates(page: $page, limit: $limit) {
                  id
                  item_id
                  creator {
                    id
                  }
                  body
                  text_body
                  created_at
                  updated_at
                }
              }
            }
            """

            variables = {"boardId": [int(board_id)], "page": page, "limit": limit}

            try:
                response = self._graphql_client.execute(query, variables)
            except GraphQLError as e:
                raise RuntimeError(f"Monday.com API error for updates: {e}")

            boards_data = response.get("data", {}).get("boards", [])
            if not boards_data:
                current_board_idx += 1
                page = 1
                continue

            updates = boards_data[0].get("updates", [])

            if not updates:
                current_board_idx += 1
                page = 1
                continue

            for update in updates:
                record = {
                    "id": str(update.get("id", "")),
                    "item_id": update.get("item_id"),
                    "creator_id": self._extract_id(update.get("creator")),
                    "body": update.get("body"),
                    "text_body": update.get("text_body"),
                    "created_at": update.get("created_at"),
                    "updated_at": update.get("updated_at"),
                }
                all_records.append(record)

            page += 1
            pages_fetched += 1
            time.sleep(0.2)

        next_offset = {"board_idx": current_board_idx, "page": page}
        return iter(all_records), next_offset

    def _read_columns(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> (Iterator[dict], dict):
        """Read columns table (extracted from boards)."""
        # Columns are board-specific, extract from boards query
        board_ids_str = table_options.get("board_ids")
        if not board_ids_str:
            raise ValueError("columns table requires 'board_ids' in table_options")

        board_ids = [int(bid.strip()) for bid in board_ids_str.split(",")]

        query = """
        query GetBoardColumns($boardIds: [Int!]!) {
          boards(ids: $boardIds) {
            id
            columns {
              id
              title
              type
              description
              width
              settings_str
            }
          }
        }
        """

        variables = {"boardIds": board_ids}

        try:
            response = self._graphql_client.execute(query, variables)
        except GraphQLError as e:
            raise RuntimeError(f"Monday.com API error for columns: {e}")

        boards = response.get("data", {}).get("boards", [])

        records = []
        for board in boards:
            board_id = str(board.get("id", ""))
            for column in board.get("columns", []):
                record = {
                    "id": str(column.get("id", "")),
                    "title": column.get("title"),
                    "type": column.get("type"),
                    "description": column.get("description"),
                    "board_id": board_id,
                    "width": column.get("width"),
                    "settings_str": column.get("settings_str"),
                }
                records.append(record)

        return iter(records), {}

    def _read_groups(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> (Iterator[dict], dict):
        """Read groups table (extracted from boards)."""
        board_ids_str = table_options.get("board_ids")
        if not board_ids_str:
            raise ValueError("groups table requires 'board_ids' in table_options")

        board_ids = [int(bid.strip()) for bid in board_ids_str.split(",")]

        query = """
        query GetBoardGroups($boardIds: [Int!]!) {
          boards(ids: $boardIds) {
            id
            groups {
              id
              title
              color
              position
            }
          }
        }
        """

        variables = {"boardIds": board_ids}

        try:
            response = self._graphql_client.execute(query, variables)
        except GraphQLError as e:
            raise RuntimeError(f"Monday.com API error for groups: {e}")

        boards = response.get("data", {}).get("boards", [])

        records = []
        for board in boards:
            board_id = str(board.get("id", ""))
            for group in board.get("groups", []):
                record = {
                    "id": str(group.get("id", "")),
                    "title": group.get("title"),
                    "color": group.get("color"),
                    "position": group.get("position"),
                    "board_id": board_id,
                }
                records.append(record)

        return iter(records), {}

    def _read_tags(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> (Iterator[dict], dict):
        """Read tags table (snapshot mode)."""
        query = """
        query GetTags {
          tags {
            id
            name
            color
          }
        }
        """

        try:
            response = self._graphql_client.execute(query)
        except GraphQLError as e:
            raise RuntimeError(f"Monday.com API error for tags: {e}")

        tags = response.get("data", {}).get("tags", [])

        records = []
        for tag in tags:
            record = {
                "id": str(tag.get("id", "")),
                "name": tag.get("name"),
                "color": tag.get("color"),
            }
            records.append(record)

        return iter(records), {}

    def _read_activity_logs(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> (Iterator[dict], dict):
        """Read activity_logs table (append mode)."""
        # Activity logs have limited support in GraphQL API
        # This is a placeholder - actual implementation depends on API availability

        # For now, return empty
        return iter([]), {}

    # Helper Methods

    def _extract_id(self, obj: Optional[dict]) -> Optional[str]:
        """Extract ID from nested object."""
        if obj and isinstance(obj, dict):
            id_val = obj.get("id")
            return str(id_val) if id_val is not None else None
        return None

    def _transform_nested_list(self, items: list[dict]) -> list[dict[str, str]]:
        """Transform list of nested objects to list of string maps."""
        result = []
        for item in items:
            if isinstance(item, dict):
                # Convert all values to strings
                transformed = {k: str(v) if v is not None else None for k, v in item.items()}
                result.append(transformed)
        return result

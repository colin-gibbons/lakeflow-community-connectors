# Lakeflow Monday.com Community Connector

This documentation provides setup instructions and reference information for the Monday.com source connector.

## Prerequisites

- A Monday.com account with access to the boards and data you want to sync
- A personal API token from Monday.com (requires Developer access)
- Read access to the boards, items, and users you want to ingest

## Setup

### Required Connection Parameters

To configure the connector, provide the following parameters in your connector options:

| Parameter | Type | Required | Description | Example |
|-----------|------|----------|-------------|---------|
| `api_token` | string | Yes | Your Monday.com personal API token | `cgJhbGciOiJIUzI1...` |

### Table-Specific Options

The following table-specific options are supported and should be included in the `externalOptionsAllowList` connection option:

| Option | Applicable Tables | Required | Description |
|--------|------------------|----------|-------------|
| `board_ids` | items | No | Comma-separated list of board IDs to fetch items from. If not specified, items from all accessible boards will be fetched. |
| `page_size` | boards, items, users | No | Number of records to fetch per page (default: 50 for boards/users, 100 for items) |
| `state` | boards | No | Filter boards by state: `active`, `all`, `archived`, `deleted` (default: `all`) |
| `kind` | users | No | Filter users by kind: `all`, `guests`, `non_guests`, `non_pending` (default: `all`) |

**externalOptionsAllowList**: `board_ids,page_size,state,kind`

### Obtaining Your API Token

1. Log in to your Monday.com account
2. Click your profile picture (bottom left corner)
3. Select **Developers**
4. Click **My Access Tokens** tab
5. Click **Show** to reveal your personal API token
6. Copy the token and store it securely

**Note**: Your API token inherits your account permissions. You can only access boards and data that you have permission to view in the Monday.com UI.

### Create a Unity Catalog Connection

A Unity Catalog connection for this connector can be created in two ways via the UI:

1. Follow the Lakeflow Community Connector UI flow from the "Add Data" page
2. Select any existing Lakeflow Community Connector connection for Monday.com or create a new one
3. Include `board_ids,page_size,state,kind` in the `externalOptionsAllowList` to enable table-specific configuration

The connection can also be created using the standard Unity Catalog API.

## Supported Objects

The Monday.com connector supports the following objects:

| Object | Primary Key | Sync Mode | Description |
|--------|-------------|-----------|-------------|
| `boards` | id | CDC (Incremental) | Board metadata including name, description, state, and settings |
| `items` | id | CDC (Incremental) | Items (rows) from boards with all column values |
| `users` | id | Full Refresh | Account users with profile information |
| `workspaces` | id | Full Refresh | Workspaces that organize boards |
| `teams` | id | Full Refresh | Teams and their members |
| `groups` | id, board_id | Full Refresh | Groups (sections) within boards |

### Change Data Capture (CDC)

The `boards` and `items` tables support CDC (Change Data Capture) for incremental syncs. Since Monday.com's GraphQL API does not support filtering by `updated_at`, the connector uses the **Activity Logs API** to identify changes.

**How CDC Works:**
1. **First sync**: Performs a full snapshot of all data
2. **Subsequent syncs**: Queries activity logs since the last sync timestamp to identify changed entities
3. **Fetches only changed records**: Only boards/items that have been modified are retrieved

**Activity Log Details:**
- Activity logs track all changes to boards and items (creates, updates, deletes)
- The connector applies a 60-second lookback window to avoid missing near-simultaneous updates
- `users` table remains snapshot-only as there is no activity log tracking for user changes

**Note**: Activity logs have a retention period. For very old cursors, the connector will fall back to a full snapshot.

### boards

Retrieves all boards accessible to the authenticated user.

**Key Fields:**
- `id` - Unique board identifier
- `name` - Board name
- `state` - Board state (active, archived, deleted)
- `board_kind` - Board type (public, private, share)
- `workspace_id` - Parent workspace ID
- `items_count` - Number of items on the board
- `created_at`, `updated_at` - Timestamps

**Options:**
- `state` - Filter by board state (default: `all`)

### items

Retrieves items from specified boards. Items represent rows in Monday.com boards.

**Key Fields:**
- `id` - Unique item identifier
- `name` - Item name
- `board_id` - Parent board ID
- `group_id` - Group the item belongs to
- `state` - Item state (active, archived, deleted)
- `column_values` - JSON string containing all column values for the item
- `created_at`, `updated_at` - Timestamps
- `creator_id` - ID of the user who created the item

**Options:**
- `board_ids` - Comma-separated list of board IDs to fetch items from. If not specified, discovers and fetches items from all accessible boards.

**Note on column_values:** Column values are stored as a JSON string to accommodate the dynamic nature of Monday.com boards. Each board can have different columns, and this approach ensures all column data is captured without requiring schema changes.

### users

Retrieves all users in the Monday.com account.

**Key Fields:**
- `id` - Unique user identifier
- `name` - User's display name
- `email` - User's email address
- `enabled` - Whether the user account is active
- `is_admin`, `is_guest`, `is_view_only` - User role flags
- `title`, `location` - Profile information
- `time_zone_identifier` - User's timezone

**Options:**
- `kind` - Filter users by type (default: `all`)

### workspaces

Retrieves all workspaces accessible to the authenticated user.

**Key Fields:**
- `id` - Unique workspace identifier
- `name` - Workspace name
- `description` - Workspace description
- `kind` - Workspace type (open, closed, template)
- `state` - Workspace state (active, archived, deleted)
- `created_at` - Creation timestamp
- `is_default_workspace` - Whether this is the default workspace

**Options:**
- `state` - Filter by workspace state (default: `all`)
- `kind` - Filter by workspace type (open, closed, template)

### teams

Retrieves all teams in the Monday.com account.

**Key Fields:**
- `id` - Unique team identifier
- `name` - Team name
- `picture_url` - Team picture URL
- `owner_ids` - JSON array of owner user IDs
- `member_ids` - JSON array of member user IDs

### groups

Retrieves groups (sections) from boards. Groups organize items within boards.

**Key Fields:**
- `id` - Unique group identifier (string)
- `title` - Group display name
- `color` - Group color (hex code)
- `position` - Group position on board
- `archived` - Whether the group is archived
- `deleted` - Whether the group is deleted
- `board_id` - Parent board ID

**Options:**
- `board_ids` - Comma-separated list of board IDs to fetch groups from. If not specified, discovers all accessible boards.

## Data Type Mapping

| Monday.com Type | Spark/Databricks Type | Notes |
|-----------------|----------------------|-------|
| ID | BIGINT | Unique identifiers |
| String | STRING | Text values |
| Boolean | BOOLEAN | True/false values |
| Date/DateTime | STRING | ISO 8601 format timestamps |
| ColumnValue array | STRING | JSON-serialized array of column values |
| Integer | BIGINT | Numeric values (using BIGINT to avoid overflow) |

## How to Run

### Step 1: Clone/Copy the Source Connector Code

Follow the Lakeflow Community Connector UI, which will guide you through setting up a pipeline using the selected source connector code.

### Step 2: Configure Your Pipeline

1. Update the `pipeline_spec` in the main pipeline file (e.g., `ingest.py`).
2. Configure table-specific options as needed:

```json
{
  "pipeline_spec": {
      "connection_name": "my_monday_connection",
      "object": [
        {
            "table": {
                "source_table": "boards",
                "state": "active"
            }
        },
        {
            "table": {
                "source_table": "items",
                "board_ids": "18394670765,18394670764"
            }
        },
        {
            "table": {
                "source_table": "users"
            }
        }
      ]
  }
}
```

3. (Optional) Customize the source connector code if needed for special use cases.

### Step 3: Run and Schedule the Pipeline

#### Best Practices

- **Start Small**: Begin by syncing a specific board using `board_ids` to test your pipeline before expanding to all boards
- **Use Board Filtering**: For large Monday.com accounts, specify `board_ids` to limit API calls and improve performance
- **Mind Rate Limits**: Monday.com has complexity-based rate limits. The connector uses conservative page sizes by default.
- **Column Values Processing**: Parse the `column_values` JSON field downstream to extract specific column data

#### Troubleshooting

**Common Issues:**

1. **Authentication Error (401)**
   - Verify your API token is correct and hasn't been regenerated
   - Check that you're using the token directly (no "Bearer" prefix needed)

2. **No Data Returned for Items**
   - Ensure you have access to the boards in the Monday.com UI
   - Try specifying `board_ids` explicitly with boards you can access

3. **Rate Limiting Errors**
   - Reduce `page_size` in table options
   - Add delays between pipeline runs
   - Check your Monday.com plan's API limits

4. **Missing Column Values**
   - Column values are captured as JSON; ensure downstream processing handles the JSON structure
   - Some columns may have null values if not populated in Monday.com

## References

- [Monday.com API Documentation](https://developer.monday.com/api-reference/docs)
- [Monday.com Authentication Guide](https://developer.monday.com/api-reference/docs/authentication)
- [Monday.com Rate Limits](https://developer.monday.com/api-reference/reference/rate-limits)
- [GraphQL API Reference](https://developer.monday.com/api-reference/reference/about-the-api-reference)

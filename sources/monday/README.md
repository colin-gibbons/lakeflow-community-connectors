# Monday.com Connector

This connector allows you to extract data from Monday.com into Databricks Lakeflow using the Monday.com GraphQL API.

## Overview

The Monday.com connector provides comprehensive data extraction capabilities for Monday.com workspaces, including boards, items, users, teams, updates, and more. It supports both incremental (CDC) and snapshot loading modes.

## Supported Tables

### Priority 1: Core Objects
- **boards** - Board structures and metadata (CDC)
- **items** - Board items with column values (CDC)
- **users** - User accounts (Snapshot)
- **teams** - Team organization (Snapshot)
- **workspaces** - Workspace containers (Snapshot)

### Priority 2: Relational Data
- **updates** - Item comments and activity (CDC)
- **columns** - Column definitions per board (Snapshot)
- **groups** - Section groupings within boards (Snapshot)

### Priority 3: Extended
- **tags** - Tags applied to items (Snapshot)
- **activity_logs** - System activity logs (Append)

## Authentication

### Getting Your API Key

1. Log in to your Monday.com account
2. Click your avatar in the bottom left corner
3. Select "Admin" > "API"
4. Copy your API key

**Important:** Keep your API key secure and never commit it to version control.

## Connection Configuration

### Required Parameters

```json
{
  "api_key": "your_monday_api_key_here"
}
```

## Table Configuration

### Boards Table

No additional parameters required. Fetches all boards accessible by the API key.

**Optional parameters:**
- `limit` - Items per page (default: 50, max: 50)
- `max_pages_per_batch` - Maximum pages to fetch per batch (default: 50)

**Example:**
```json
{
  "boards": {
    "limit": 50,
    "max_pages_per_batch": 10
  }
}
```

### Items Table

**Required parameters:**
- `board_ids` - Comma-separated list of board IDs to fetch items from

**Optional parameters:**
- `limit` - Items per page (default: 50)
- `max_pages_per_batch` - Maximum pages to fetch per batch (default: 50)

**Example:**
```json
{
  "items": {
    "board_ids": "1234567890,9876543210",
    "limit": 50,
    "max_pages_per_batch": 10
  }
}
```

**How to find Board IDs:**
1. Open a board in Monday.com
2. Look at the URL: `https://yourcompany.monday.com/boards/1234567890`
3. The number `1234567890` is the board ID

### Users, Teams, Workspaces Tables

No additional parameters required. Fetches all accessible entities.

```json
{
  "users": {},
  "teams": {},
  "workspaces": {}
}
```

### Updates Table

**Required parameters:**
- `board_ids` - Comma-separated list of board IDs to fetch updates from

**Optional parameters:**
- `limit` - Items per page (default: 50)
- `max_pages_per_batch` - Maximum pages to fetch per batch (default: 50)

**Example:**
```json
{
  "updates": {
    "board_ids": "1234567890,9876543210",
    "limit": 50
  }
}
```

### Columns and Groups Tables

**Required parameters:**
- `board_ids` - Comma-separated list of board IDs

**Example:**
```json
{
  "columns": {
    "board_ids": "1234567890,9876543210"
  },
  "groups": {
    "board_ids": "1234567890,9876543210"
  }
}
```

## Ingestion Modes

### CDC (Change Data Capture)
Incremental loading that tracks changes using `updated_at` timestamps.

**Tables:** boards, items, updates

**How it works:**
- First sync: Fetches all records
- Subsequent syncs: Fetches only new/updated records since last sync
- Uses lookback window (default 5 minutes) to prevent missing concurrent updates

**Benefits:**
- Efficient for large datasets
- Reduces API calls and data transfer
- Faster sync times

### Snapshot
Full refresh of all data each sync.

**Tables:** users, teams, workspaces, columns, groups, tags

**How it works:**
- Each sync fetches complete dataset
- Previous data is replaced

**Use when:**
- Tables are small
- Data changes infrequently
- Complete refresh is preferred

### Append
Append-only mode for immutable event logs.

**Tables:** activity_logs

## Rate Limits

Monday.com uses **complexity-based rate limiting**:
- **10,000,000 complexity points per minute per account**
- Each field in a GraphQL query has a complexity cost
- Rate limit is shared across all API requests for your account

**Best practices:**
- The connector includes automatic rate limiting (0.2s delay between requests)
- Limit `max_pages_per_batch` to avoid hitting rate limits
- Monitor your API usage in Monday.com admin panel

**If you hit rate limits:**
- Reduce `max_pages_per_batch`
- Increase delay between syncs
- Fetch fewer boards/items per sync

## Column Values

Monday.com has 30+ column types (status, date, person, text, numbers, etc.). The connector stores column values in a flexible format:

```json
{
  "column_values": [
    {
      "id": "status_1",
      "column_id": "status",
      "type": "color",
      "text": "Done",
      "value": "{\"index\":2,\"label\":\"Done\"}"
    }
  ]
}
```

**Fields:**
- `id` - Unique value ID
- `column_id` - Column identifier
- `type` - Column type (color, date, text, etc.)
- `text` - Human-readable text representation
- `value` - JSON string with full column value data

**Parsing column values:**
Use SQL or Python to parse the `value` field based on the `type`:

```sql
-- Extract status label
SELECT
  id,
  name,
  get_json_object(cv.value, '$.label') as status
FROM items
LATERAL VIEW explode(column_values) t AS cv
WHERE cv.type = 'color'
```

## Example Pipeline Configuration

```json
{
  "connection_name": "monday_production",
  "objects": [
    {
      "table": {
        "source_table": "boards",
        "destination_catalog": "main",
        "destination_schema": "monday",
        "destination_table": "boards",
        "table_configuration": {
          "scd_type": "SCD_TYPE_1",
          "primary_keys": ["id"],
          "sequence_by": "updated_at",
          "limit": 50,
          "max_pages_per_batch": 20
        }
      }
    },
    {
      "table": {
        "source_table": "items",
        "destination_catalog": "main",
        "destination_schema": "monday",
        "destination_table": "items",
        "table_configuration": {
          "scd_type": "SCD_TYPE_1",
          "primary_keys": ["id"],
          "sequence_by": "updated_at",
          "board_ids": "1234567890,9876543210",
          "limit": 50,
          "max_pages_per_batch": 20
        }
      }
    },
    {
      "table": {
        "source_table": "users",
        "destination_catalog": "main",
        "destination_schema": "monday",
        "destination_table": "users",
        "table_configuration": {
          "scd_type": "SCD_TYPE_1",
          "primary_keys": ["id"]
        }
      }
    }
  ]
}
```

## Troubleshooting

### "GraphQL errors: Not authorized"
- Verify your API key is correct
- Check that the API key has access to the requested boards/workspaces

### "items table requires 'board_ids' in table_options"
- Add `board_ids` parameter to your table configuration
- Example: `"board_ids": "1234567890,9876543210"`

### Rate limit errors
- Reduce `max_pages_per_batch` in table configuration
- Limit number of boards synced per job
- Space out sync schedules

### Empty results
- Verify board IDs are correct (check Monday.com URL)
- Ensure API key has access to the boards
- Check that boards contain data

### Slow performance
- Monday.com API can be slow for large boards with many items
- Use CDC mode for incremental syncs
- Reduce `max_pages_per_batch` to process in smaller batches
- Consider syncing fewer boards per pipeline

## Schema Information

### Boards Schema
- `id` (string, PK)
- `name` (string)
- `description` (string)
- `state` (string) - active, archived, deleted
- `board_kind` (string) - public, private, share
- `created_at` (string, ISO 8601)
- `updated_at` (string, ISO 8601)
- `workspace_id` (string)
- `owner_id` (string)
- `groups` (array<map<string,string>>) - Board sections
- `columns` (array<map<string,string>>) - Column definitions

### Items Schema
- `id` (string, PK)
- `name` (string)
- `board_id` (string)
- `group_id` (string)
- `state` (string)
- `created_at` (string, ISO 8601)
- `updated_at` (string, ISO 8601)
- `creator_id` (string)
- `column_values` (array<struct>) - Array of column value objects

### Users Schema
- `id` (string, PK)
- `name` (string)
- `email` (string)
- `url` (string)
- `title` (string)
- `created_at` (string, ISO 8601)
- `birthday` (string)
- `country_code` (string)
- `location` (string)
- `time_zone_identifier` (string)
- `is_guest` (boolean)
- `is_pending` (boolean)
- `enabled` (boolean)
- `photo_thumb` (string)

## Limitations

1. **Board-specific tables require board IDs** - Items, updates, columns, and groups require explicit board IDs
2. **Activity logs not fully supported** - Limited GraphQL API support for activity logs
3. **Complexity-based rate limiting** - Shared across your account, can affect other integrations
4. **No server-side filtering** - Monday.com GraphQL has limited filter support; some filtering done client-side
5. **Page-based pagination** - Most queries use page-based pagination (may miss concurrent updates; mitigated by lookback window)

## Related Documentation

- [Monday.com GraphQL API Documentation](https://developer.monday.com/api-reference/docs/basics)
- [Monday.com API Reference](https://developer.monday.com/api-reference/)
- [Rate Limits](https://developer.monday.com/api-reference/docs/rate-limits)
- [Column Types](https://developer.monday.com/api-reference/docs/column-types)

## Support

For issues or questions:
1. Check Monday.com API documentation
2. Verify your API key and permissions
3. Check rate limit usage in Monday.com admin panel
4. Review connector logs for error messages

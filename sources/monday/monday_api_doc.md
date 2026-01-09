# Monday.com API Documentation

## Overview

Monday.com uses a **GraphQL API (v2)** for all data access. Unlike REST APIs, all requests go to a single endpoint using POST method with the query in the request body.

- **API Endpoint**: `https://api.monday.com/v2`
- **Method**: POST
- **Content-Type**: `application/json`

## Authorization

### Preferred Method: Personal API Token

Monday.com supports personal API tokens for authentication. The token is passed in the `Authorization` header.

**How to obtain a token:**
1. Click your profile picture (bottom left) → Select "Developers"
2. Click "My Access Tokens" → "Show"
3. Copy your personal API token

**Header Format:**
```
Authorization: <your_api_token>
Content-Type: application/json
```

Note: The token is passed directly without a "Bearer" prefix.

**Example authenticated request:**
```bash
curl -X POST https://api.monday.com/v2 \
  -H "Authorization: xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx" \
  -H "Content-Type: application/json" \
  -d '{"query": "query { me { id name } }"}'
```

**Scopes:** Personal tokens inherit all permissions from your monday.com account. If you can access a board in the UI, you can access it via API.

**Alternative:** OAuth 2.0 is also supported for app integrations but requires client_id/client_secret flow.

## Object List

The connector supports the following objects (static list):

| Object | Parent | Description |
|--------|--------|-------------|
| `boards` | None | Boards contain items, columns, groups |
| `items` | boards | Items are rows within boards |
| `users` | None | Account users |

**Object Hierarchy:**
- `boards` are top-level containers
- `items` belong to `boards` (require board_id to query efficiently)
- `users` are independent account-level objects

## Object Schema

### boards

Queried via GraphQL `boards` query.

| Field | Type | Nullable | Description |
|-------|------|----------|-------------|
| id | ID! | No | Unique board identifier |
| name | String! | No | Board name |
| description | String | Yes | Board description |
| state | State! | No | Board state: `active`, `archived`, `deleted` |
| board_kind | BoardKind! | No | Board type: `private`, `public`, `share` |
| workspace_id | ID | Yes | Parent workspace ID |
| created_at | ISO8601DateTime | Yes | Creation timestamp |
| updated_at | ISO8601DateTime | Yes | Last update timestamp |
| url | String! | No | Board URL |
| items_count | Int | Yes | Number of items on board |
| permissions | String! | No | Default role for users |

**Example Query:**
```graphql
query {
  boards(limit: 25, page: 1, state: all) {
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
```

### items

Queried via `items_page` on boards or `next_items_page` for pagination.

| Field | Type | Nullable | Description |
|-------|------|----------|-------------|
| id | ID! | No | Unique item identifier |
| name | String! | No | Item name |
| state | State | Yes | Item state: `active`, `archived`, `deleted` |
| created_at | Date | Yes | Creation timestamp |
| updated_at | Date | Yes | Last update timestamp |
| creator_id | String! | Yes | Creator's user ID (null if system-created) |
| board.id | ID! | No | Parent board ID |
| group.id | ID | Yes | Parent group ID |
| column_values | [ColumnValue] | Yes | Array of column values |
| url | String! | No | Item URL |

**Column Values Structure:**
Each column_value contains:
- `id`: Column ID
- `text`: Human-readable text value
- `value`: JSON string of the raw value

**Example Query (initial page):**
```graphql
query($boardIds: [ID!]) {
  boards(ids: $boardIds) {
    items_page(limit: 100) {
      cursor
      items {
        id
        name
        state
        created_at
        updated_at
        creator_id
        board { id }
        group { id }
        column_values { id text value }
        url
      }
    }
  }
}
```

**Example Query (next page):**
```graphql
query($cursor: String!) {
  next_items_page(limit: 100, cursor: $cursor) {
    cursor
    items {
      id
      name
      state
      created_at
      updated_at
      creator_id
      board { id }
      group { id }
      column_values { id text value }
      url
    }
  }
}
```

### users

Queried via GraphQL `users` query.

| Field | Type | Nullable | Description |
|-------|------|----------|-------------|
| id | ID! | No | Unique user identifier |
| name | String! | No | User's display name |
| email | String! | No | User's email address |
| enabled | Boolean! | No | Whether user is active |
| url | String! | No | User's profile URL |
| created_at | Date | Yes | Account creation date |
| is_admin | Boolean | Yes | Admin status |
| is_guest | Boolean | Yes | Guest user status |
| is_view_only | Boolean | Yes | View-only access |
| title | String | Yes | Job title |
| location | String | Yes | User's location |
| phone | String | Yes | Phone number |
| mobile_phone | String | Yes | Mobile phone number |
| time_zone_identifier | String | Yes | Timezone identifier |

**Example Query:**
```graphql
query {
  users(limit: 50, page: 1) {
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
```

## Get Object Primary Keys

Primary keys are static for all objects:

| Object | Primary Key |
|--------|-------------|
| boards | `id` |
| items | `id` |
| users | `id` |

All IDs are unique within the Monday.com account.

## Object's Ingestion Type

| Object | Ingestion Type | Reason |
|--------|---------------|--------|
| boards | `snapshot` | No reliable incremental cursor; updated_at available but filtering not supported |
| items | `snapshot` | Cursor-based pagination but no incremental filter on API |
| users | `snapshot` | No incremental API endpoint |

**Note:** While boards and items have `updated_at` fields, the GraphQL API does not support filtering by these fields efficiently. Airbyte's approach uses Activity Logs for incremental syncs, but this adds complexity. For hackathon scope, snapshot mode is recommended.

## Read API for Data Retrieval

### GraphQL Request Format

All requests are HTTP POST to `https://api.monday.com/v2`:

```python
import requests

response = requests.post(
    "https://api.monday.com/v2",
    headers={
        "Authorization": api_token,
        "Content-Type": "application/json"
    },
    json={
        "query": graphql_query,
        "variables": variables_dict  # optional
    }
)
```

### Pagination

**boards and users: Page-based pagination**
- Use `limit` (default: 25) and `page` (starts at 1)
- Continue until response returns fewer items than limit

```graphql
query($limit: Int, $page: Int) {
  boards(limit: $limit, page: $page) {
    id name
  }
}
```

**items: Cursor-based pagination**
- Initial request uses `items_page` on boards query
- Subsequent requests use `next_items_page` with cursor
- Continue until cursor is `null`

```graphql
# Initial request
query($boardIds: [ID!]) {
  boards(ids: $boardIds) {
    items_page(limit: 100) {
      cursor
      items { id name }
    }
  }
}

# Subsequent requests
query($cursor: String!) {
  next_items_page(limit: 100, cursor: $cursor) {
    cursor
    items { id name }
  }
}
```

**Important:** Cursors expire after 60 minutes.

### Items Query Strategy

Items require a board context. Two approaches:

1. **Require board_ids table option**: User provides comma-separated board IDs
2. **Auto-discover boards**: Query all boards first, then iterate

Recommended: Require `board_ids` for explicit control and efficiency.

### Rate Limits

Monday.com enforces multiple rate limits:

| Limit Type | Value | Notes |
|------------|-------|-------|
| Complexity | 5M points per minute | Each query has a complexity cost |
| Daily calls | 200-25,000 | Varies by plan tier |
| Minute limit | 1,000-5,000 queries | Varies by plan tier |
| Concurrency | 40-250 requests | Simultaneous request limit |

**Rate Limit Response Headers:**
- `retry_in_seconds`: Wait time before retrying
- `Retry-After`: For minute limit errors

**Error Types:**
- `ComplexityException`: Query too complex
- `DAILY_LIMIT_EXCEEDED`: Daily quota reached
- `CONCURRENCY_LIMIT_EXCEEDED`: Too many parallel requests

**Best Practices:**
- Use conservative page sizes (25-50)
- Check for errors in response even on HTTP 200
- Implement exponential backoff on rate limit errors

### Error Handling

GraphQL returns errors in the response body, even with HTTP 200:

```json
{
  "errors": [
    {
      "message": "Some error occurred",
      "extensions": {
        "code": "SomeErrorCode"
      }
    }
  ],
  "data": null
}
```

Always check `response.json().get("errors")` before processing data.

### Table Options

| Table | Option | Required | Description |
|-------|--------|----------|-------------|
| items | board_ids | Yes | Comma-separated list of board IDs to fetch items from |
| boards | state | No | Filter by state: `active`, `all`, `archived`, `deleted` (default: `all`) |
| users | kind | No | Filter by kind: `all`, `guests`, `non_guests`, `non_pending` |

## Field Type Mapping

| Monday.com Type | Spark Type | Notes |
|-----------------|------------|-------|
| ID! | LongType | Unique identifiers (numeric strings) |
| String | StringType | Text values |
| String! | StringType | Required text values |
| Boolean | BooleanType | True/false values |
| Int | LongType | Use LongType to avoid overflow |
| Date | StringType | ISO 8601 format (YYYY-MM-DD) |
| ISO8601DateTime | StringType | Full timestamp format |
| [ColumnValue] | StringType | JSON-serialized array |
| State | StringType | Enum: active, archived, deleted |
| BoardKind | StringType | Enum: private, public, share |

**Special Handling:**
- `column_values` is serialized to JSON string to avoid dynamic schema complexity
- IDs are returned as strings by GraphQL but should be stored as LongType
- Nested objects (board, group) are flattened to extract only IDs

## Sources and References

### Research Log

| Source Type | URL | Accessed (UTC) | Confidence | What it confirmed |
|-------------|-----|----------------|------------|-------------------|
| Official Docs | https://developer.monday.com/api-reference/reference/about-the-api-reference | 2026-01-09 | Highest | API structure, GraphQL nature |
| Official Docs | https://developer.monday.com/api-reference/docs/authentication | 2026-01-09 | Highest | Auth header format, token types |
| Official Docs | https://developer.monday.com/api-reference/reference/boards | 2026-01-09 | Highest | Board fields, query params |
| Official Docs | https://developer.monday.com/api-reference/reference/items | 2026-01-09 | Highest | Item fields, pagination limits |
| Official Docs | https://developer.monday.com/api-reference/reference/items-page | 2026-01-09 | Highest | Cursor pagination, 60-min expiry |
| Official Docs | https://developer.monday.com/api-reference/reference/next-items-page | 2026-01-09 | Highest | Cursor continuation queries |
| Official Docs | https://developer.monday.com/api-reference/reference/users | 2026-01-09 | Highest | User fields, query params |
| Official Docs | https://developer.monday.com/api-reference/reference/rate-limits | 2026-01-09 | Highest | Rate limit values, best practices |
| Airbyte Docs | https://docs.airbyte.com/integrations/sources/monday | 2026-01-09 | High | Supported streams, incremental approach |

### Known Quirks

1. **No Bearer prefix**: Authorization header uses token directly, not "Bearer <token>"
2. **Cursor expiration**: Items pagination cursors expire after 60 minutes
3. **Items require board context**: Cannot query items globally; must specify board IDs
4. **GraphQL errors on 200**: Always check response body for errors even on successful HTTP status
5. **ID types**: IDs are returned as strings in GraphQL but represent numeric values

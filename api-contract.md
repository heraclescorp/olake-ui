# Olake Server API Contract

### For now use olake as project id, later on it can be used to make multitenant system

## Base URL
```
http://localhost:8080
```

## Async operations

Spec (`sources/spec`, `destinations/spec`), test connection (`sources/test`, `destinations/test`),
discover (`sources/streams`) and stream difference (`jobs/:id/stream-difference`) run a connector
container. Their POST starts the work and returns `202`:

```json
{
  "success": "boolean",
  "message": "string",
  "data": { "operation_id": "string" }
}
```

The payload documented for those endpoints below is the operation result.

- `GET /api/v1/project/:projectid/operations/:operationid?wait=N` — status. `wait` long-polls up to
  N seconds (capped at 10). `data`: `{ "status": "running" | "succeeded" | "failed", "error": "string" }`.
- `GET /api/v1/project/:projectid/operations/:operationid/result` — the result `data`. `409` while
  running, `404` for an unknown operation or one of another project, `500` when the operation failed.

Request validation (for example the catalog format of a stream difference) still fails on the
POST, before any operation starts.

## Uninterrupted table resync

`POST /api/v1/project/:projectid/jobs/:id/resync-table` accepts only
`{"namespace":"public","stream_name":"orders"}` from the saved selected catalog.
The saved source must be PostgreSQL and the saved destination configuration must
select `type: "ICEBERG"` with `writer.catalog_type` in the canonical REST family:
`rest`, `lakekeeper`, `nessie`, `s3tables`, `unity`, `polaris`, `biglake`, or `horizon`.
Catalog comparison trims whitespace and ignores case, matching the connector.
Native S3 Tables is supported. Missing/empty `writer.catalog_type` defaults to Glue
in the connector and is excluded, as are explicit Glue and other catalog families.
Unsupported saved configuration is rejected before schedule changes or workflow
start. Ordinary Glue, Parquet and non-PostgreSQL jobs retain their existing
lifecycle; Clear Destination remains a separate explicitly destructive operation.
The job must be unpaused, idle, and use a backfill-capable mode. Successful acceptance
returns the usual `200` response with null data; it does not mean publication has finished.
History identifies the operation as `resync`, separate from destructive Clear Destination.

The worker reloads the full saved source, destination, catalog, and state, then runs
`resync` with `--resync-namespace`, `--resync-table`, and a stable workflow ID as
`--resync-id`. It never routes through `clear-destination` or resets saved state first.
Unsupported connector/runtime, destination, or schema transitions fail without a
destructive fallback. Deploy the matching worker and connector before using this endpoint.
Configured PostgreSQL `backfill_host` readers remain supported. Only the resync snapshot
uses the writer connection and its exported snapshot; saved source configuration is not changed.

The previous committed snapshot remains readable during rebuild and CDC catch-up.
Successful publication replaces the snapshot atomically on the same table UUID/name/location;
it is not a rename swap. Readers such as Snowflake observe it after their normal refresh.
Existing timestamp-default schema compatibility limits still apply. Each staging attempt
has a 12-hour safety bound; longer attempts fail rather than risk staging-file expiration.

Pre-publication failure retains the old committed data. Once published, checkpoint
persistence and an ambiguous acknowledgement require reconciliation, not rollback.
The schedule remains paused on failure or an uncertain start/publication/cleanup outcome.
Confirmed publication and successful state persistence resume through the bound handoff;
positively unpublished/settled retirement requires a separate explicit schedule resume.
The internal nullable `execution_workflow_id` and `execution_run_id` job columns
identify the stable operation and its controlling Temporal run. They are read-only in
recovery responses; clients cannot supply an execution epoch or handoff token.
The internal `<job-table>_execution_records` table records launch admission,
runtime/resource identity, outcome, and checkpoint/catalog-bound schedule handoff.
Its primary key is `(job_id, workflow_id)` and `(job_id, phase)` is indexed.
The JSON-hidden `controlled BOOLEAN NOT NULL DEFAULT false` field records the
worker's canonical saved-job domain decision under the shared database fence;
caller flags cannot select it. It is immutable per operation: domain mismatch
requires reconciliation, not a guessed upgrade or downgrade. Explicit legacy
records have `controlled=false` and empty runtime/launch IDs; controlled executions
use the runtime-fence and retirement-proof protocol. No automatic backfill guesses
the domain of an existing record.
Closed records remain permanent tombstones after ownership release and job deletion
so a delayed activity cannot reopen an already retired operation. They have no
cascading job foreign key. Storage retention/deletion must preserve CLOSED runtime
fences and immutable runtime pointers; only log data may be pruned.

Job mutations share the catalog-edit advisory fence with worker ownership claims.
Nested same-job edits reuse the outer fence, without unlocking it on inner return.
A cancelled/timed-out request can lose its PostgreSQL advisory transaction before
its handler returns: final state, config, activation and deletion writes therefore
also use the request context and atomic predicates requiring both owner columns
to be NULL and no execution record in `handoff`. Catalog-revision checks and writes
are in the same row-locking transaction. Linked-job catalog migrations have the
same idle-owner/handoff predicate. Worker cancellation state persistence and release
do not wait for the API advisory fence; they must compare both execution IDs.

Publication does not make a job editable before normal-sync handoff finishes.
Pending handoff remains durable authority and rejects state restoration, pause,
configuration, deletion, manual sync, clear and resync requests with `409`, even
when Temporal's eventually consistent workflow listing reports no active resync.
The worker retains ownership and validates the published checkpoint, catalog and
current active intent before schedule transfer, using a persistent idempotency
request ID. An ambiguous handoff acknowledgement does not permit a blind later
unpause. Do not manually clear an owner or edit execution records to bypass busy
responses; reconcile the admitted runtime and durable checkpoint first.

### Canceled execution recovery

Authenticated `GET /api/v1/project/:projectid/jobs/:id/reconcile` reads the SQL
owner and handoff in one read-only snapshot without taking the worker's catalog
fence, then inspects the controlling Temporal run. The result is observational:
either authority can change afterward, and mutations recheck under the fence. The server verifies that the job
belongs to the route's project. Data includes `status` (`idle`, `active`,
`awaiting_reconciliation`, `reconciling`, `blocked`), `workflow_id`, `run_id`,
`phase`, `outcome`, `temporal_status`, `can_reconcile`, `schedule_paused`, `message`
and an available `last_error`. `idle` means no owner **and no pending handoff**;
it does not mean the ordinary schedule is unpaused. Missing authority, unsupported
legacy records, unavailable history and unknown remote outcomes are not success.

`POST` to the same endpoint takes no owner/run/launch input (the UI sends `{}`).
It reads the retained owner and controlled record under the shared SQL exclusion
fence, confirms the exact old run is terminal, verifies matching original workflow
inputs and resets the **same WorkflowID** before its first workflow-task completion.
It never resets a running operation or starts a new data operation under the same
ID. A deterministic request ID deduplicates a lost reset response; another running
recovery returns `409`. An accepted reset returns `202` with `status: reconciling`
and `recovery_run_id`, not a claim of retirement. Poll GET until the real SQL
ownership/phase settles. A completed failed recovery remains actionable `blocked`;
after restoring the missing evidence/access, POST can recheck the same operation.

The worker first consumes an exact original-runtime `not_published` receipt only
after SQL admission closure, acknowledged CLOSED fence and confirmed immutable
resource retirement. A proven canceled resync is adopted CLOSED/retired and released
without a connector relaunch. Unknown resync publication may use the existing
recovery-only publication-marker path, never a fresh rebuild. Pending published
handoffs retain their original checkpoint/catalog/active-intent/token binding.
Old ordinary `execution-result.json` with `outcome: unknown` cannot be reclassified
from cancellation, exit status, process absence or logs: restore access to an
existing valid immutable receipt, or independently establish remote disposition
with the connector/catalog operator and a supported evidence-producing reconciliation
mechanism. This API has no force-unlock, fabricated receipt or unsafe override.

After proof-backed retirement, the settings UI offers **Resume schedule** only
when GET reports idle and paused. It uses the existing activation endpoint with
`{ "activate": true }`, including when the database already says active. Activation
rechecks no owner/no pending handoff under the fence and positively verifies that
the current schedule action is a normal sync for this project/job before unpausing.
It does not explicitly trigger a sync; the user can request a fresh sync or table
resync afterward. A stale idle UI cannot resume a newly owned job.


Deploy the database migration before the new worker, then the matching worker and
connector images before admitting new work; see [deployment guidance](#atomic-table-resync-deployment).
Drain all old workers and active/queued old resync, clear/handoff and sync executions
before cutover: an old writer does not honor the new epoch or runtime fence. Mixed
old/new execution is unsupported. The connector must support `--execution-id`,
`--execution-fence` and recovery-only `--resync-recover-only`; an older image must
fail on these flags rather than use a destructive fallback.

Do not repoint or edit a shared source/destination during an active operation or
pending handoff. Standalone shared-connector edits are an existing unsupported
cross-entity configuration path; this change only guards their linked-job catalog
writes, not the source/destination configuration itself. Different jobs must not
share a destination table or PostgreSQL replication slot: ownership is per job,
not a global destination/slot lock. Independent reserved Iceberg resync-property
edits are unsupported. Clear Destination remains explicitly destructive.

Task history keeps the stable workflow ID. Before log/archive storage lookup, the
API validates the requested task against the saved project's job namespace and
reads execution authority scoped to `(job_id, workflow_id)`. Current connector
and original-launch directories come directly from that SQL record's runtime and
launch IDs. Mutable `execution-runtime.json` objects never select a generation:
a delayed old storage Put cannot hide recovery logs or redirect an archive.
Stable layout applies only when no execution record exists or when the record
explicitly has `controlled=false` with both runtime and launch IDs empty. A legacy
record containing runtime identity is rejected rather than downgraded. Database
errors and controlled prelaunch records never enable connector fallback; the
latter expose stable worker logs only. Runtime roots are sibling
`sha256(RuntimeID)` directories/prefixes, not children of `sha256(WorkflowID)`.
Log cursors bind the current runtime; reload logs after a recovery runtime changes.
Archive preparation pins one SQL identity snapshot for both filename and streaming.
Downloads contain current connector artifacts at the archive root, stable worker
artifacts under `workflow/`, and original admitted-launch artifacts under `launch/`
when recovery is separate. Intermediate artifacts and immutable pointers remain
retained in storage; the API does not need pointer objects to read recorded runs.

### Atomic table resync deployment

Treat the API database migration, worker and connector as a coordinated upgrade.

1. Stop admitting job mutations and new schedule actions. Drain old workers and
   their active/queued sync, clear, resync and legacy clear-to-sync handoff
   executions, including outstanding activity attempts. Resolve any uncertain
   publication or cleanup outcome before replacing the old runtime.
2. Start the updated API with database DDL privileges while workers remain stopped.
   Startup `AutoMigrate` adds nullable `execution_run_id` alongside
   `execution_workflow_id` and creates `<job-table>_execution_records` (for example
   `olake-prod-job_execution_records`). Besides its composite primary key and
   pending-phase index, the internal record contains `owner_run_id`, `controlled`, `launch_id`,
   `launcher_attempt_id`, `phase`, `runtime_id`, `resource_id`, `resource_uid`,
   `outcome`, `state_sha256`, `catalog_sha256`, `handoff_token`, stable transfer
   epoch `handoff_run_id`, `handoff_active`, `handoff_request_id`, and timestamps.
   Keep these permanent admission tombstones across releases and job deletion.
3. Deploy the matching worker and connector. Runtime-specific config/artifact
   storage must be shared with the worker and support complete atomic fence
   publication plus create-if-absent OPEN initialization. A CLOSED identity must
   never be reopened; missing, unreadable or mismatched fences fail closed.
   Keep storage permissions and control objects throughout cleanup and
   reconciliation. Kubernetes cleanup uses immutable UID preconditions; Docker
   cleanup addresses the immutable container ID, not a reusable name.
4. Resume admission only after all executing images honor the new contract. Do
   not roll back to an old writer while owners, admitted runtimes or pending
   handoffs remain. Configure object-store lifecycle and log retention to prune
   log data only, never CLOSED fences, immutable runtime pointers or entire
   workflow/runtime prefixes: delayed OPEN/create requests can outlive logs.

An unknown publication stays paused until the admitted runtime is retired and
publication/checkpoint disposition is proven. Recovery-only execution cannot
rebuild data or infer abort from a missing publication marker. Never bypass a
busy job by clearing ownership, deleting execution records or blindly unpausing.

For isolated PostgreSQL regressions, set `OLAKE_TEST_POSTGRES_DSN`; service tests
also require `POSTGRES_DB` to that same DSN and `SESSION_ON=false` before starting
the test process. Without the DSN, PostgreSQL tests skip. The cancellation cases
use PostgreSQL advisory-wait barriers, cancel the request, allow a replacement
worker epoch to claim, and attempt late state/config/activation/deletion writes.
Handoff cases exercise exclusion without Temporal visibility and with both job
owners NULL. Run from `server/` against an isolated real PostgreSQL database:

```bash
POSTGRES_DB="$OLAKE_TEST_POSTGRES_DSN" SESSION_ON=false OLAKE_STORAGE_MODE=nfs \
  go test -count=1 -race ./internal/database ./internal/services/etl \
  ./internal/models ./internal/utils ./internal/services/temporal
```


## Authentication

### Login

- **Endpoint**: `/login`
- **Method**: POST
- **Description**: Authenticate user and get access token
- **Request Body**:
  ```json
  {
    "username": "string",
    "password": "string"
  }
  ```
- **Response**:
  ```json
  {
    "success": "boolean",
    "message": "string",
    "data": {
      "username": "string"
    }
  }
  ```

### Signup

- **Endpoint**: `/signup`
- **Method**: POST
- **Description**: Register a new user
- **Request Body**:
  ```json
  {
    "email":"string",
    "username": "string",
    "password": "string"
  }
  ```
- **Response**:

  ```json
  {
    "success": "boolean",
    "message": "string",
    "data": {
      "email":"string",
      "username": "string"
    }
  }
  ```

### Check Authentication
- **Endpoint**: `/auth`
- **Method**: GET
- **Description**: Verify if user is authenticated
- **Headers**: `Authorization: Bearer <token>` // we are using cookie currently so frontend take care accordingly
- **Response**:
  ```json
  {
    "success": "boolean",
    "message": "string",
    "data": {
      "username": "string" // can be user id let us discuss, else let us store username in session
    }
  }
  ```

## Sources
### Get All Version Of Source 
- **Endpoint**: `/api/v1/project/:projectid/sources/versions`
- **Method**: GET
- **Description**: Give spec based on source type
- **Headers**: `Authorization: Bearer <token>`
- **Request Body**:
  ```json
  {
    "type":"string",
  }
  ```
- **Response**:

  ```json
   {
    "success": "boolean",
    "message": "string",
    "data": {
     "version":["string","string"]
    }
  }
  ```
### Get Spec Of Source

- **Endpoint**: `/api/v1/project/:projectid/sources/spec`
- **Method**: GET
- **Description**: Give spec based on source type
- **Headers**: `Authorization: Bearer <token>`
- **Request Body**:
  ```json
  {
    "type":"string",
    "version": "string",
    "type":"string",
    "version": "string",
  }
  ```
- **Response**:

  ```json
  {
    "success": "boolean",
    "message": "string",
    "data": {
      "type": "string",
      "version": "string",
      "spec": {
        "jsonschema": "object",
        "uischema": "string"
      }
    }
  }
  ```

### Source Test Connection

- **Endpoint**: `/api/v1/project/:projectid/sources/test`
- **Method**: POST
- **Description**: Test configured source configuration
- **Headers**: `Authorization: Bearer <token>`
- **Request Body**:

  ```json
  {
    "type":"string",
    "version":"string",
    "config": "json"
  }
  ```

- **Response**:

  ```json
{
   "success": boolean,
  "message": "string",
  "data": {
    "connection_result": {
      "message": "string",
      "status": "string",
      "prerequisites": [ // optional, sources with CDC checks only
        {"name": "string", "required": boolean, "passed": boolean, "current_value": "string", "recommended_value": "string", "description": "string", "error": "string (optional: why the check could not be evaluated)"}
      ]
    },
    "logs": "json"
  }
}
  ```

### Create Source

- **Endpoint**: `/api/v1/project/:projectid/sources`
- **Method**: POST
- **Description**: Create a new source
- **Headers**: `Authorization: Bearer <token>`
- **Request Body**:
  ```json
  {
    "name": "string",  // we have to make sure in database that it must also unique according to project id (for doubt let us discuss)
    "type": "string", 
    "version":"string", // this field need to be shown on frontend as well, we discussed at time of design as well
    "config": "json"
  }
  ```
- **Response**:
  ```json
  {
    "success": "boolean",
    "message": "string",
    "data": { // whatever received send back
        "name": "string",  
        "type": "string", 
        "version":"string", 
        "config": "json"
      }
  }
  ```

### Get All Sources

- **Endpoint**: `/api/v1/project/:projectid/sources`
- **Method**: GET
- **Description**: Retrieve all sources
- **Headers**: `Authorization: Bearer <token>`
- **Response**:
  ```json
  {
    "success": "boolean",
    "message": "string",
    "data": [
      {
        "id": "integer",
        "name": "string",
        "type": "string",
        "version": "string",
        "config": "json",
        "created_at": "timestamp",
        "updated_at": "timestamp",
        "created_by": "string", // only username of user
        "updated_by": "string", // only username of user,
        "jobs": [
          {
            "name": "string",
            "id": "integer",
            "activate": "boolean",
            "last_run_time": "timestamp",
            "last_run_state": "string",
            "dest_name": "string"
          }
        ]
      }
    ]
  }


### Get Source

- **Endpoint**: `/api/v1/project/:projectid/sources/:id`
- **Method**: GET
- **Description**: Retrieve a single source by id
- **Headers**: `Authorization: Bearer <token>`
- **Response**:

  ```json
  {
    "success": "boolean",
    "message": "string",
    "data": {
      "id": "integer",
      "name": "string",
      "type": "string",
      "version": "string",
      "config": "json",
      "created_at": "timestamp",
      "updated_at": "timestamp",
      "created_by": "string",
      "updated_by": "string",
      "jobs": [
        {
          "name": "string",
          "id": "integer",
          "activate": "boolean",
          "last_run_time": "timestamp",
          "last_run_state": "string",
          "dest_name": "string"
        }
      ]
    }
  }
  ```


### Update Source

- **Endpoint**: `/api/v1/project/:projectid/sources/:id`
- **Method**: PUT
- **Description**: Update an existing source
- **Headers**: `Authorization: Bearer <token>`
- **Version**: a version below `MinStreamsV2Version` returns `400` when any job of the source
  stores `available_streams_config` + `selected_streams_config`.
- **Request Body**:
  ```json
  {
    "name": "string",  
    "type": "string", 
    "version":"string", 
    "config": "json"
  }
  ```
- **Response**:
  ```json
  {
    "success": "boolean",
    "message": "string",
    "data": { // send same back
      "name": "string",  
      "type": "string", 
      "version":"string",
      "config": "json"
    }
  }
  ```

### Delete Source 

- **Endpoint**: `/api/v1/project/:projectid/sources/:id`
- **Method**: DELETE
- **Description**: Delete a source
- **Headers**: `Authorization: Bearer <token>`
- **Response**:
```json
  {    // Note: it is soft delete not hard delete
      "success": "boolean",
      "message": "string",
      "data": { 
        "name" :"string", // name of source deleted
      }
  }
```



## Destinations
### Get All Version Of Destinations 
- **Endpoint**: `/api/v1/project/:projectid/destinations/versions`
- **Method**: GET
- **Description**: Give spec based on source type
- **Headers**: `Authorization: Bearer <token>`
- **Request Body**:
  ```json
  {
    "type":"string",
  }
  ```
- **Response**:

  ```json
   {
    "success": "boolean",
    "message": "string",
    "data": {
     "version":["string","string"]
    }
  }
  ```
### Destination Spec
- **Endpoint**: `/api/v1/project/:projectid/destinations/spec`
- **Method**: GET
- **Description**: Give spec based on destination type
- **Headers**: `Authorization: Bearer <token>`
- **Request Body**:
  ```json
  {
    "type":"string",
    "version": "string"
  }
  ```
- **Response**:
  ```json
 {
    "success": "boolean",
    "message": "string",
    "data": {
      "type": "string",
      "version": "string",
      "spec": {
        "jsonschema": "object",
        "uischema": "string"
      }
    }
  }
  ```

// currently this is not avaialable in olake will build this

### Test Destination 

- **Endpoint**: `/api/v1/project/:projectid/destinations/test`
- **Method**: POST
- **Description**: Test configured destination configuration
- **Headers**: `Authorization: Bearer <token>`
- **Request Body**:

  ```json
  {
    "type": "string",
    "version": "string",
    "config": "json"
  }
  ```

- **Response**:
  ```json
 {
  "success": boolean,
  "message": "string",
  "data": {
    "connection_result": {
      "message": "string",
      "status": "string"
    },
    "logs": "json"
  }
}
  ```

### Create Destination

- **Endpoint**: `/api/v1/project/:projectid/destinations`
- **Method**: POST
- **Description**: Create a new destination
- **Headers**: `Authorization: Bearer <token>`
- **Request Body**:
  ```json
  {
    "name": "string",
    "type": "string",
    "config": "json",
    "version":"string"
  }
  ```
- **Response**:

  ```json
  {
    "success": "boolean",
    "message": "string",
    "data": {
      "name": "string",
      "type": "string",
      "config": "json",
      "version":"string" // to create a job same version of destination and same version of source required
    }
  }
  ```

### Get All Destinations

- **Endpoint**: `/api/v1/project/:projectid/destinations`
- **Method**: GET
- **Description**: Retrieve all destinations
- **Headers**: `Authorization: Bearer <token>`
- **Response**:
  ```json
{
    "success": "boolean",
    "message": "string",
    "data": [
      {
        "id": "integer",
        "name": "string",
        "type": "string",
        "config": "json",
        "version": "string",
        "created_at": "timestamp",
        "updated_at": "timestamp",
        "created_by": "string", // username only
        "updated_by": "string", // username only
        "jobs": [
          {
            "name": "string",
            "id": "integer",
            "activate": "boolean",
            "last_run_time": "timestamp",
            "last_run_state": "string",
            "source_name": "string"
          }
        ]
      }
    ]
  }
  ```

### Update Destination

- **Endpoint**: `/api/v1/project/:projectid/destinations/:id`
- **Method**: PUT
- **Description**: Update an existing destination
- **Headers**: `Authorization: Bearer <token>`
- **Request Body**:
  ```json
  {
    "name": "string",
    "type": "string",
    "config": "json",
    "version":"string"
  }
  ```
- **Response**:
  ```json
  {
    "success": "boolean",
    "message": "string",
    "data": {
      "name": "string",
      "type": "string",
      "version":"string",
      "config": "json"
    }
  }
  ```

### Get Destination

- **Endpoint**: `/api/v1/project/:projectid/destinations/:id`
- **Method**: GET
- **Description**: Retrieve a single destination by id
- **Headers**: `Authorization: Bearer <token>`
- **Response**:

  ```json
  {
    "success": "boolean",
    "message": "string",
    "data": {
      "id": "integer",
      "name": "string",
      "type": "string",
      "version": "string",
      "config": "json",
      "created_at": "timestamp",
      "updated_at": "timestamp",
      "created_by": "string",
      "updated_by": "string",
      "jobs": [
        {
          "name": "string",
          "id": "integer",
          "activate": "boolean",
          "last_run_time": "timestamp",
          "last_run_state": "string",
          "source_name": "string"
        }
      ]
    }
  }
  ```

### Delete Destination

- **Endpoint**: `/api/v1/project/:projectid/destinations/:id`
- **Method**: DELETE
- **Description**: Delete a destination
- **Headers**: `Authorization: Bearer <token>`
- **Response**:

```json
  { // NOTE: this is only soft delete not hard
    "success": "boolean",
    "message": "string",
    "data": {
      "name": "string"
    }
  }
```

## Jobs

### Create Job

- **Endpoint**: `/api/v1/project/:projectid/jobs`
- **Method**: POST
- **Description**: Create a new job
- **Headers**: `Authorization: Bearer <token>`
- **Streams format**: send the catalog as `streams_config`, or as `available_streams_config` +
  `selected_streams_config` (always both; source version ≥ `MinStreamsV2Version`). An invalid
  catalog format returns `400`.
- **Request Body**:

  ```json
  {
    "name": "string",
    "source": {
      "name": "string",
      "type": "string",
      "config": "json",
      "version": "string"
    },
    "destination": {
      "name": "string",
      "type": "string",
      "config": "string",
      "version": "string"
    },
    "frequency": "string",
    "streams_config": "json",               // legacy jobs
    "available_streams_config": "json",     // v2 jobs only
    "selected_streams_config": "json"       // v2 jobs only
  }
  ```

- **Response**:
  ```json
  {
    "success": "boolean",
    "message": "string",
    "data": {
      // request body as it is 
    }
  }
  ```

### Get All Jobs

- **Endpoint**: `/api/v1/project/:projectid/jobs`
- **Method**: GET
- **Description**: Retrieve all jobs
- **Headers**: `Authorization: Bearer <token>`
- **Response**:
  ```json
  {
    "success": "boolean",
    "message": "string",
    "data": [
      {
        "id": "int",
        "name": "string",
        "source": {
          "name": "string",
          "type": "string",
          "version": "string"
        },
        "destination": {
          "name": "string",
          "type": "string",
          "version": "string"
        },
        "frequency": "string",
        "last_run_time": "timestamp",
        "last_run_state": "string",
        "last_run_type": "string",
        "created_at": "timestamp",
        "updated_at": "timestamp",
        "activate": "boolean",
        "created_by":  "string", // username 
        "updated_by":  "string" // username
      // can also send state but if it is required
      }
    ]
  }
  ```

### Get Job

- **Endpoint**: `/api/v1/project/:projectid/jobs/:id`
- **Method**: GET
- **Description**: Retrieve a single job by id
- **Headers**: `Authorization: Bearer <token>`
- **Response**:

  ```json
  {
    "success": "boolean",
    "message": "string",
    "data": {
      "id": "int",
      "name": "string",
      "source": {
        "name": "string",
        "type": "string",
        "config": "json",
        "version": "string"
      },
      "destination": {
        "name": "string",
        "type": "string",
        "config": "json",
        "version": "string"
      },
      "streams_config": "json",               // legacy jobs only
      "available_streams_config": "json",     // v2 jobs only
      "selected_streams_config": "json",      // v2 jobs only
      "frequency": "string",
      "last_run_time": "timestamp",
      "last_run_state": "string",
      "last_run_type": "string",
      "created_at": "timestamp",
      "updated_at": "timestamp",
      "activate": "boolean",
      "created_by":  "string",
      "updated_by":  "string"
    }
  }
  ```

### Update Job

- **Endpoint**: `/api/v1/project/:projectid/jobs/:id`
- **Method**: PUT
- **Description**: Update an existing job
- **Headers**: `Authorization: Bearer <token>`
- **Streams format**: send the catalog as `streams_config`, or as `available_streams_config` +
  `selected_streams_config` (always both; source version ≥ `MinStreamsV2Version`). A job that
  stores `available_streams_config` + `selected_streams_config` cannot switch back to
  `streams_config`. An invalid catalog format returns `400`.
- **Request Body**:

  ```json
  {
    "name": "string",
    "source": {
      "name": "string",
      "type": "string",
      "config": "json",
      "version": "string"
    },
    "destination": {
      "name": "string",
      "type": "string",
      "config": "json",
      "version": "string"
    },
    "frequency": "string",
    "streams_config": "json",               // legacy jobs
    "available_streams_config": "json",     // v2 jobs only
    "selected_streams_config": "json",      // v2 jobs only
    "difference_streams": "string",
    "activate": "boolean" // send this to activate or deactivate job
  }
  ```

- **Response**:
  ```json
  {
    "success": "boolean",
    "message": "string",
    "data": {
      "name": "string",
      "source": {
        "name": "string",
        "type": "string",
        "config": "json",
        "version": "string"
      },
      "destination": {
        "name": "string",
        "type": "string",
        "config": "json",
        "version": "string"
      },
      "frequency": "string",
      "streams_config": "json",
      "activate": "boolean"
    }
  }
  ```





### Delete Job

- **Endpoint**: `/api/v1/project/:projectid/jobs/:id`
- **Method**: DELETE
- **Description**: Delete a job
- **Headers**: `Authorization: Bearer <token>`
- **Response**:

```json
{
    "success": "boolean",
    "message": "string",
    "data": {
      "name": "boolean"
    }
}
```

### Source Associated Streams (Discover Catalog)

- **Endpoint**: `/api/v1/project/:projectid/sources/streams`
- **Method**: POST
- **Description**: Discover the source's streams.
  - existing job (`job_id` >= 0): discovers from the job's stored catalog and returns it in the
    fields the job stores it in.
  - new job (`job_id` = -1): returns `available_streams` + `selected_streams` when the driver writes
    them (version ≥ `MinStreamsV2Version`), otherwise `streams_config`.
  - Exactly one format is set; the fields of the other format are `null`. Check
    `available_streams` first, then fall back to `streams_config`.
- **Headers**: `Authorization: Bearer <token>`
- **Request Body**:

  ```json
  {
    "name": "string",
    "type": "string",
    "version": "string",
    "config": "json",
    "job_id": "integer",
    "job_name": "string"
  }
  ```

- **Response**: `202` with `operation_id` (see [Async operations](#async-operations)). The operation
  result is below; each field is a catalog file exactly as the CLI wrote it, so a client can send it
  back unchanged on job create/update.
  - `streams_config`: `streams.json` — `{ streams, selected_streams }`; `selected_streams` entries are
    fully written out.
  - `available_streams`: `available_streams.json` — holds `streams`.
  - `selected_streams`: `selected_streams.json` — `{ selected_streams }`; entries are sparse
    (`normalization`/`append_mode`/`update_type` fall back to `default_stream_properties`, absent
    `selected_columns` means all columns, `sync_mode`/`cursor_field`/`destination_*` override the
    stream's values).

  ```json
  {
    "success": "boolean",
    "message": "string",
    "data": {
      "streams_config": "json",            // legacy; null for split
      "available_streams": "json",         // split; null for legacy
      "selected_streams": "json"           // split; null for legacy
    }
  }
  ```

### Job Sync

- **Endpoint**: `/api/v1/project/:projectid/jobs/:id/sync`
- **Method**: POST
- **Description**: Sync the job
- **Headers**: `Authorization: Bearer <token>`
- **Response**:

  ```json
  {
    "success": "boolean",
    "message": "string"
  }
  ```

### Activate/Inactivate Job

- **Endpoint**: `/api/v1/project/:projectid/jobs/:id/activate`

- **Method**: POST
- **Description**: Update the activate status of job
- **Headers**: `Authorization: Bearer <token>`
- **Request Body**:

  ```json
  {
   "activate":boolean
  }
  ```

- **Response**:
  ```json
  {
    "success": "boolean",
    "message": "string",
    "data": {
      // request body as it is
    }
  }
  ```

### Job Tasks

- **Endpoint**: `/api/v1/project/:projectid/jobs/:jobid/tasks`
- **Method**: GET
- **Description**: Give the History of jobs
- **Headers**: `Authorization: Bearer <token>`

- **Response**:

  ```json
  {
    "success": "boolean",
    "message": "string",
    "data": [
      {
        "file_path": "string",
        "start_time": "timestamp",
        "runtime": "integer",
        "status": "string",
        "job_type": "string"
      }
    ]
  }
  ```
### cancel Job workflow

- **Endpoint**: `/api/v1/project/:projectid/jobs/:jobid/cancel`
- **Method**: GET
- **Description**: Cancel the job workflow
- **Headers**: `Authorization: Bearer <token>`

- **Response**:

  ```json
  {
    "success": "boolean",
    "message": "string",
    "data": {
      "message": "string"
    }
  }
  ```


### Activate/Inactivate Job

- **Endpoint**: `/api/v1/project/:projectid/jobs/:id/activate`
- **Method**: POST
- **Description**: Update the activate status of job
- **Headers**: `Authorization: Bearer <token>`
- **Request Body**:

  ```json
  {
   "activate":boolean
  }
  ```

  - **Response**:

  ```json
  {
    "success": "boolean",
    "message": "string",
    "data": {
      "activate": "boolean"
    }
  }
  ```

### Fetch Job Task Logs

- **Endpoint**: `/api/v1/project/:projectid/jobs/:jobid/tasks/:taskid/logs`
- **Method**: POST
- **Description**: Fetch logs for a specific job task with cursor-based pagination supporting both older and newer directions. Sync logs come from the connector (`olake.log`, or `connector-*` chunks in S3 storage mode); worker logs come from the worker that ran the job (`worker.log`, or `logs/worker/worker-*` chunks in S3 mode). With `source=all` both are merged by time.
- **Headers**: `Authorization: Bearer <token>`
- **Query Params**:
  - `source` _(optional, string)_: `"all"` (default), `"sync"` or `"worker"`.
  - `cursor` _(optional, string)_: opaque cursor from a previous response (`older_cursor` / `newer_cursor`). Omit or send empty for tailing from the end.
  - `limit` _(optional, number)_: number of log entries to return. Defaults to `1000`, capped at `5000`.
  - `direction` _(optional, string)_: `"older"` (default) to read towards the start of the file, or `"newer"` to read towards the end.
- **Request Body**:

  ```json
  {
    "file_path": "string"
  }
  
- **Response**:

  ```json
  {
    "success": "boolean",
    "message": "string",
    "data": {
      "logs": [
        {
          "level": "string",
          "time": "string", // RFC3339, UTC
          "message": "string",
          "source": "string", // "sync" or "worker"
          "tip": "boolean" // optional, true for recommendation lines
        }
      ],
      "older_cursor": "string", // opaque, position before the first returned line
      "newer_cursor": "string", // opaque, position after the last returned line
      "has_more_older": "boolean",
      "has_more_newer": "boolean"
    }
  }

  ```

### Download Job Logs

---

- **Endpoint**: `/api/v1/project/:projectid/jobs/:id/logs/download`
- **Method**: GET
- **Description**: Download all log files and state.json for a specific job task as a compressed tar.gz archive. The archive includes all files from the sync logs directory and the state.json file.
- **Headers**: `Authorization: Bearer <token>`
- **Query Parameters**:
  - `file_path` _(required, string)_: file path of the task

- **Response**:  
  When requested, the browser will start downloading a file named `job-{id}-logs-{timestamp}.tar.gz`, which is a compressed archive containing all logs and the state file.  
  - **Content-Type**: `application/gzip`  
  - **Content-Disposition**: `attachment; filename="job-{id}-logs-{timestamp}.tar.gz"`  
  - **Cache-Control**: `no-cache`  
  - **Body**: Binary stream (tar.gz archive)  
  - **Archive Structure**:
    ```
    job-{id}-logs-{timestamp}.tar.gz
    ├── logs/
    │   ├── olake.log
    │   ├── worker.log
    │   └── [other log files]
    └── state.json  
    ```

### Check Unique Name
---

- **Endpoint**: `/api/v1/project/:projectid/check-unique`
- **Method**: POST
- **Description**: Check if a name is unique for a given entity type (job, source, or destination)
- **Headers**: `Authorization: Bearer <token>`
- **Request Body**:

  ```json
  {
    "name": "string",
    "entity_type": "job" | "source" | "destination"
  }
  ```

- **Response**:

  ```json
  {
    "success": "boolean",
    "message": "string",
    "data": {
     "unique": "boolean"
    }
  }
  ```


### Clear destination for a Job
---

- **Endpoint**: `/api/v1/project/:projectid/jobs/:id/clear-destination`
- **Method**: POST
- **Description**: clears the destination data
- **Headers**: `Authorization: Bearer <token>`

- **Response**: 

  ```json
  {
    "success": "boolean",
    "message": "string"
  }
  ```

### Get Clear Destination Status
---

- **Endpoint**: `/api/v1/project/:projectid/jobs/:id/clear-destination`
- **Method**: GET
- **Description**: Get the status of clear destination operation for a job
- **Headers**: `Authorization: Bearer <token>`

- **Response**: 

  ```json
  {
    "success": "boolean",
    "message": "string",
    "data": {
      "running": "boolean"
    }
  }
  ```
  
  ### Difference Streams
- **Endpoint**: `/api/v1/project/:projectid/jobs/:id/stream-difference`
- **Method**: POST
- **Description**: returns the stream difference between the job's saved catalog and the updated
  one. Send the updated catalog as `updated_streams_config` or as
  `updated_available_streams_config` + `updated_selected_streams_config`. An invalid catalog
  format returns `400` before any operation starts.
- **Headers**: `Authorization: Bearer <token>`
- **Request Body**:

  ```json
  {
    "updated_streams_config": "json", // legacy format
    "updated_available_streams_config": "json", // split format
    "updated_selected_streams_config": "json" // split format
  }
  ```

- **Response**: `202` with `operation_id` (see [Async operations](#async-operations)). Operation
  result (for a split catalog, `difference_streams` is `difference_selected_streams.json`):

  ```json
  {
    "success": "boolean",
    "message": "string",
    "data": {
      "difference_streams": "json"
    }
  }
  ```

### Get System Settings

---

- **Endpoint**: `/api/v1/project/:projectid/settings`
- **Method**: GET
- **Description**: Get system-level settings for a project (including webhook alert URL).
- **Headers**: `Authorization: Bearer <token>`

- **Response**:

  ```json
  {
    "success": "boolean",
    "message": "string",
    "data": {
      "id": "number",
      "project_id": "string",
      "webhook_alert_url": "string"
    }
  }
  ```

### Update System Settings

---

- **Endpoint**: `/api/v1/project/:projectid/settings`
- **Method**: PUT
- **Description**: Update system-level settings for a project.
- **Headers**: `Authorization: Bearer <token>`

- **Request Body**:

  ```json
  {
    "id": "number (optional)",
    "project_id": "string",
    "webhook_alert_url": "string"
  }
  ```

- **Response**:

  ```json
  {
    "success": "boolean",
    "message": "string"
  }
  ```


## Platform

### Get Release Updates

---

- **Endpoint**: `/api/v1/platform/releases`
- **Method**: GET
- **Description**: Fetch release metadata for Olake UI and Worker, Helm, and Olake.
- **Headers**: `Authorization: Bearer <token>`
- **Query Parameters**:
  - `limit` _(optional, number)_: Max number of releases to fetch for each component.

- **Response**:

  ```json
  {
    "success": "boolean",
    "message": "string",
    "data": {
      "features": {
        "releases": ["json"]
      },
      "olake_ui_worker": {
        "current_version": "string",
        "releases": ["json"]
      },
      "olake_helm": {
        "releases": ["json"]
      },
      "olake": {
        "current_version": "string",
        "releases": ["json"]
      }
    }
  }
  ```

## Error Responses

All endpoints may return the following error responses:

### 400 Bad Request

```json
{
  "success": false,
  "message": "Bad request"
}
```

### 401 Unauthorized

```json
{
  "success": false,
  "message": "Authentication required"
}
```

### 403 Forbidden

```json
{
  "success": false,
  "message": "Insufficient permissions"
}
```

### 404 Not Found

```json
{
  "success": false,
  "message": "Resource not found"
}
```

### 500 Internal Server Error

```json
{
  "success": false,
  "message": "Internal server error"
}
```

## CORS Configuration

The API allows requests from:

- Origin: `http://localhost:8000`
- Methods: GET, POST, PUT, DELETE, OPTIONS
- Headers: Origin, Content-Type, Accept, Authorization
- Credentials: true

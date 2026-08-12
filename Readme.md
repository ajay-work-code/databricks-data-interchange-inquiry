# Data Interchange Inquiry Project — Process Flow

Weekly ingest of storage locations by CareTria.

**Source:** Excel file emailed weekly by CareTria (manually generated from the Warehouse
Anywhere portal), dropped into a Unity Catalog volume `manual_inputs`.
**Target:** Bronze append-only history, Silver current snapshot (one row per representative).

This is approached as a **three-task Databricks job** — ingest, Silver refresh, and QA — so
that each stage runs on the compute it needs and in a guaranteed order.

Catalog is a job parameter, so promotion to `bis_prod` is a parameter change, not a code change.

---

## 1. End-to-end flow

```Diagram
CareTria email (weekly, Mondays)
   │
   ▼
/Volumes/<catalog>/bronze_roster/manual_inputs/
   │   Auto Loader claims only files matching *[Ss]torage*[Ss]pace*.xlsx
   │   Checkpoint tracks which files are already loaded
   ▼
TASK 1  Data Interchange Inquiry  (notebook)
   │   decode Excel  →  normalize  →  parse address  →  append
   ▼
<catalog>.bronze_roster.emp_storage_location          append-only, full history
   │   + <catalog>.bronze_roster.autoloader_audit_log   one row per load attempt
   ▼
TASK 2  Silver Refresh Pipeline  (Refresh Silver Roster)
   │   REFRESH the materialized view
   ▼
<catalog>.silver_roster.emp_storage_location          latest file only, 1 row per Emp_ID
   ▼
TASK 3  QA Checks  (notebook)
   │   3 Bronze checks + 1 Silver check
   ▼
<catalog>.bronze_roster.roster_qa                     one row per check per run
```

The volume is a shared drop zone, so the file glob is deliberately strict — the pipeline
never claims a file it was not meant to read. Also the excel stricts to pick the sheet name `Spaces` since it was from the 7/29 file.
This Sheet name also parameter change not a code change.

---

## 2. Ingest — the Data Interchange Inquiry notebook

This notebook is the entry point. It performs the ingest with Auto Loader and lands parsed
rows in Bronze. Address parsing lives in a separate utility notebook,
`parse_usaddress_util`, which is invoked at the top with a relative `%run` so a single
implementation is shared rather than duplicated.

### Cell flow

| Order | Cell                        | Purpose                                                                                         |
| ----- | --------------------------- | ----------------------------------------------------------------------------------------------- |
| 1     | `%run parse_usaddress_util` | Loads the address parsing utility and its output contract                                       |
| 2     | Configuration               | Job parameters, table and volume paths, column mapping, Bronze schema, filename date extraction |
| 3     | Excel reader                | Sheet selection, header validation, cell normalization, row assembly                            |
| 4     | Batch loader                | Decodes each file in a micro-batch, parses addresses, appends to Bronze, writes the audit row   |
| 5     | Auto Loader                 | Starts the stream and reports the outcome                                                       |

The utility notebook loads first because the configuration cell builds the Bronze schema from
the parser's output contract — the dependency points one way, so the parser stays portable.

### Auto Loader conditions and filters

Three separate controls decide what gets read:

**File glob** — `*[Ss]torage*[Ss]pace*.xlsx`

`manual_inputs` is a shared drop zone, so the pipeline must claim only the storage location
file and nothing else in the volume. A non-matching file is never seen by the stream at all.

**Checkpoint** — tracks consumed files so the same file is not loaded twice

The checkpoint records file **paths** that have already been processed. On each run, Auto
Loader lists the volume, compares against that record, and processes only what is new. A file
is marked consumed only once its batch commits — so a failed batch leaves the file available
for retry rather than silently dropping it.

**Sheet name** — `Spaces`

The reader targets the named worksheet rather than whichever sheet happens to be active.
If `Spaces` is absent it falls back to the only visible sheet, and only when exactly one
exists — so the fallback can never be ambiguous.

Alongside these, the reader validates the **header row by name**, case- and
whitespace-insensitive, never by column position. A reordered export is handled without a
code change; a missing expected column fails the load.

### Ingest format

Auto Loader reads in `binaryFile` format — the raw `.xlsx` bytes rather than parsed rows,
because Spark cannot read Excel natively. The bytes are decoded with `openpyxl` inside the
batch loader.

Excel was chosen over CSV because the Facility Address field contains a comma in every row of
the source file. A manually exported CSV can mis-quote those addresses and shift columns
silently; Excel is structurally unambiguous.

### Normalization

Every cell is normalized to a trimmed string, and blanks become NULL. This matters because
Excel types cells individually: Territory ID and Emp ID arrive as numbers, and Space Number
arrives as a mix of numbers and text within the same column.

### Load fails, and nothing is written, if

- any of the 12 expected columns is missing from the header
- the `Spaces` sheet is absent and there is no single unambiguous fallback sheet
- the file is corrupt or unreadable

A failure leaves the checkpoint unadvanced, so the file is retried on the next run rather
than being silently consumed. The failure is written to the audit log first.

### Address parsing

Addresses are parsed at Bronze, per John's 7/29 direction ("parse out the addresses and then
load that data into a table"). The parsing checklist placed this at Silver; the output
columns are identical either way, and this note records that the layering differs from that
draft.

`parse_usaddress_util` consolidates the prototype logic into one function. Output:
`Address_1`, `Address_2`, `City`, `State`, `Zip_Code`, `Parsing_Error`. The raw address is
always retained in `Facility_Raw_Address` so a failed parse can be investigated or reparsed.

---

## 3. Bronze — how data lands

**Table:** `<catalog>.bronze_roster.emp_storage_location`
**Strategy:** full append, no updates, no deletes. Every week's file stays in history.

### Columns (22)

| Group       | Columns                                                                |
| ----------- | ---------------------------------------------------------------------- |
| Source (12) | The 12 columns supplied in the vendor extract, all String              |
| Parsed (6)  | `Address_1`, `Address_2`, `City`, `State`, `Zip_Code`, `Parsing_Error` |
| Lineage (4) | `File_Date`, `File_Name`, `Load_Timestamp`, `_rescued_data`            |

**`File_Date`** — taken from the filename where a US-format date is present
(`MMDDYYYY`, `MM-DD-YYYY`, `MM_DD_YYYY`, `MM.DD.YYYY`), converted to a date. If the filename
carries no usable date, it falls back to the file's modification time on the volume,
converted to UTC. This satisfies "actual metadata from the file, otherwise the load date."

**`File_Name`** — the source file the row came from, so any row in Bronze can be traced back
to a specific delivery.

**`Load_Timestamp`** — UTC time the ingest wrote the row. Used to order rows within a
`File_Date`, which is what lets Silver resolve duplicates deterministically.

**`_rescued_data`** — the rescue column. Detection is at **column** level: a new, renamed or
unexpected header in the source file is captured here as JSON. Under an all-String schema
there are no value-level type mismatches to rescue, so column drift is the meaningful signal.

**All columns are nullable** at the table level. The mapping marks most as `Nullable = No`,
which is honored as a QA expectation and a catalog comment rather than a physical constraint —
the 7/29 file has one blank `Storage_Size` cell, and a `NOT NULL` constraint would have
rejected all 173 rows over that single blank.

### Audit log

`<catalog>.bronze_roster.autoloader_audit_log` records every load attempt:

| Status          | Meaning                                                           |
| --------------- | ----------------------------------------------------------------- |
| `SUCCESS`       | Files decoded and rows appended                                   |
| `FAILED`        | A file could not be decoded; nothing was written, file will retry |
| `EMPTY`         | Files were read but contained no data rows                        |
| `NO_NEW_FILES`  | The stream ran and found nothing unprocessed                      |
| `STREAM_FAILED` | The stream itself failed before or outside file processing        |

This exists so that "the job ran and did nothing" is distinguishable from "the job ran and
loaded a file" without inspecting notebook output.

---

## 4. Silver — how the current snapshot is built

**Object:** `<catalog>.silver_roster.emp_storage_location`, a **materialized view**, defined
in a Lakeflow pipeline.

Silver is a snapshot of one file, not accumulated history. Three sequential filters:

**1. Latest file only** — `WHERE File_Date = (SELECT MAX(File_Date) FROM bronze)`

John's requirement: "a materialized view with only the most recent file that was loaded."
This is also what handles removals with no delete logic — a representative dropped from this
week's extract simply stops appearing.

**2. One row per Emp_ID** — `ROW_NUMBER() OVER (PARTITION BY Emp_ID ORDER BY ...)`, keep `rn = 1`

The requirement: only one record per `Emp_ID` in Silver, using `row_number` in case a file
violates one row per emp id — trip a warning, and pick one.

A single file **can** contain the same Emp_ID twice — Ryan Sandy appeared on the 7/28 extract
with two facilities under Emp ID 6584. Silver resolves it to one row; the Bronze QA check
reports that a duplicate existed. Two halves of one requirement.

**3. Phone transform** — digits only, leading `1` removed when the result is 11 digits

Guarded on length so a legitimate 10-digit number beginning with 1 does not lose its area
code.

### Why refresh is a separate task

Materialized view operations cannot run from serverless generic compute. `CREATE` and
`REFRESH` both fail there with `MV_NOT_ENABLED_ON_SERVERLESS_GENERIC_COMPUTE`. The refresh
therefore runs as its own pipeline task rather than a cell in the ingest notebook.

This separation is also correct on ordering grounds: Silver QA must run **after** the refresh,
or it validates the previous week's snapshot and reports a pass.

---

## 5. Setup — once per environment

Create in this order:

1. **Schemas** — `bronze_roster` and `silver_roster`
2. **Volume** — `manual_inputs` under `bronze_roster`, the drop zone for the weekly file
3. **Tables** — `emp_storage_location`, `roster_qa` and `autoloader_audit_log`, with column
   comments
4. **Silver materialized view** — created by the first run of the Lakeflow pipeline

The checkpoint is created automatically under `manual_inputs/_checkpoints/`. If a
`_checkpoints` directory already exists in the volume, the pipeline's own subdirectory sits
alongside anything else there — no separate setup step, and no conflict with other pipelines
using the same convention.

Column comments for the 12 source columns, `File_Date`, `File_Name` and the rescue column come
from the agreed mapping table verbatim. The remaining columns are documented in the same style.

---

## 6. QA checks

Four checks were added based on the email thread, run as the final task. Results are written
to `<catalog>.bronze_roster.roster_qa`, one row per check per run, so results are queryable
over time rather than only visible in notebook output.

| Layer  | Check                           | Severity | Requirement                                                                 |
| ------ | ------------------------------- | -------- | --------------------------------------------------------------------------- |
| Bronze | `email_matches_roster`          | ERROR    | Check the email provided for each emp id matches the email in the roster    |
| Bronze | `one_location_per_emp_per_file` | WARNING  | Check that each file only has 1 location per emp id                         |
| Bronze | `rescued_data_present`          | WARNING  | Alert if any rescue data is found                                           |
| Silver | `address_not_parsed`            | WARNING  | Flag any address not parsed —`Address_1` blank or `Parsing_Error` populated |

Checks report; they do not halt the pipeline. A duplicate Emp_ID or an unparsed address is
surfaced for review, not treated as a load failure.

`email_matches_roster` is configured but its roster table is set to `None` for now, since the
table and column to match against have not been confirmed. It records as **SKIPPED**, which is
reported distinctly from a pass — the run summary reads "2 passed, 1 skipped" rather than
implying the roster was verified.

---

## 7. Proposed job structure

The proposed orchestration is three sequential tasks:

```Diagram
┌──────────────────────────┐    ┌──────────────────────┐    ┌──────────────────────┐
│ Data Interchange Inquiry │    │    Refresh_Silver    │    |  QA_Checks           │
│ notebook                 │  → │  Lakeflow pipeline   │  → │  notebook            │
│ serverless               │    │                      │    │  serverless          │
└──────────────────────────┘    └──────────────────────┘    └──────────────────────┘
      volume → Bronze                REFRESH the MV            3 Bronze + 1 Silver
      + audit log                                              → roster_qa
```

Each task depends on the one before it. If ingest fails, the refresh is skipped and Silver
retains last week's snapshot — the correct outcome.

**Compute.** Task 1 needs `usaddress` and `openpyxl`, declared as serverless task environment
dependencies or added as cluster libraries. Task 3 is pure SQL and needs neither.

**Parameters.** `CATALOG` (default `bis_dev`) and `SHEET_NAME` (default `Spaces`) are job
parameters, so promoting to production is a parameter change.

**Schedule.** The schedule is not yet fixed.

> A run where no new file arrived **succeeds**. It is not a failure — it logs
> `NO_NEW_FILES`, refreshes Silver over unchanged Bronze, and QA passes. Distinguishing
> "no file arrived" from "file arrived and loaded" requires reading the audit log.

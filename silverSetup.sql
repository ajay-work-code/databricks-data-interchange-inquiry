

-- =====================================================================
-- 02_silver_materialized_view.sql
-- Run ONCE, from a SQL warehouse. NOT from the ingest notebook.
--
-- Materialized views cannot be created from serverless generic compute
-- without the workspace feature preview, and CREATE OR REPLACE in a weekly
-- job would tear down and rebuild the backing pipeline every run.
--
-- To pick up new data after each Bronze load:
--     REFRESH MATERIALIZED VIEW bis_dev.silver_roster.emp_storage_location;
--
-- NOTE: This query may be slow because it scans the entire
-- bis_dev.bronze_roster.emp_storage_location table to find the latest File_Date,
-- then filters and ranks all rows for that date. If the table is large or not
-- partitioned by File_Date, performance will be impacted.
-- Consider partitioning the source table by File_Date and/or optimizing with ZORDER.
-- =====================================================================

CREATE OR REFRESH MATERIALIZED VIEW bis_dev.silver_roster.emp_storage_location
COMMENT 'Current storage location per representative. Most recent file only, one row per Emp_ID.'
AS
WITH latest AS (
    -- Most recent file only
    SELECT *
    FROM bis_dev.bronze_roster.emp_storage_location
    WHERE File_Date = (SELECT MAX(File_Date) FROM bis_dev.bronze_roster.emp_storage_location)
),
ranked AS (
    -- One row per Emp_ID. If a file carries duplicates, the most recently
    -- loaded row wins; the Bronze QA check reports that this happened.
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY Emp_ID ORDER BY Load_Timestamp DESC) AS rn
    FROM latest
),
phone AS (
    SELECT *, regexp_replace(Rep_Phone, '[^0-9]', '') AS digits
    FROM ranked
    WHERE rn = 1
)
SELECT
    Emp_ID,
    Rep_Name,
    Rep_Email,
    CASE WHEN length(digits) = 11 AND left(digits, 1) = '1'
         THEN substr(digits, 2)
         ELSE digits
    END AS Rep_Phone,
    Territory_ID,
    Territory_Name,
    Region_Emp_Name,
    Region_Name,
    Facility_Name,
    Space_Number,
    Storage_Size,
    Facility_Raw_Address,
    Address_1,
    Address_2,
    City,
    State,
    Zip_Code,
    Parsing_Error,
    File_Date,
    File_Name
FROM phone;
     

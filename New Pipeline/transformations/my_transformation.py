import pyspark.pipelines as dp
from pyspark.sql import SparkSession

@dp.materialized_view(
    name="bis_dev.silver_roster.emp_storage_location",
    comment="",
)
def silver_roster():
    spark = SparkSession.getActiveSession()
    return spark.sql("""
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
        FROM phone
    """)
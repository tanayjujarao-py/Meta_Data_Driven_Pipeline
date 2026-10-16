from ingestion.snowflake_connection import get_snowflake_connection


def load_file(pipeline, file):

    connection = get_snowflake_connection()
    cursor = connection.cursor()

    target_database = pipeline["TARGET_DATABASE"]
    target_schema = pipeline["TARGET_SCHEMA"]
    target_table = pipeline["TARGET_TABLE"]

    stage_name = pipeline["STAGE_NAME"]
    source_path = pipeline["SOURCE_PATH"]

    source_file = file["file_name"]

    # Extract only the filename from the full S3 path
    file_name = source_file.split("/")[-1]

    # Build the path relative to the Snowflake stage
    stage_file_path = f"{source_path}{file_name}"

    target_table_name = f"{target_database}.{target_schema}.{target_table}"

    # --------------------------------------------------
    # CUSTOMER FILE
    # --------------------------------------------------

    if target_table == "RAW_CUSTOMERS":

        query = f"""
            COPY INTO {target_table_name}
            (
                CUSTOMER_ID,
                FIRST_NAME,
                LAST_NAME,
                EMAIL,
                CITY,
                REGISTRATION_DATE,
                UPDATED_AT,
                INGESTED_AT,
                SOURCE_FILE
            )
            FROM (
                SELECT
                    $1,
                    $2,
                    $3,
                    $4,
                    $5,
                    TO_DATE($6, 'DD-MM-YYYY'),
                    TO_TIMESTAMP_NTZ($7, 'DD-MM-YYYY HH24:MI'),
                    CURRENT_TIMESTAMP(),
                    METADATA$FILENAME
                FROM @{stage_name}/{stage_file_path}
            )
            FILE_FORMAT = (
                FORMAT_NAME = META_DATA_PIPELINE.RAW.CSV_FORMAT
            )
        """

    # --------------------------------------------------
    # ORDER FILE
    # --------------------------------------------------

    elif target_table == "RAW_ORDERS":

        query = f"""
            COPY INTO {target_table_name}
            (
                ORDER_ID,
                CUSTOMER_ID,
                ORDER_DATE,
                PRODUCT_NAME,
                CATEGORY,
                QUANTITY,
                UNIT_PRICE,
                ORDER_AMOUNT,
                ORDER_STATUS,
                UPDATED_AT,
                INGESTED_AT,
                SOURCE_FILE
            )
            FROM (
                SELECT
                    $1,
                    $2,
                    TO_DATE($3, 'DD-MM-YYYY'),
                    $4,
                    $5,
                    $6::NUMBER,
                    $7::NUMBER(12,2),
                    $8::NUMBER(14,2),
                    $9,
                    TO_TIMESTAMP_NTZ($10, 'DD-MM-YYYY HH24:MI'),
                    CURRENT_TIMESTAMP(),
                    METADATA$FILENAME
                FROM @{stage_name}/{stage_file_path}
            )
            FILE_FORMAT = (
                FORMAT_NAME = META_DATA_PIPELINE.RAW.CSV_FORMAT
            )
        """

    else:

        raise ValueError(f"Unsupported target table: {target_table}")

    try:

        print(f"Loading file: {source_file}")
        print(f"Target table: {target_table_name}")

        cursor.execute(query)

        results = cursor.fetchall()

        return results

    finally:

        cursor.close()
        connection.close()

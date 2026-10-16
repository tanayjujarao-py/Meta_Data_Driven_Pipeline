import uuid
from datetime import datetime

from ingestion.metadata_reader import get_active_pipelines
from ingestion.file_discovery import discover_files
from ingestion.ingestion_log import (
    is_file_processed,
    write_ingestion_log,
)
from ingestion.loader import load_file


def run_pipeline():

    # Generate a unique ID for this pipeline run
    run_id = str(uuid.uuid4())

    print("=" * 70)
    print("METADATA-DRIVEN PIPELINE STARTED")
    print(f"RUN ID: {run_id}")
    print("=" * 70)

    # Get all active pipelines from metadata
    pipelines = get_active_pipelines()

    for pipeline in pipelines:

        pipeline_id = pipeline["PIPELINE_ID"]
        pipeline_name = pipeline["PIPELINE_NAME"]

        print("\n" + "=" * 70)
        print(f"PIPELINE: {pipeline_name}")
        print("=" * 70)

        # Discover files for this pipeline
        files = discover_files(pipeline)

        if not files:

            print("No files found.")

            continue

        for file in files:

            source_file = file["file_name"]
            file_size = file["file_size"]
            file_last_modified = file["last_modified"]

            # Check whether this file was already successfully processed
            processed = is_file_processed(
                pipeline_id,
                source_file,
            )

            if processed:

                print(f"SKIPPING: {source_file}")
                print("Reason: File was already successfully processed.")

                continue

            start_time = datetime.now()

            print(f"PROCESSING: {source_file}")

            try:

                # Load the file into Snowflake RAW table
                results = load_file(
                    pipeline,
                    file,
                )

                # COPY INTO returns information about the load
                rows_loaded = results[0][3]

                end_time = datetime.now()

                # Write SUCCESS record to ingestion log
                write_ingestion_log(
                    run_id=run_id,
                    pipeline_id=pipeline_id,
                    source_file=source_file,
                    file_size=file_size,
                    file_last_modified=file_last_modified,
                    status="SUCCESS",
                    rows_loaded=rows_loaded,
                    start_time=start_time,
                    end_time=end_time,
                    error_message=None,
                )

                print(f"SUCCESS: {source_file}")
                print(f"ROWS LOADED: {rows_loaded}")

            except Exception as error:

                end_time = datetime.now()

                print(f"FAILED: {source_file}")
                print(f"ERROR: {error}")

                # Write FAILED record to ingestion log
                write_ingestion_log(
                    run_id=run_id,
                    pipeline_id=pipeline_id,
                    source_file=source_file,
                    file_size=file_size,
                    file_last_modified=file_last_modified,
                    status="FAILED",
                    rows_loaded=0,
                    start_time=start_time,
                    end_time=end_time,
                    error_message=str(error),
                )

    print("\n" + "=" * 70)
    print("METADATA-DRIVEN PIPELINE FINISHED")
    print("=" * 70)


if __name__ == "__main__":
    run_pipeline()

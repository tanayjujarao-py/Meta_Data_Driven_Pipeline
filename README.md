🚀 Metadata-Driven Data Ingestion PipelineAn enterprise-grade, metadata-driven data ingestion framework built with Python, Snowflake, Amazon S3, and Apache Airflow. This pipeline dynamically configures, discovers, loads, and audits data pipelines without requiring code changes for new data sources.🌟 Key Features & In-Demand Engineering SkillsMetadata-Driven Architecture: Decouples pipeline logic from configuration. New pipelines and data sources are onboarded simply by inserting records into the PIPELINE_CONFIG table rather than modifying application code.   Idempotency & Duplicate Prevention: Implements state-tracking via an INGESTION_LOG audit table. The framework checks if a specific source file has already been successfully processed (STATUS = 'SUCCESS'), safely skipping duplicates on subsequent runs.   Robust Observability & Auditing: Tracks end-to-end lineage and execution details—including unique Run IDs, file sizes, timestamps, row counts loaded, status tracking (SUCCESS/FAILED), and detailed error messages.   Cloud Data Warehousing & Storage: Integrates AWS S3 external stages securely with Snowflake via Storage Integrations and IAM roles, leveraging bulk COPY INTO commands for optimal performance.   Workflow Orchestration: Automated daily scheduling and management using Apache Airflow DAGs (metadata_driven_pipeline).   🛠️ Tech StackOrchestration: Apache Airflow   Data Warehouse: Snowflake (RAW, METADATA, BRONZE, SILVER, GOLD schemas)   Cloud Storage: Amazon S3 (External Stage)   Language: Python (Snowflake Connector, UUID, Datetime)   Infrastructure: Docker & Docker Compose   📂 Project StructurePlaintextMETA-DATA-DRIVEN-PIPELINE/
│
├── 📄 .env                  # Environment variables (Snowflake credentials)
├── 📄 .gitignore            # Git ignore file[cite: 1]
├── 🐳 docker-compose.yml    # Docker services configuration[cite: 1]
├── 📄 requirements.txt      # Python dependencies[cite: 1]
│
├── 📁 airflow/
│   ├── 📁 config/           # Airflow configurations[cite: 1]
│   ├── 📁 dags/
│   │   └── 🐍 metadata_pipeline_dag.py  # Airflow DAG for orchestration[cite: 1, 9]
│   ├── 📁 logs/             # Execution and task logs[cite: 1]
│   └── 📁 plugins/          # Custom Airflow plugins[cite: 1]
│
├── 📁 data/
│   ├── 📊 customers.csv     # Sample customer source data[cite: 1]
│   └── 📊 orders.csv        # Sample order source data[cite: 1]
│
└── 📁 ingestion/            # Core ingestion package[cite: 1, 10]
    ├── 📄 __init__.py
    ├── 🐍 config.py         # Config loader for Snowflake settings[cite: 2]
    ├── 🐍 file_discovery.py # S3 file listing and parsing[cite: 3]
    ├── 🐍 ingestion_log.py  # Audit logging and idempotency checks[cite: 4]
    ├── 🐍 loader.py         # Snowflake COPY INTO loading logic[cite: 5]
    ├── 🐍 metadata_reader.py# Active pipeline configurations reader[cite: 6]
    ├── 🐍 run_pipeline.py   # Orchestrator runner script[cite: 7, 10]
    └── 🐍 snowflake_connection.py # Snowflake connection manager[cite: 8]
🔄 Workflow ArchitectureMetadata Initialization: Active pipeline definitions (source paths, file patterns, target tables) are fetched from META_DATA_PIPELINE.METADATA.PIPELINE_CONFIG.   File Discovery: The framework queries the S3 external stage (S3_RAW_STAGE) dynamically to discover available files matching the pipeline configurations.   Idempotency Check: Each discovered file is checked against the INGESTION_LOG table. If it has already processed successfully, it is skipped; otherwise, processing proceeds.   Data Ingestion: Files are bulk loaded into designated Snowflake raw tables (RAW_CUSTOMERS, RAW_ORDERS) via optimized COPY INTO commands with transformations and error handling.   Audit Logging: Execution metrics (Run ID, status, row counts, start/end times, and error logs) are written to the INGESTION_LOG table for complete traceability.   ⚡ Execution OutputRunning the pipeline locally via Python demonstrates dynamic discovery, idempotency checks, and successful ingestion tracking:Plaintext(.venv) PS D:\Projects\META-DATA-DRIVEN-PIPELINE> python -m ingestion.run_pipeline
======================================================================
======================================================================
PIPELINE: customers_ingestion
======================================================================
PROCESSING: s3://meta-data-driven-pipeline/raw/customers/Book1.csv
Loading file: s3://meta-data-driven-pipeline/raw/customers/Book1.csv
Target table: META_DATA_PIPELINE.RAW.RAW_CUSTOMERS
SUCCESS: s3://meta-data-driven-pipeline/raw/customers/Book1.csv
ROWS LOADED: 10
SKIPPING: s3://meta-data-driven-pipeline/raw/customers/customers.csv
Reason: File was already successfully processed.

======================================================================
PIPELINE: orders_ingestion
======================================================================
SKIPPING: s3://meta-data-driven-pipeline/raw/orders/orders.csv
Reason: File was already successfully processed.
⚙️ Setup & Installation1. Clone the RepositoryBashgit clone https://github.com/your-username/META-DATA-DRIVEN-PIPELINE.git
cd META-DATA-DRIVEN-PIPELINE
2. Configure Environment VariablesCreate a .env file in the root directory[cite: 1] and populate your credentials:Code snippetSNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_USER=your_user
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_WAREHOUSE=your_warehouse
3. Install DependenciesBashpip install -r requirements.txt
4. Run the PipelineBashpython -m ingestion.run_pipeline

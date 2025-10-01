# E-Commerce-Pipeline

his project implements a scalable and secure end-to-end streaming data pipeline for e-commerce analytics on the Azure cloud platform. It ingests simulated order events into Azure Event Hubs, processes them in real time using Apache Spark Structured Streaming, and persists the data across three layers of a medallion architecture: Bronze (raw), Silver (cleaned and validated), and Gold (aggregated metrics).

The pipeline emphasizes security by avoiding hardcoded credentials, leveraging external configuration for storage paths, and relying on Azure-managed identities or secret-backed authentication for data access. Transformations include data cleansing, deduplication, enrichment, and time-windowed aggregation to support downstream analytics and business intelligence.

Built with Delta Lake as the storage format, the solution ensures ACID compliance, schema enforcement, and efficient incremental processing. The modular design allows each layer to evolve independently, supporting maintainability, testing, and future enhancements such as data quality monitoring or machine learning integration.

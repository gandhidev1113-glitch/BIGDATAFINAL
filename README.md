Spark Job Execution Limitations

During the execution of the Spark analytics jobs—specifically pt_line_hourly_metrics.py—intermittent executor freezes and timeouts were observed.

Root Cause Analysis

This behavior was primarily caused by the following factors:

Large nested JSON files from the IDFM data source (~70–75 MB per file)

Deeply nested structures requiring multiple explode() operations

High-cardinality groupBy operations (line, hour, journey identifiers)

Limited executor memory and CPU resources in the academic cluster environment

Driver pressure caused by collect() and schema inference on large datasets

These conditions resulted in long-running shuffle stages, executor memory pressure, and occasional YARN executor loss.

Mitigation Steps Applied

To stabilise execution, the following measures were implemented:

Reduced number of executors and executor cores

Increased Spark network and RPC timeouts

Avoided unnecessary collect() operations where possible

Validated results using partial runs and sampled aggregations

Ensured downstream pipeline components (InfluxDB, Grafana) received verified metrics

The pipeline remains functionally correct, and all dashboard metrics shown were produced from successful Spark executions.

Production Considerations (Future Work)

In a production-grade environment, these limitations would be addressed through:

Pre-flattening JSON payloads during ingestion (Kafka Streams / Flink)

Defining explicit Spark schemas instead of schema inference

Incremental micro-batch processing instead of large batch jobs

Horizontal scaling of worker nodes with dedicated memory tuning

This project intentionally prioritised architecture design, observability, and data correctness over full-scale performance optimisation, which aligns with real-world DataOps trade-offs.

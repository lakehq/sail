========================
Apache Spark Integration
========================

The :mod:`pysail.spark` module provides integration with Apache Spark.

.. currentmodule:: pysail.spark

.. autosummary::
    :toctree: api/

    SparkConnectServer

Distributed Diagnostics
-----------------------

Sail exposes distributed plans separately from Spark's ``DataFrame.explain``
compatibility API.  The diagnostics report is versioned and can be rendered as
text, JSON, or Graphviz without modifying the input DataFrame::

    from pysail.spark import diagnostics

    report = diagnostics.explain(df, format="json", analyze=False)
    print(report.text)
    for stage in report.stages:
        print(stage.id, stage.placement, stage.partitions)

.. currentmodule:: pysail.spark.diagnostics

.. autosummary::
    :toctree: api/

    explain
    ExplainReport
    DistributedStage
    DistributedEdge

def test_list_databases_returns_name(spark):
    """listDatabases returns database with 'name' field."""
    databases = spark.catalog.listDatabases()
    db_names = [db.name for db in databases]
    assert "default" in db_names

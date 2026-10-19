---
title: Catalog Management
rank: 3
---

# Catalog Management

Sail can work with several catalogs in one session, such as an in-memory catalog, an Iceberg REST catalog, AWS Glue, Hive Metastore, Unity Catalog, and OneLake.
Each session has a **catalog manager** that keeps track of these catalogs and resolves the names used in queries to objects in a specific catalog.

## Catalog Providers

A catalog is accessed through a **catalog provider**, which implements a common interface for databases, tables, views, and functions.
A catalog contains _databases_, and each database has a multi-level name called a _namespace_ (for example, `sales` or `sales.emea`).
A database contains objects such as tables and views.

The catalogs are defined by the `catalog.list` configuration option.
Each entry has a name, which is how queries refer to the catalog, and a type, which selects the provider implementation.
The same catalog can be registered under different names in different sessions.
The name `system` is reserved for the built-in system catalog.

Providers can be wrapped with a cache for databases, tables, and views.
The cache is either global, shared by all sessions that use the catalog, or local to a session.
The caching wrapper forwards every call it does not serve from the cache, so the rest of the catalog manager treats a cached provider like any other provider.

Each provider also defines a **default namespace**, which is the namespace used for unqualified object names when that catalog is not the current one.
For the memory catalog, this is the `initial_database` from its configuration.
The other providers use `default`.

## Session State

The catalog manager holds the following state for each session:

- the registered catalog providers, keyed by name;
- the **current catalog** (also called the default catalog);
- the **current database** (also called the default database), which is a namespace in the current catalog;
- the namespace used for global temporary views (`global_temp` by default).

Temporary views are kept by the catalog manager itself rather than by any catalog provider.

When a session starts, the current catalog is `catalog.default_catalog`.
If that option is not set and only one catalog is defined, that catalog is used. If several catalogs are defined, the option must be set.
The current database starts as `catalog.default_database`.

## Changing the Current Catalog and Database

The current catalog changes with `USE CATALOG`, `SET CATALOG`, or `spark.catalog.setCurrentCatalog()`, and the current database with `USE` or `spark.catalog.setCurrentDatabase()`.
These follow Spark semantics:

- Switching to a different catalog resets the current database to that catalog's default namespace.
  The database selected in the previous catalog does not carry over.
- Setting the current catalog to the catalog that is already current keeps the current database.
- Switching back to a catalog uses its default namespace, not the database that was selected the last time it was current.
- Switching to a catalog that does not exist returns an error and leaves the session state unchanged.
- Setting the current database checks that the database exists in the current catalog, so a missing database is reported immediately.
  The database can still be dropped externally later, in which case statements that use it report an error.

Since the session starts with `catalog.default_database` but switching back to a catalog uses the provider's default namespace, the two can differ if they are configured differently.
For the default configuration they are the same (`default`).

## Name Resolution

A table or view name has one or more parts, and the catalog manager resolves it to a catalog, a namespace, and an object name:

| Name                                           | Resolves to                                  |
| :--------------------------------------------- | :------------------------------------------- |
| `t`                                            | the current catalog and the current database |
| `c.db.t`, where `c` is a registered catalog    | catalog `c`, namespace `db`                  |
| `db.t`, where `db` is not a registered catalog | the current catalog, namespace `db`          |
| `a.b.t`, where `a` is not a registered catalog | the current catalog, namespace `a.b`         |

The first part is treated as a catalog name only if a catalog with that name is registered, so a namespace whose first level shares a name with a catalog must be qualified with its catalog.

Some statements name a catalog of their own, for example a procedure call such as `CALL c.system.ancestors_of('t')`.
A table argument of such a statement is resolved against catalog `c` instead of the current catalog.
An unqualified table name then uses the current database if `c` is the current catalog, and the default namespace of `c` otherwise.

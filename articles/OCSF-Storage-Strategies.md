# OCSF – Iceberg Storage Strategies
Paul Agbabian, April 2026; Updated May, June 2026

Contributions by Matthias Vallentin, Hunter Madison

## Overview
There are multiple ways that OCSF structured events can be stored for analysis in Parquet/Iceberg table format. The following list is not exhaustive but forms the most common approaches seen so far in the industry. 

1. Single table
2. By event source
3. By event category
4. By event class
5. By Observable and category
6. By Required only


This short document will discuss the pros and cons of each strategy along with general considerations that apply to each strategy.

## General Considerations

In its normative form, an OCSF class is a JSON formatted structure, where each instance of a JSON class is an event. We can think of OCSF classes as the finest granularity of meaningful tables: i.e. each class has instances, or logical rows, while the class attributes are logical columns of a table. The strategies considered are based on combinations of classes (or logical inner joins) into Iceberg table structures, using Parquet column-oriented file formats.

It should be noted that these tables will be sparsely populated in most cases with empty cells as not all attributes are populated for each OCSF class. In a relational database, which is row-oriented, a sparsely populated table can waste space due to row-oriented fixed storage extents. This is not an issue with Parquet encoded columnar storage as the missing attributes (cells) don’t use any storage as they usually do with relational database storage extents. Tables may have many columns, but they are storage efficient.

However, minimizing the number of identical columns across tables may provide better compressibility, especially for row group columns of lower volume event types. This means when pre-joining multiple classes into a table, factoring out the identical attributes that become columns, since any single event will only populate the columns of a single OCSF class.


### Parquet
Parquet is a column-oriented file format whose files hold column values contiguously, unlike row-oriented file formats like CSV or JSON, where each file stores columns across rows, where rows are stored contiguously. Due to the diversity of column types, and distribution of column values, this traditional approach does not compress as efficiently, as values need to be skipped across the columns of each row. For low cardinality columns, the compressibility is highest. Just as important, search performance tends to favor contiguously stored column values since most projections and predicates do not require all the columns in a row. Therefore I/O load is greatly reduced, and the time required to return a resultset is optimized.

Iceberg doesn't define its own file format, but rather supports different underlying physical file formats, including Parquet. Avro, another format supported by Iceberg, is an example of a row-oriented format.

### Partitioning

Partitioning of a table is a physical organization of the underlying files, based on combining similar rows based on column values. This helps speed queries by only having to read the files that are needed for a given query. For example, partitioning based on time, say by day, means queries that are outside of a time range only need to open files in the partitions that span the day range of the query. Multiple partitions are possible, for example, within a `day` partition, the rows (that is, the column files of rows) can be organized by `product` keeping the events from specific products together within each day.

Iceberg has hidden partitioning and schema evolution features that make table maintenance easier than with Hive or traditional RDBMS tables. Partitions can be made by values that are not explicit columns. For example, if a column is `time` with a millisecond resolution, Iceberg can partition by day without having to include a `day` column. And partitions can change over time, as with schema. Nevertheless, maintenance and onboarding of new sources are dynamic considerations for any of the approaches discussed.

### Sorting

Sorting of a table organizes files physically, much like partitioning a table. As the name implies, sorting based on ordering the values of one or more columns at write time helps with queries by predicting where values in and among files will reside. Additionally, range queries on the sorted columns can reduce the number of files that need to be opened, improving performance. For time series data, such as OCSF events, sorting by time and partitioning on a derived time measure is likely to improve general performance. Like with database tables in general, this is not specific to Iceberg or Parquet, but equally applies.

### Structured Columns

OCSF makes heavy use of objects, which are structured sets of scalar and other object attributes. These attributes can directly become Iceberg structured columns (also encoded in Parquet files). While this can reduce the number of table columns, the underlying Iceberg files hold the elements of arrays and structures as scalar columns on disk. Although Parquet can store structures and lists without flattening them, in Iceberg each structure or array is decomposed (shredded) into its separate scalar column names assigned unique Field IDs. Elements of structures or arrays (or arrays of structures) can be accessed in queries via dot notation (or bracket and dot notation in Spark SQL).

#### Example Table

| Attribute | Parquet Column Type | Comment |
| --------- | ----------- | ------- |
├ time            |   bigint  |   ← Base required, partition on *days(time)*
├ class_uid       |   int     |   ← Base required, partition on *class_uid*
├ category_uid    |   int     |   ← Base required, partition on *category_uid*
├ activity_id     |   int     |   ← Base required
├ severity_id     |   int     |   ← Base required
├ metadata        |   struct<version, product struct<name,vendor_name>, ...>    |   ← Base required, partition on *product.name*
├ observables     |   list<struct<name, type, value, ...>>  |   ← Base optional
├ device          |   struct<hostname, os struct<name,type>, ...>  |  ← [some class attribute], null for events that don't have one
├ actor           |   struct<user struct<name,uid>, process struct<...>, ...>   | ...
├ src_endpoint    |   struct<ip, port, ...> | ...
├ dst_endpoint    |   struct<ip, port, ...> | ...
├ file            |   struct<name, hashes list<struct<...>>, ...>   | ...
└── ... (the rest of the join across classes)

### Schema Evolution

Iceberg allows for schema column changes, partitioning and sorting changes at any time after tables are defined and populated. The changes take place and are realized for data written after the changes or until rewriting or compaction takes place. This mitigates some of the table mainteance that is necessary when either new products are onboarded or the OCSF schema itself evolves from version to version.

### Compaction

Over time, data can get stale, or the frequency of new data subsides such that the underlying files become too small, hindering compression, and forcing more file opens. For partitioning and sorting changes that would benefit historical data, rewriting the table files to compact them into more optimum sizes (usually around 125 MB) can be done. This is termed compaction and is part of the maintenance of the Iceberg lakehouse. As schema changes can be made at any time, for example adding class tables, new partitions, or adding columns to existing tables, the volume flow frequency that fragments the Parquet files will benefit from compaction periodically.

Compaction does not need to be applied to an entire table, but can be applied to specific partitions or date ranges.

### Limits

There are no theoretical limits in Iceberg as to how many columns or rows a given table can contain, and with schema evolution, alterations are metadata changes that don't need to touch existing data files. However in practice, too many columns in particular can surface operational issues depending on query engines, catalogs and field ID assignments. The latter is of particular concern with OCSF and Iceberg, due to the number of unique columns the array and structure decomposition entails.

Column IDs can run into internally reserved IDs for example. Query engines like AWS Athena can fail with more than 7500 columns, it has been reported. Statistics are collected for a limited number of columns, and configuring a high number of columns for statistics increases metadata size and impacts query planning. And while structured type columns can help keep top level columns in check, the depth and breadth of those structures can have practical limits, although they are quite high.

While these limits (e.g. 10,000 columns or 1000 structured sub-columns) might seem very high, and they are, one should realize that the combinatorial total of every OCSF attribute in all of its object, profile and class combinations can exceed one million scalar columns. Therefore, particular implementations of Iceberg with the various query engines in practice determine the actual limitations.

### OCSF Profiles

The structure of Iceberg tables is directly a function of the OCSF attributes of the class or classes modeled by the table. OCSF profiles can augment those attributes to broaden the applicability of a class, and those profile attributes are not bound by the categories of the classes. For example, adding `device` and `actor` attributes to network classes can be done by applying the `Host` profile to `network_activity`. This would be common for EDR events that can report network activity from a computer, including what user or process initiated the activity.

This means that the Iceberg tables will have additional columns from the profiles that may be applied to classes. These columns can be added a priori, ready for events that include profile attributes, or as part of the schema evolution of the tables, when onboarding event sources from products that emit them.

## Single Table Join-Union

One obvious approach that has been used at large scale is a single, partitioned table with structured Parquet columns. Logically, separate class tables would be joined (as with a LEFT OUTER JOIN), and the table would be a union of the events across all classes.

Given the number of OCSF dictionary attributes that can be combined into the number of OCSF objects, and optional profiles that add attributes across classes and objects, the distinct number of underlying Iceberg column Field IDs can be very high. Nevertheless, with proper partitioning this approach has its advantages. In practice, not every class is required for the particular event sources that are stored. They can be added as necessary with schema evolution metadata updates.

### Single Table Pros
-	A stable number of tables (1)
-	Reuse of table by multiple products
-	Minimize duplication of related columns within a table
-   No cross-table joins for all use cases including single source or product queries
-   Single target for all ETL targets
-   Leverage schema evolution for new class and partition maintenance

### Single Table Cons
-   Extremely high number of columns may hit metadata and query engine operational limits
-   The union of all events creates a massive dataset, impacting compaction
-   Compaction strategy is more complex
-   All use cases depend on a single table with cross-use-case data mingled in files and partitions

## By Event Source
Each event is emitted by an event source, usually associated with a single product with one or more features. A fully normalized or natively producing event source could span many different event classes. Each event class contains attributes which in turn can be of scalar or object type. Objects can have attributes that are scalar or object type, and so on.

Hence when creating tables by event source, there are two issues to be considered. First, do we know what classes the event source will emit? Second, how many event sources will be received by a given lakehouse implementation.

The assumption here is that each event source will have its own table, but each table will be different since each event source will likely emit different class information. In addition, the number of tables will expand based on the number of event sources, where new event source tables need to be built each time a new product is onboarded.

Lastly, there are over 50 OCSF event classes to date. Creating a single table structure for all products would be the cross-class union of all the attributes from each class, many of which are shared across classes and objects. If modeled as separate columns, advantages of a columnar table structure are reduced.

Nevertheless, several implementations have used this approach to be more product or event source oriented.

### Event Source Pros
-	Direct storage by source or product makes analyzing a single product or source easier
-	No cross-table joins for single source analysis across classes of events
-	1:1 ETL process from single source to single table: `metadata.product.name` or `metadata.source` directs the events to its destination

### Event Source Cons
-	New tables need to be created every time a new source is onboarded
-	Tables will all be different, with different but overlapping columns
-	Each table may have an unknown number of columns
-	As a source starts to emit new event classes, the table needs to be altered
-	Table management is more frequent

## By Event Category
OCSF event classes are organized by category, where each category contains classes that are related to each other by category domain. For example, operating system activities are found within the System Activity category, while network activities are found within the Network Activity category. (Host-based network activities are supported via the Host profile against network classes).

Further, classes in each category share common attributes, and all classes share common Base class attributes. (Note not all attributes are populated by every event source that uses the classes). Reuse of category-based tables across product event sources are by design. When new product event sources are onboarded, new tables are not generally required.

Because of the commonality across event categories, columns are not duplicated across the classes. Lastly, because the number of categories grows slowly across OCSF evolution, the number of tables will be relatively stable.

### Category Pros
-	A stable number of tables
-	Reuse of tables by multiple products
-	Minimize duplication of related columns within a table
-	Table holds all related classes in a domain
-	Reduce the number of joins within a domain’s category
-	ETL is straightforward as category_uid directs the events to its destination

### Category Cons
-	Searching for all events from a single product requires searching all tables if the product events span categories
-   Table will be larger than by source or by event class impacting compaction

## By Event Class
Each OCSF event is an instance of a single OCSF class. Events across different sources share the same classes, distinguished by their `metadata.source` or `metadata.product.name` attributes.

However, there are at this time of writing more than 50 event classes and new classes are developed more often than new categories of classes. Hence to cover a wide range of products that may share some but not all of the same classes, many tables need to be maintained and periodically added.

More joins are needed when analysis spans classes, as they map to separate tables. Given the commonality of many of the attributes across classes, columnar compression is not maximized. However, for targeted analysis, tables will be smaller (fewer rows).

### Class Pros
•	ETL is straightforward as `class_uid` directs the events to its destination
-	Reuse of tables by multiple products
-	Fewest number of columns per table
-	Best for targeted analysis across products emitting the same event types

### Class Cons
-	Searching for all events from a single product requires searching all tables if the product events span classes
-	Duplication of related columns across tables reducing compression
-	More joins within a single domain than with categories
-	Higher table growth velocity than with categories equating to more table maintenance

## By Observable and Category
This strategy is hybrid. It takes advantage of an OCSF schema feature called Observables.

Observables are an array of the most common attributes surfaced from any of the events that are emitted in a flat generic array. Each element of the array is a single Observable whose `value` attribute holds the element data. The `name` attribute provides the schema context of the attribute, for example `src_endpoint.ip` and `dst_endpoint.ip` are distinct attributes of the `ip` observable, and the `class_uid` and `activity_id` are present for behavior.

The original intent of the observables was for threat intelligence matching, where the total fidelity and context of the event was not required, just the observable values that might match Indicators of Compromise (IoCs).

There is another use case for Observables: a table constructed for observables will be common across every event class, and therefore every event across all products. This table can have foreign keys to dimension tables much like a STAR schema in OLAP. The Observables array is flattened into about 40 columns, along with the most important Base event attribute columns that identify and classify the events.

In practice, many analytics can run very efficiently directly against a single table across all products and classes, while drill-down and detailed investigation is performed by a minimum number of joins to dimension tables (i.e. joins are not required for detection analytics, only for drill-down).

Based on the above analysis of strategies, either the By Event Class strategy or the By Event Category strategy could be employed for the dimension tables. Given that for most use cases, By Event Category is more efficient than By Event Class, using categories as the dimension tables is suggested here.

### Observable and Category Pros
-	Many cybersecurity detections use cases can run against a single table across all product event sources without joins
-	Single product searches are possible against a single table for all products
-	Observables table has a fixed number of non-duplicative columns with many rows for optimum compression
-	Ideal for threat intelligence matching and retrospective matching against IOCs
-	Drill-down to details is efficient
-	Retains all the Pros of By Category 

### Observable and Category Cons
-	Requires an ETL that can surface the Observables, or product native sources that send Observables
-	Duplication of data: an additional table is required with duplicate values from the category dimension tables (since dimension tables hold full fidelity events)
-	ETL is less straightforward as the Observables array needs to be retrieved and parsed before events are stored (and the array removed from the events to reduce but not eliminate duplication in dimension tables).


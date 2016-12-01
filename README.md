Dex, the Index Bot
--------

DEPRECATED - This tool is recommended for MongoDB version <= 2.6

Dex is a MongoDB performance tuning tool that compares  queries to the
available indexes in the queried collection(s) and generates index suggestions
based on simple heuristics. Currently you must provide a connection URI for
your database.

Dex uses the URI you provide as a helpful way to determine when an index is
recommended. Dex does not take existing indexes into account when actually
constructing its ideal recommendation.

Currently, Dex only recommends complete indexes, not partial indexes. Dex
ignores partial indexes that may be used by the query in favor of a better
index, if one is not found. Dex recommends partially-ordered indexes according
to a rule of thumb:

Your index field order should first answer:

1. Equivalent value checks
2. Sort clauses
3. Range value checks ($in, $nin, $lt/gt, $lte/gte, etc.)

Note that your data cardinality may warrant a different order than the suggested
indexes.

Windows not supported.

Usage
--------

### Common
Run Dex on a log file and provide a URI (with auth credentials, if any) to the
corresponding database.

```
> dex -f my/mongod/data/path/mongodb.log mongodb://myUser:myPass@myHost:12345/myDb
```

Or, run Dex on a populated MongoDB system.profile collection. It is recommended
that you run db.setProfilingLevel(1), and wait until a representative set of
queries/operations have been run on the database. Then run db.setProfilingLevel(0)
to stop profiling. Then, run Dex:

```
> dex -p mongodb://myUser:myPass@myHost:12345/myDb
```

Note: Because Dex is chiefly concerned with un-indexed queries, Dex output should not
be affected by the additional data produced by profiling level 2. However, Dex may
take longer to run.

### Filter by db/collection
Dex supports filtering the analysis by specific collections and databases.
Note that when you intend to analyze multiple databases you must provide a
connection URI for the admin database.

```
> dex -f my/mongod/data/path/mongodb.log -n "myFirstDb.collectionOne" mongodb://myUser:myPass@myHost:12345/myFirstDb

> dex -p -n "*.collectionOne" mongodb://myUser:myPass@myHost:12345/admin

> dex -f my/mongod/data/path/mongodb.log -n "myFirstDb.*" -n "mySecondDb.*" mongodb://myUser:myPass@myHost:12345/admin
```

### Filter by query time (millis)
Dex also supports filtering the analysis by query execution time. Provide the
-s/--slowms argument to specity the minimum time in millis. Queries completing
in less than the indicated time will not be analyzed.

```
> dex -f my/mongod/data/path/mongodb.log -s 400

> dex -p -n "*.collectionOne" mongodb://myUser:myPass@myHost:12345/admin --slowms 1000
```

### Watch Mode
When you provide the -w/--watch argument, Dex does not process the full logfile
or any existing contents in the system.profile collection. Instead, Dex evaluates
entries as they are logged/profiled. Use a keyboard interrupt (Ctrl+C) to terminate
Dex when running in watch mode.

Use watch mode to obtain running information in real time.

Example:

```
> dex -w -f my/mongod/data/path/mongodb.log mongodb://myUser:myPass@myHost:12345/myDb
```

Note that Dex still caches its suggestions, so each unique recommendation will
only print once.

When using -w/--watch with -p/--profile to watch the system.profile collection,
you must currently filter your focus to one database by providing a namespace
argument of the form -n "[db_name].*"
For Example:

```
> dex -w -p -n "myDb.*" mongodb://myUser:myPass@myHost:12345/myDb
```

In addition, if profiling is not enabled, Dex will enable profile level 1 for
the duration of its operation.

### Other useful options

-t/--timeout - Logfile (-f) mode only. Useful to truncate a Dex operation after
a number of minutes. If your database is generating extraordinarily large logfiles,
you may only need to dex for 1-3 minutes to obtain usable information.

--nocheck - Don't check existing indexes in the database. This means Dex will
recommend indexes for all queries, even indexed ones.

### Help Contents

```
usage: dex [uri] (-f <logfile_path> | -p) [<options>]

Scans a provided MongoDB log file or profile collection and uses the provided URI
to compare queries found in the logfile or profile collection to the indexes available
in the database, recommending indexes for those queries which are not indexed.
Recommended for MongoDB version 2.2.0 or later.


Options:
  -h, --help            show this help message and exit
  -f LOGFILE_PATH, --file LOGFILE_PATH
                        path to a MongoDB log file. If provided, the file will
                        be searched for queries.
  -p, --profile         flag to examine the MongoDB system.profile collection.
                        If set, the profile collection will be searched for
                        queries. URI is required for profile mode.
  -w, --watch           instructs Dex to watch the system.profile or log
                        (depending on which -p/-f is specified) for entries,
                        rather than processing existing content. Upon keyboard
                        interrupt (Ctrl+C) watch terminates and the
                        accumulated output is provided. When using watch mode
                        and profile mode together, you must target a specific
                        database using -n "dbname.*"
  -n NAMESPACES, --namespace NAMESPACES
                        a MongoDB namespace (db.collection). Can be provided
                        multiple times. This option creates a filter, and
                        queries not in the provided namespace(s) will not be
                        analyzed. Format: -n ('db.collection' | '*' | 'db.*' |
                        'collection'). '*.*' and '*.collection' are redundant
                        but also supported. An asterisk is shorthand for 'all'
                        --actual regexes are not supported. Note that -n '*'
                        is equivalent to not providing a -n argument.
  -s SLOWMS, --slowms SLOWMS
                        minimum query execution time for analysis, in
                        milliseconds. Analogous to MongoDB's SLOW_MS value.
                        Queries that complete in fewer milliseconds than this
                        value will will not be analyzed. Default is 0.
  -t TIMEOUT, --timeout TIMEOUT
                        Maximum Dex time in minutes. Default is 0 (no
                        timeout).Applies to logfile (-f) mode only.
  --nocheck             if provided, Dex will recommend indexes without
                        checkingthe specified database to see if they exist.
                        This meansDex may recommend an index that's already
                        been created
  -v, --verbose         enables provision of additional output information.
```

Requirements
--------

Dex is designed to comprehend logs and profile collections for mongod 2.0.4 or
later.

Libraries:
* pyyaml
* pymongo
* dargparse

Installation
--------

```
> pip install dex
```

Testing
--------

To run Dex's unit test suite, you must bring up a mongodb server on 27017. Dex
will use create the dex_test db and drop it when the tests are complete.

```
> python -m dex.test.test
```

Output
--------

For each run, Dex provides:
* runStats - statistics for the parsed log or profile
 * runStats.linesRead - The number of entries (log or profile) sent to Dex.
 * runStats.linesAnalyzed - The number of entries from which Dex successfully
extracted queries and attempted recommendations.
 * runStats.linesWithRecommendations - The number of lines that prompted and could potentially benefit from an index recommendation.
 * runStats.dexTime - The time Dex was initiated.
 * runStats.logSource - Path to logfile processed. Null for -p/--profile mode.
 * runStats.timeRange - The range of times passed to Dex. Includes all lines read.
 * runStats.timedOut - True if the Dex operation times out per the -t/--timeout flag.
 * runStats.timeoutInMinutes - If timedOut is true, this contains the time.
Dex provides information and statistics for each unique query in the form of a. A
recommendation includes:
* results - A list of query reports including index recommendations.

#### Results Output to STDOUT

Dex returns an array of query reports as results. Each query report is for a unique
query as identified by 'queryMask'. Each report includes:

* queryMask - The query pattern, with values masked ($query for query component, $orderby for sort component)
* namespace - The MongoDB namespace in which to create the index,
in the form "db.collection"
* stats - specific query statistics aggregated from each query occurrence.
* stats.count - The total number of queries that occurred.
* stats.avgTimeMillis - The average time this query currently takes.
* stats.totalTimeMillis - The sum amount of time consumed by all of the queries that
match the queryMask.
* recommendation - A fully-formed recommendation object.
 * recommendation.index - The index recommended.
 * recommendation.namespace - The recommendation namespace.
 * recommendation.shellCommand - A helpful string for creating the index in
the MongoDB shell.

Sample:
```

```

#### Watch Mode Output to STDERR

Dex provides runtime output during watch (-w) mode. Every 30 seconds,
the full list of recommendations is printed with updated statistics.

### Questions?

Email support@mongolab.com

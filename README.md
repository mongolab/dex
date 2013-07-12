Dex, the Index Bot
--------

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

Usage
--------

### Common
Run Dex on a log file and provide a URI (with auth credentials, if any) to the
corresponding database.

```
> dex -f my/mongod/data/path/mongodb.log mongodb://myUser:myPass@myHost:12345/myDb
```

Or, run Dex on a populated MongoDB system.profile collection. It is recommended
that you run db.enableProfilingLevel(1), and wait until a representative set of
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
  -v, --verbose         enables provision of additional output information,
                        including Dex's query and index analysis structures.
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

### Default
Dex provides information and statistics for each unique recommendation. A
recommendation includes:
* index - The index recommended.
* queryCount - The total number of queries that will benefit from the
recommendation.
* namespace - The MongoDB namespace in which to create the index,
in the form "db.collection"
* totalTimeMillis - The sum amount of time consumed by all of the queries
that prompted the recommendation.
* avgTimeMillis - The average time each query currently takes.
* queries - An array of query patterns addressed by the recommendation,
with values masked (q for query component, s for sort component).

For each run, Dex also provides a brief set of run stats:
* linesPassed - The number of entries (log or profile) sent to Dex.
* linesProcessed - The number of entries from which Dex successfully
extracted queries.
* linesRecommended - The number of lines that prompted an index recommendation.
* timedOut - True if the Dex operation times out per the -t/--timeout flag.
* timeoutInMinutes - If timedOut is true, this contains the time.

#### Watch Mode Output to STDERR

Dex provides runtime output during watch (-w) mode. Every 30 seconds,
the full list of recommendations is printed with updated statistics.

Default Output Sample:
```
{
    'runStats': {
        'linesRecommended': 6482,
        'linesProcessed': 6482,
        'linesPassed': 6759
    },
    'results': [
        {
            'index': '{"classes": 1, "name": 1, "level": 1}',
            'totalTimeMillis': 441459,
            'namespace': 'mongoquest.adventurers',
            'queryCount': 2161,
            'avgTimeMillis': 204,
            'queries': [
                '{"q": {"classes": "<classes>", "name": "<name>", "level": "<level>"}}'
            ]
        },
        {
            'index': '{"name": 1, "classes": 1, "level": 1}',
            'totalTimeMillis': 441324,
            'namespace': 'mongoquest.adventurers',
            'queryCount': 2160,
            'avgTimeMillis': 204,
            'queries': [
                '{"q": {"name": "<name>"}, "s": {"classes": "<sort-order>", "level": "<sort-order>"}}'
            ]
        },
        {
            'index': '{"name": 1}',
            'totalTimeMillis': 410041,
            'namespace': 'mongoquest.adventurers',
            'queryCount': 2161,
            'avgTimeMillis': 189,
            'queries': [
                '{"q": {"name": "<name>"}}'
            ]
        }
    ]
}
```

#### Final Output to STDOUT

Dex returns an array of JSON documents containing each unique recommendation.

NOTE: Dex no longer provides runtime output in default mode (no -w)

#### Verbose Output
When -v/--verbose is specified, Dex provides additional information for
each recommendation, including:

* namespace - The MongoDB namespace in which to create the index,
in the form "db.collection"
* queryCount - The total number of queries that will benefit from the
recommendation.
* avgTimeMillis - The average time each query currently takes.
* totalTimeMillis - The sum amount of time consumed by all of the queries that
prompted the recommendation.
* recommendation - A fully-formed recommendation object.
 * recommendation.index - The index recommended.
 * recommendation.namespace - The recommendation namespace.
 * recommendation.shellCommand - A helpful string for creating the index in
the MongoDB shell.
* queries - An array of query patterns addressed by the recommendation,
with values masked (q for query component, s for sort component).
* queryDetails - An array of statistics for each unique query pattern (see queries)
 * queryMask - The query pattern, with values masked (q for query component,
 s for sort component)
 * queryCount - The total number of queries matching the query
 pattern.
 * avgTimeMillis - The average time each query with the pattern
takes.
 * totalTimeMillis - The sum amount of time consumed by all of
 the queries of that pattern.

Verbose Sample:
```
{
    'runStats': {
        'linesRecommended': 6482,
        'linesProcessed': 6482,
        'linesPassed': 6761
    },
    'results': [
        {
            'totalTimeMillis': 441459,
            'queries': [
                '{"q": {"classes": "<classes>", "name": "<name>", "level": "<level>"}}'
            ],
            'namespace': 'mongoquest.adventurers',
            'queryCount': 2161,
            'avgTimeMillis': 204,
            'recommendation': {
                'index': '{"classes": 1, "name": 1, "level": 1}',
                'namespace': 'mongoquest.adventurers',
                'shellCommand': 'db["adventurers"].ensureIndex({"classes": 1, "name": 1, "level": 1}, {"background": true})'
            },
            'queryDetails': [
                {
                    'avgTimeMillis': 204,
                    'queryCount': 2161,
                    'totalTimeMillis': 441459,
                    'queryMask': '{"q": {"classes": "<classes>", "name": "<name>", "level": "<level>"}}'
                }
            ]
        },
        {
            'totalTimeMillis': 441324,
            'queries': [
                '{"q": {"name": "<name>"}, "s": {"classes": "<sort-order>", "level": "<sort-order>"}}'
            ],
            'namespace': 'mongoquest.adventurers',
            'queryCount': 2160,
            'avgTimeMillis': 204,
            'recommendation': {
                'index': '{"name": 1, "classes": 1, "level": 1}',
                'namespace': 'mongoquest.adventurers',
                'shellCommand': 'db["adventurers"].ensureIndex({"name": 1, "classes": 1, "level": 1}, {"background": true})'
            },
            'queryDetails': [
                {
                    'avgTimeMillis': 204,
                    'queryCount': 2160,
                    'totalTimeMillis': 441324,
                    'queryMask': '{"q": {"name": "<name>"}, "s": {"classes": "<sort-order>", "level": "<sort-order>"}}'
                }
            ]
        },
        {
            'totalTimeMillis': 410041,
            'queries': [
                '{"q": {"name": "<name>"}}'
            ],
            'namespace': 'mongoquest.adventurers',
            'queryCount': 2161,
            'avgTimeMillis': 189,
            'recommendation': {
                'index': '{"name": 1}',
                'namespace': 'mongoquest.adventurers',
                'shellCommand': 'db["adventurers"].ensureIndex({"name": 1}, {"background": true})'
            },
            'queryDetails': [
                {
                    'avgTimeMillis': 189,
                    'queryCount': 2161,
                    'totalTimeMillis': 410041,
                    'queryMask': '{"q": {"name": "<name>"}}'
                }
            ]
        }
    ]
}
```


### Questions?

Email support@mongolab.com
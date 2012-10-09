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

### Filtered
Dex also supports filtering this analysis by specific collections and databases.
Note that when you intend to analyze multiple databases you must provide a
connection URI for the admin database.

```
> dex -f my/mongod/data/path/mongodb.log -n "myFirstDb.collectionOne" mongodb://myUser:myPass@myHost:12345/myFirstDb

> dex -p -n "*.collectionOne" mongodb://myUser:myPass@myHost:12345/admin

> dex -f my/mongod/data/path/mongodb.log -n "myFirstDb.*" -n "mySecondDb.*" mongodb://myUser:myPass@myHost:12345/admin
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

### Help Contents

```
Usage: dex [<options>] uri

Scans a provided MongoDB log file or profile collection and uses the provided
URI to compare queries found in the logfile or profile collection to the indexes
available in the database, recommending indexes for those queries which are not
indexed. Recommended for MongoDB version 2.2.0 or later.

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

Use these statistics to prioritize your indexing efforts.

For each run, Dex also provides a brief set of run stats:
* linesPassed - The number of entries (log or profile) sent to Dex.
* linesProcessed - The number of entries from which Dex successfully
extracted queries.
* linesRecommended - The number of lines that prompted an index recommendation.

#### Watch Mode Output to STDERR

Dex provides runtime output during watch (-w) mode. Every 30 seconds,
the full list of recommendations is printed with updated statistics.

Default Output Sample:
```
{
    "runStats": {
        "linesRecommended": 27677,
        "linesProcessed": 27677,
        "linesPassed": 28954
    },
    "results": [
        ...
        {
            "index": "{'classes': 1, 'name': 1, 'level': 1}",
            "queryCount": 13837,
            "namespace": "mongoquest.adventurers",
            "totalTimeMillis": 3315793,
            "avgTimeMillis": 239
        },
        ...
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
* queriesCovered - An array of unique query patterns addressed by the
recommendation, and statistics for each.
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
    "runStats": {
        "linesRecommended": 27677,
        "linesProcessed": 27677,
        "linesPassed": 28954
    },
    "results": [
        ...
        {
            "queriesCovered": [
                {
                    "queryMask": "{'q': {'classes': '<classes>' , 'name': '<name>' , 'level': '<level>' }}",
                    "avgTimeMillis": 210,
                    "queryCount": 6919,
                    "totalTimeMillis": 1454932
                },
                {
                    "queryMask": "{'q': {'classes': '<classes>', 'name': '<name>' }, 's': {'level': <sort-order>}}",
                    "avgTimeMillis": 268,
                    "queryCount": 6918,
                    "totalTimeMillis": 1860861
                }
            ],
            "totalTimeMillis": 3315793,
            "namespace": "mongoquest.adventurers",
            "queryCount": 13837,
            "avgTimeMillis": 239,
            "recommendation": {
                "index": "{'classes': 1, 'name': 1, 'level': 1}",
                "namespace": "mongoquest.adventurers",
                "shellCommand": "db['adventurers'].ensureIndex({'classes': 1, 'name': 1, 'level': 1}, {'background': true})"
            }
        },
        ...
    ]
}
```


### Questions?

Email support@mongolab.com
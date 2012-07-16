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
1) Equivalent value checks
2) Sort clauses
3) Range value checks ($in, $nin, $lt/gt, $lte/gte, etc.)

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

### Help Contents

```
Usage: dex [<options>] uri

Scans a provided MongoDB log file and uses the provided URI to compare queries
found in the logfile or profile collection to the indexes available in the
database, recommending indexes for those queries which are not indexed.
Recommended for MongoDB version 2.0.4 or later.

Options:
  -h, --help            show this help message and exit
  -f LOGFILE_PATH, --file LOGFILE_PATH
                        path to a MongoDB log file. If provided, the file will
                        be searched for queries. Currenly not accepted with -p.
  -p, --profile         flag to examine the MongoDB system.profile collection.
                        If set, the profile collection will be searched for
                        queries. Currently not accepted with -f.
  -n NAMESPACES, --namespace NAMESPACES
                        a MongoDB namespace (db.collection). Can be provided
                        multiple times. This option creates a filter, and
                        queries not in the provided namespace(s) will not be
                        analyzed. Format: -n ('db.collection' | '*' | 'db.*' |
                        'collection'). '*.*' and '*.collection' are redundant
                        but also supported. An asterisk is shorthand for 'all'
                        --actual regexes are not supported. Note that -n '*'
                        is equivalent to not providing a -n argument.
  -v, --verbose         enables provision of additional output information.
```

Requirements
--------

Dex is designed to comprehend logs and profile collections for mongod 2.0.4 or later.

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
By default, Dex outputs each unique recommendation. A recommendation includes a
db name, index command, and the fields from the query that prompted the
recommendation. Dex concludes a run with a list of run statistics, including:
* Total lines read - Total number of lines in the input file
* Understood query lines - Number of lines successfully parsed by the LogParser. 
  For the average case, this line is expected to be somewhat low compared to
  Total Lines Read.
* Unique recommendations - Number of unique recommendations found for all
  understood query lines
* Lines impacted by recommendations - Total number of understood lines that
  generated recommendations.

#### Runtime Output to STDERR

Dex provides runtime output as it processes. In default mode, Dex outputs each
unique recommendation it generates, and concludes with run statistics. Each
recommendation includes the following fields:

* namespace - The db.collection name.
* index - A json string describing the recommended index.
* shellCommand - A helpful cut-and-paste command to create the index from
  the MongoDB shell.

Sample: 
```
...
{
    "index": "{'simpleIndexedField': 1, 'simpleUnindexedFieldThree': 1}", 
    "namespace": "dex_test.test_collection" 
    "shellCommand": "db.test_collection.ensureIndex({'simpleIndexedField': 1, 'simpleUnindexedFieldThree': 1}, {'background': true})"
}
...
Total lines read: 7
Understood query lines: 7
Unique recommendations: 5
Lines impacted by recommendations: 5
```

#### Final Output to STDOUT

Dex returns a JSON document containing runtime statistics and each unique
recommendation.

Sample:
```
{
    "linesPassed": 7, 
    "linesRecommended": 5, 
    "results": [
        {
            "index-json": "{'simpleUnindexedField': 1}", 
            "shell-command": "db.test_collection.ensureIndex({'simpleUnindexedField': 1}, {'background': true})", 
            "namespace": "dex_test.test_collection" 
        }, 
       ...
    ], 
    "uniqueRecommendations": 5, 
    "linesProcessed": 7
}
```

### Verbose
When -v/--verbose is specified, Dex outputs the full query report for each
unique recommendation, including:

* namespace - The db.collection name.
* rawFields - Raw fields extracted from the query
* queryAnalysis - Provides basic information about the query, such as
  fieldCount), as well as the analyzed type of each field. 'suppported' is
  False when the query contains an UNSUPPORTED field.
* indexAnalysis - Shows which indexes cover the query (either in full
  or partially), and whether or not the analysis shows room for improvement.
  For each index, the following information is available:

  idealOrder - Is the index sorted according to Dex's internal heuristic.

  queryFieldsCovered - Number of query fields the index services.

  coverage - Indexes with a coverage of 'none' are not output. 'partial'
coverage indicates that (0 < query-fields-covered < total-query-fields).
'full' coverage indicates that (query-fields-covered == total-query-fields).

  supported - Indicates that the index is not of an unsupported type (such as 2d)

  index - The mongodb-provided document describing the index.

* parsed - The raw query as read by Dex's LogParser.
* recommendation - The recommendation itself (as described in Default Output
  above)

Sample:

```
...
{
    "indexAnalysis": {
        "needsRecommendation": true, 
        "fullIndexes": [], 
        "partialIndexes": [
            {
                "index": {
                    "key": [
                        [
                            "simpleIndexedField", 
                            1
                        ]
                    ], 
                    "v": 1
                }, 
                "supported": true, 
                "coverage": "partial", 
                "idealOrder": true, 
                "queryFieldsCovered": 1
            }
        ]
    }, 
    "parsed": {
        "ns": "dex_test.test_collection", 
        "query": {
            "simpleIndexedField": "value"
        }, 
        "orderby": {
            "simpleUnindexedFieldThree": -1
        }, 
        "findandmodify": "test_collection", 
        "update": {
            "$set": {
                "something": "somethingelse"
            }
        }
    }, 
    "recommendation": {
        "index": "{'simpleIndexedField': 1, 'simpleUnindexedFieldThree': 1}", 
        "namespace": "dex_test.test_collection",
        "shellCommand": "db.test_collection.ensureIndex({'simpleIndexedField': 1, 'simpleUnindexedFieldThree': 1}, {'background': true})"
    }, 
    "namespace": "dex_test.test_collection",
    "queryAnalysis": {
        "fieldCount": 2, 
        "supported": true, 
        "analyzedFields": [
            {
                "fieldName": "simpleIndexedField", 
                "fieldType": "EQUIV"
            }, 
            {
                "fieldName": "simpleUnindexedFieldThree", 
                "fieldType": "SORT", 
                "seq": 0
            }
        ]
    }
}
...
Total lines read: 7
Understood query lines: 7
Unique recommendations: 5
Lines impacted by recommendations: 5

```

### Questions?

Email support@mongolab.com
# dexResult
The output of Dex
```
{
  "runStats": {
    "linesRead": <int>,
    "linesAnalyzed": <int>,
    "linesWithRecommendations": <int>,
    "dexTime": <datetime>,
    ["timedOut": <boolean>,]
    ["timeoutInMinutes": <int>,] (present if timedOut)
    "logSource": <string>,
    "timeRange": {
        "start": <datetime>,
        "end": <datetime>
      },
    "unparsableLineInfo": {
        "unparsableLines": <int>,
        "unparsableLinesWithoutTime": <int>,
        "unparsableLinesWithTime": <int>,
        "unparsedTimeMillis": <int>
      }
    },
  "results": [<queryReport>,...]
}
```

##dexResult.queryReport
A report on a query uniquely identified by queryMask.

```
{
    "queryMask": <queryOccurrence.queryMask>,
    "recommendation": <queryOccurrence.recommendation>,
    "namespace": <queryReport.namespace>,
    "lastSeenDate": <datetime>,
    "stats": {
       "count": <int>,
       "totalTimeMillis": <int>,
       "avgTimeMillis": <int>,
       "avgNumReturned": <int>,
       "scanAndOrder": <boolean>,
       "avgNumScanned": <int>,
       "supported" : <boolean>
    }
}
```

#queryOccurrence
Primary internal data structure for Dex. Represents Dex"s assessment of a single occurrence of a single query.
```
{
    "queryMask": <string>
    "recommendation": <recommendation>,
    "indexAnalysis": <indexAnalysis>,
    "namespace":  <string>,
    "supported": <boolean>,
    "parsed": <parsed>,
    "queryAnalysis": <queryAnalysis>
}
```

##queryOccurrence.parsed
Initial data structure extracted from a parsed log line or profile entry.

```
{
    "queryMask": <string>,
    "ns": <string>,
    "supported": <boolean>,
    "query": <json>,
    "orderby": <json>,
    "stats": {
        "millis": <int>,
        "nreturned": <int>,
        "reslen": <int>,
        "scanAndOrder": <boolean>,
        "r": <int>,
        "ntoreturn": <int>,
        "keyUpdates": <int>,
        "ntoskip": <int>,
        "nscanned": <int>,
        ...
    }
}
```

##queryOccurrence.queryAnalysis
An extraction of field information from the queryReport.query and queryReport.orderby

```
{
    "supported": true,
    "analyzedFields": [<fieldAnalysis>,...],
    "fieldCount": <int>
}
```

###queryOccurrence.queryAnalysis.fieldAnalysis
A description of the field, specifically, its type.

```
{
    "fieldName": <string>,
    "fieldType": <string>("EQUIV"|"SORT"|"RANGE"),
    "seq": <int>
}
```

##queryOccurrence.indexAnalysis
A comparison of the available indexes in a collection to the needs of the query

```
{
    "indexStatus": <string ("full" | "partial" | "none")>
    "fullIndexes": [<indexReport>],
    "partialIndexes": [<indexReport>]
}
```

###queryOccurrence.indexAnalysis.indexReport
An evaluation of each index"s ability to cover the query

```
{
    "index": <json> 
    "supported": <boolean>, 
    "queryFieldsCovered": <int>, 
    "coverage": <string ("full" | "partial" | "none")>,
    "idealOrder": <boolean>
}
```

##queryOccurrence.recommendation
The ultimate recommendation for the query

```
{
    "index": <string>
}
```

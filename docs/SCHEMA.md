#queryOccurrence
Primary structure for Dex. Represents Dex's assessment of a single occurrence of a single query.


```
{
    "queryMask": <string>
    "recommendations": <recommendation>,
    "indexAnalysis": <indexAnalysis>,
    "namespace":  <string>,
    "details": <details>,
    "queryAnalysis": <queryAnalysis>
}
```

##queryOccurrence.details
Initial data structure extracted from a parsed log line or profile entry.

```
{
    "mask": <string>,
    "ns": <string>,
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
    "needsRecommendation": <boolean>,
    "fullIndexes": [<indexReport>],
    "partialIndexes": [<indexReport>]
}
```

###queryOccurrence.indexAnalysis.indexReport
An evaluation of each index's ability to cover the query

```
{
    "index": <json> 
    "supported": <boolean>, 
    "queryFieldsCovered": <int>, 
    "coverage": <string ("full" | "partial"), 
    "idealOrder": <boolean>
}
```

##queryOccurrence.recommendation
The ultimate recommendation for the query

```
{
    "index": <string>,
}
```

#queryReport
A union of multiple queryOccurrences that drops line-specific information and aggregates fully to the query level. In the future, this will aggregate multiple distinct queries, which is why queryMask and details are arrays. (initially they do not need to be)

```
{
    "queryMask": <queryReport.queryMask>,
    "recommendations": <queryReport.recommendation>,
    "namespace": <queryReport.namespace>,
    "details": <aggregatedDetails>
}
```

##queryReport.aggregatedDetails
Aggregated details across multiple queries, drawn from aggregating queryReport.parsed

```
{
    "query": <string>,
    "orderby": <string>,
    "stats": {
    	"count": <int>,
    	"totalTimeMillis": <int>,
        "avgTimeMillis": <int>,
        "avgNumReturned": <int>,
        "scanAndOrder": <boolean>,
        "avgNumScanned": <int>
    }
}
```

# DexResult
```
{
  "runStats": {
    "linesProcessed": <int>,
    "linesPassed": <int>,
    "linesRecommended": <int>
    },
  "results": [<queryReport>,...]
}
```

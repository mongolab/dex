Future

- Aggregation Framework, Geospatial, Map/Reduce
- min/max/average nscanned and nreturned
- scanAndOrder true/false
- Test revision
- Setwise optimization (group non-unique queries into fewer index recommendations)

### Version 0.6 2013-7-19

- Renovation of data model. See schema.md
- -v/--verbose currently nonfunctional

### Version 0.5.5 2013-5-09

- Better regexes for syslog and other log handlers which prepend their own info
- Added -s/--slowms
- Added -t/--timeout
Logfile mode only
- Added --nocheck

### 0.5.1 2012-11-05

- Expanded use of OrderedDicts throughout Dex and added an OrderedDict YAML
parser for log lines. This corrects multiple issues with the
recommendation of indexes for compound sort fields.

### 0.5.0 2012-10-09

- Weighted Recommendations:  
For each recommendation, Dex tallies the number of affected queries, the total time consumed by those queries, and the average time for a single query. Note that Dex keeps subtotal statistics for each unique query pattern that prompted a given recommendation. Subtotals are available in –verbose/-v mode only.
- Output changes:  
We’ve modified Dex’s output for the purposes of readability and convenience, in the following ways:
	- By default (i.e., not in –watch/-w mode), Dex no longer provides runtime output. Dex reads the entire profile collection or log file and then outputs one set of full results.
	- In –watch/-w mode, Dex still provides runtime output, periodically printing all recommendations with up-to-date statistics.
	- The shell command suggestion is removed from default output in favor of concise, weighted index recommendations. The shell command is still available in –verbose/-v output, but is no longer included by default.
- Support for MongoDB 2.2 log files:  
While Dex has supported MongoDB 2.2 in –profile/-p mode, updates to Dex’s log file regexes now support recent updates to the MongoDB log file line format.

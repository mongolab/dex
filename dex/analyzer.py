__author__ = 'eric'

from utils import pretty_json
import sys
import pymongo
try:
    from collections import OrderedDict
except ImportError:
    from ordereddict import OrderedDict

################################################################################
# Constants
#    query operator groupings and flag values
################################################################################

RANGE_QUERY_OPERATORS = ['$ne', '$gt', '$lt',
                         '$gte', '$lte', '$in',
                         '$nin', '$all', '$not']

#The following field is provided for reference and possible future use:
UNSUPPORTED_QUERY_OPERATORS = ['$mod', '$exists', '$size',
                               '$type', '$elemMatch', '$where']

COMPOSITE_QUERY_OPERATORS = ['$or', '$nor', '$and']
RANGE_TYPE = 'RANGE'
EQUIV_TYPE = 'EQUIV'
UNSUPPORTED_TYPE = 'UNSUPPORTED'
SORT_TYPE = 'SORT'
BACKGROUND_FLAG = 'true'


################################################################################
# QueryAnalyzer
#   Maintains an internal cache of indexes to analyze queries against. Connects
#   to databases to populate cache.
################################################################################
class QueryAnalyzer:
    def __init__(self, check_indexes):
        self._internal_map = {}
        self._check_indexes = check_indexes

    ############################################################################
    def _generate_query_report(self, db_uri, parsed_query, db_name, collection_name):
        """Generates a comprehensive report on the raw query"""
        index_analysis = None
        recommendation = None
        namespace = parsed_query['ns']

        index_cache_entry = self._ensure_index_cache(db_uri,
                                                     db_name,
                                                     collection_name)
        query_analysis = self._generate_query_analysis(parsed_query,
                                                       db_name,
                                                       collection_name)
        if ((query_analysis['analyzedFields'] != []) and
                query_analysis['supported']):
            index_analysis = self._generate_index_analysis(query_analysis,
                                                           index_cache_entry['indexes'])
            if index_analysis['needsRecommendation']:
                recommendation = self._generate_recommendation(query_analysis,
                                                               db_name,
                                                               collection_name)

        # QUERY REPORT
        return OrderedDict({
            'parsed': parsed_query,
            'namespace': namespace,
            'queryAnalysis': query_analysis,
            'indexAnalysis': index_analysis,
            'recommendation': recommendation
        })

    ############################################################################
    def _ensure_index_cache(self, db_uri, db_name, collection_name):
        """Adds a collections index entries to the cache if not present"""
        if not self._check_indexes or db_uri is None:
            return {'indexes': None}
        if db_name not in self.get_cache():
            self._internal_map[db_name] = {}
        if collection_name not in self._internal_map[db_name]:
            indexes = []
            try:
                connection = pymongo.MongoClient(db_uri,
                                                 document_class=OrderedDict)
                db = connection[db_name]
                indexes = db[collection_name].index_information()
            except:
                warning = 'Warning: unable to connect to ' + db_uri + "\n"
                sys.stderr.write(warning)
                raise
            else:
                internal_map_entry = {'indexes': indexes}
                self.get_cache()[db_name][collection_name] = internal_map_entry
        return self.get_cache()[db_name][collection_name]

    ############################################################################
    def _generate_query_analysis(self, parsed_query, db_name, collection_name):
        """Translates a raw query object into a Dex query analysis"""

        analyzed_fields = []
        field_count = 0
        supported = True
        sort_fields = []
        query_mask = None

        #if 'orderby' in parsed_query:
        sort_component = parsed_query['orderby'] if 'orderby' in parsed_query else []
        sort_seq = 0
        for key in sort_component:
            sort_field = {'fieldName': key,
                          'fieldType': SORT_TYPE,
                          'seq': sort_seq}
            sort_fields.append(key)
            analyzed_fields.append(sort_field)
            field_count += 1
            sort_seq += 1

        query_component = parsed_query['query']
        for key in query_component:
            if key not in sort_fields:
                field_type = UNSUPPORTED_TYPE
                if ((key not in UNSUPPORTED_QUERY_OPERATORS) and
                        (key not in COMPOSITE_QUERY_OPERATORS)):
                    try:
                        if query_component[key] == {}:
                            raise
                        nested_field_list = query_component[key].keys()
                    except:
                        field_type = EQUIV_TYPE
                    else:
                        for nested_field in nested_field_list:
                            if ((nested_field in RANGE_QUERY_OPERATORS) and
                                    (nested_field not in UNSUPPORTED_QUERY_OPERATORS)):
                                field_type = RANGE_TYPE
                            else:
                                supported = False
                                field_type = UNSUPPORTED_TYPE
                                break

                if field_type is UNSUPPORTED_TYPE:
                    supported = False

                analyzed_field = {'fieldName': key,
                                  'fieldType': field_type}
                analyzed_fields.append(analyzed_field)
                field_count += 1

        query_mask = parsed_query['mask']

        # QUERY ANALYSIS
        return OrderedDict({
            'analyzedFields': analyzed_fields,
            'fieldCount': field_count,
            'supported': supported,
            'queryMask': query_mask
        })

    ############################################################################
    def _generate_index_analysis(self, query_analysis, indexes):
        """Compares a query signature to the index cache to identify complete
            and partial indexes available to the query"""
        needs_recommendation = True
        full_indexes = []
        partial_indexes = []

        if indexes is not None:
            for index_key in indexes.keys():
                index = indexes[index_key]
                index_report = self._generate_index_report(index,
                                                           query_analysis)
                if index_report['supported'] is True:
                    if index_report['coverage'] == 'full':
                        full_indexes.append(index_report)
                        if index_report['idealOrder']:
                            needs_recommendation = False
                    elif index_report['coverage'] == 'partial':
                        partial_indexes.append(index_report)

        # INDEX ANALYSIS
        return OrderedDict({
            'fullIndexes': full_indexes,
            'partialIndexes': partial_indexes,
            'needsRecommendation': needs_recommendation
        })

    ############################################################################
    def _generate_index_report(self, index, query_analysis):
        """Analyzes an existing index against the results of query analysis"""

        all_fields = []
        equiv_fields = []
        sort_fields = []
        range_fields = []

        for query_field in query_analysis['analyzedFields']:
            all_fields.append(query_field['fieldName'])
            if query_field['fieldType'] is EQUIV_TYPE:
                equiv_fields.append(query_field['fieldName'])
            elif query_field['fieldType'] is SORT_TYPE:
                sort_fields.append(query_field['fieldName'])
            elif query_field['fieldType'] is RANGE_TYPE:
                range_fields.append(query_field['fieldName'])

        max_equiv_seq = len(equiv_fields)
        max_sort_seq = max_equiv_seq + len(sort_fields)
        max_range_seq = max_sort_seq + len(range_fields)

        coverage = 'none'
        query_fields_covered = 0
        query_field_count = query_analysis['fieldCount']
        supported = True
        ideal_order = True
        print pretty_json(index)
        for index_field in index['key']:
            field_name = index_field[0]

            if index_field[1] == '2d':
                supported = False
                break

            if field_name not in all_fields:
                break

            if query_fields_covered == 0:
                coverage = 'partial'

            if query_fields_covered < max_equiv_seq:
                if field_name not in equiv_fields:
                    ideal_order = False
            elif query_fields_covered < max_sort_seq:
                if field_name not in sort_fields:
                    ideal_order = False
            elif query_fields_covered < max_range_seq:
                if field_name not in range_fields:
                    ideal_order = False
            query_fields_covered += 1
        if query_fields_covered == query_field_count:
            coverage = 'full'

        # INDEX REPORT
        return OrderedDict({
            'coverage': coverage,
            'idealOrder': ideal_order,
            'queryFieldsCovered': query_fields_covered,
            'index': index,
            'supported': supported
        })

    ############################################################################
    def _generate_recommendation(self,
                                 query_analysis,
                                 db_name,
                                 collection_name):
        """Generates an ideal query recommendation"""
        index_json = '{'
        for query_field in query_analysis['analyzedFields']:
            if query_field['fieldType'] is EQUIV_TYPE:
                if len(index_json) is not 1:
                    index_json += ', '
                index_json += '"' + query_field['fieldName'] + '": 1'
        for query_field in query_analysis['analyzedFields']:
            if query_field['fieldType'] is SORT_TYPE:
                if len(index_json) is not 1:
                    index_json += ', '
                index_json += '"' + query_field['fieldName'] + '": 1'
        for query_field in query_analysis['analyzedFields']:
            if query_field['fieldType'] is RANGE_TYPE:
                if len(index_json) is not 1:
                    index_json += ', '
                index_json += '"' + query_field['fieldName'] + '": 1'
        index_json += '}'

        command_string = 'db["' + collection_name + '"].ensureIndex('
        command_string += index_json + ', '
        command_string += '{"background": ' + BACKGROUND_FLAG + '})'

        # RECOMMENDATION
        return OrderedDict({
            'namespace': db_name + '.' + collection_name,
            'index': index_json,
            'shellCommand': command_string
        })

    ############################################################################
    def get_cache(self):
        return self._internal_map

    ############################################################################
    def clear_cache(self):
        self._internal_map = {}


################################################################################
# ReportAggregation
#   Stores a merged set of query reports with running statistics
################################################################################
class ReportAggregation:
    def __init__(self):
        self._reports = []

    ############################################################################
    def add_report(self, report):
        """Adds a report to the report aggregation"""

        initial_query_detail = self._get_initial_query_detail(report)
        mask = initial_query_detail['queryMask']

        existing_report = self._get_existing_Report(mask, report)

        if existing_report is not None:
            if ((report['indexAnalysis'] is None) or
                    (report['indexAnalysis']['needsRecommendation'] is True)):
                self._merge_report(existing_report, report)
        else:
            self._reports.append(OrderedDict({
                'queryDetails': [initial_query_detail],
                'queriesCovered': [mask],
                'totalTimeMillis': int(report['parsed']['millis']),
                'avgTimeMillis': int(report['parsed']['millis']),
                'queryCount': 1,
                'recommendation': report['recommendation'],
                'namespace': report['namespace']
            }))

    ############################################################################
    def get_aggregated_reports_verbose(self):
        """Returns the whole aggregation"""
        return self._reports

    ############################################################################
    def get_aggregated_reports(self):
        """Returns a minimized version of the aggregation"""
        reports = []
        for report in self._reports:
            reports.append(self._get_abbreviated_report(report))
        return reports

    ############################################################################
    def _get_existing_Report(self, mask, report):
        """Returns the aggregated report that matches report"""
        for existing_report in self._reports:
            if existing_report['namespace'] == report['namespace']:
                if mask in existing_report['queriesCovered']:
                    return existing_report
        return None

    ############################################################################
    def _get_existing_query(self, report, queryAnalysis):
        """Returns the query in report that matches queryAnalysis"""
        for query in report['queryDetails']:
            if queryAnalysis['queryMask'] == query['queryMask']:
                return query
        return None

    ############################################################################
    def _merge_report(self, target, new):
        """Merges a new report into the target report"""
        query_millis = int(new['parsed']['millis'])
        current_sig = self._get_existing_query(target, new['queryAnalysis'])

        if current_sig is not None:
            current_sig['totalTimeMillis'] += query_millis
            current_sig['queryCount'] += 1
            current_sig['avgTimeMillis'] = current_sig['totalTimeMillis'] / current_sig['queryCount']

        else:
            initial_query_detail = self._get_initial_query_detail(new)
            target['queryDetails'].append(initial_query_detail)
            target['queriesCovered'].append(initial_query_detail['queryMask'])

        target['totalTimeMillis'] += query_millis
        target['queryCount'] += 1
        target['avgTimeMillis'] = target['totalTimeMillis'] / target['queryCount']

    ############################################################################
    def _get_initial_query_detail(self, report):
        """Returns a new query query document from the report"""
        initial_millis = int(report['parsed']['millis'])
        return OrderedDict({
            'queryMask': report['queryAnalysis']['queryMask'],
            'totalTimeMillis': initial_millis,
            'queryCount': 1,
            'avgTimeMillis': initial_millis
        })

    ############################################################################
    def _get_abbreviated_report(self, report):
        """Returns a minimum of fields from the report"""
        return OrderedDict({
            'namespace' : report['namespace'],
            'index' : report['recommendation']['index'] if report['recommendation'] is not None else None,
            'avgTimeMillis' : report['avgTimeMillis'],
            'queryCount': report['queryCount'],
            'totalTimeMillis': report['totalTimeMillis'],
            'queriesCovered': report['queriesCovered']
        })
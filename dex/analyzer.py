__author__ = 'eric'

from utils import pretty_json, validate_yaml
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
                               '$type', '$elemMatch', '$where', '$near',
                               '$within']

SUPPORTED_COMMANDS = ['count', 'findAndModify']

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
        self._index_cache_connection = None

    ############################################################################
    def generate_query_report(self, db_uri, parsed_query, db_name, collection_name):
        """Generates a comprehensive report on the raw query"""
        index_analysis = None
        recommendation = None
        namespace = parsed_query['ns']
        indexStatus = "unknown"

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
            indexStatus = index_analysis['indexStatus']
            if index_analysis['indexStatus'] != 'full':
                recommendation = self._generate_recommendation(query_analysis,
                                                               db_name,
                                                               collection_name)
                # a temporary fix to suppress faulty parsing of $regexes.
                # if the recommendation cannot be re-parsed into yaml, we assume
                # it is invalid.
                if not validate_yaml(recommendation['index']):
                    recommendation = None
                    query_analysis['supported'] = False


        # QUERY REPORT
        return OrderedDict({
            'queryMask': parsed_query['queryMask'],
            'indexStatus': indexStatus,
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
                if self._index_cache_connection is None:
                    self._index_cache_connection = pymongo.MongoClient(db_uri,
                                                                       document_class=OrderedDict,
                                                                       read_preference=pymongo.ReadPreference.PRIMARY_PREFERRED)

                db = self._index_cache_connection[db_name]
                indexes = db[collection_name].index_information()
            except:
                warning = 'Warning: unable to connect to ' + db_uri + "\n"
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

        if 'command' in parsed_query and parsed_query['command'] not in SUPPORTED_COMMANDS:
            supported = False
        else:
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

            query_component = parsed_query['query'] if 'query' in parsed_query else {}
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

        query_mask = parsed_query['queryMask']

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
        coverage = "unknown"

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

        if len(full_indexes) > 0:
            coverage = "full"
        elif (len(partial_indexes)) > 0:
            coverage = "partial"
        elif query_analysis['supported']:
            coverage = "none"

        # INDEX ANALYSIS
        return OrderedDict([('indexStatus', coverage),
                            ('fullIndexes', full_indexes),
                            ('partialIndexes', partial_indexes)])

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
        index_rec = '{'
        for query_field in query_analysis['analyzedFields']:
            if query_field['fieldType'] is EQUIV_TYPE:
                if len(index_rec) is not 1:
                    index_rec += ', '
                index_rec += '"' + query_field['fieldName'] + '": 1'
        for query_field in query_analysis['analyzedFields']:
            if query_field['fieldType'] is SORT_TYPE:
                if len(index_rec) is not 1:
                    index_rec += ', '
                index_rec += '"' + query_field['fieldName'] + '": 1'
        for query_field in query_analysis['analyzedFields']:
            if query_field['fieldType'] is RANGE_TYPE:
                if len(index_rec) is not 1:
                    index_rec += ', '
                index_rec += '"' + query_field['fieldName'] + '": 1'
        index_rec += '}'

        # RECOMMENDATION
        return OrderedDict([('index',index_rec),
                            ('shellCommand', self.generate_shell_command(collection_name, index_rec))])

    ############################################################################
    def generate_shell_command(self, collection_name, index_rec):
        command_string = 'db["' + collection_name + '"].ensureIndex('
        command_string += index_rec + ', '
        command_string += '{"background": ' + BACKGROUND_FLAG + '})'
        return command_string

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
    def add_query_occurrence(self, report):
        """Adds a report to the report aggregation"""

        initial_millis = int(report['parsed']['stats']['millis'])
        mask = report['queryMask']

        existing_report = self._get_existing_report(mask, report)

        if existing_report is not None:
            self._merge_report(existing_report, report)
        else:
            time = None
            if 'ts' in report['parsed']:
                time = report['parsed']['ts']
            self._reports.append(OrderedDict([
                ('namespace', report['namespace']),
                ('lastSeenDate', time),
                ('queryMask', mask),
                ('supported', report['queryAnalysis']['supported']),
                ('indexStatus', report['indexStatus']),
                ('recommendation', report['recommendation']),
                ('stats', OrderedDict([('count', 1),
                                       ('totalTimeMillis', initial_millis),
                                       ('avgTimeMillis', initial_millis)]))]))

    ############################################################################
    def get_reports(self):
        """Returns a minimized version of the aggregation"""
        return sorted(self._reports,
                      key=lambda x: x['stats']['totalTimeMillis'],
                      reverse=True)

    ############################################################################
    def _get_existing_report(self, mask, report):
        """Returns the aggregated report that matches report"""
        for existing_report in self._reports:
            if existing_report['namespace'] == report['namespace']:
                if mask == existing_report['queryMask']:
                    return existing_report
        return None

    ############################################################################
    def _merge_report(self, target, new):
        """Merges a new report into the target report"""
        time = None
        if 'ts' in new['parsed']:
            time = new['parsed']['ts']

        if (target.get('lastSeenDate', None) and
                time and
                    target['lastSeenDate'] < time):
            target['lastSeenDate'] = time

        query_millis = int(new['parsed']['stats']['millis'])
        target['stats']['totalTimeMillis'] += query_millis
        target['stats']['count'] += 1
        target['stats']['avgTimeMillis'] = target['stats']['totalTimeMillis'] / target['stats']['count']

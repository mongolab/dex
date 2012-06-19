################################################################################
#
# Copyright (c) 2012 ObjectLabs Corporation
#
# Permission is hereby granted, free of charge, to any person obtaining
# a copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to
# the following conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
# LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
# OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
# WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
################################################################################

import pymongo
import traceback
import sys
import json
import re
import yaml
from bson import json_util

################################################################################
# Configuration
################################################################################

IGNORE_DBS = ['local', 'admin']
IGNORE_COLLECTIONS = ['system.profile', 'system.users', 'system.indexes']
BACKGROUND_FLAG = 'true'

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

################################################################################
# Utilities
################################################################################
def pretty_json(obj):
    return json.dumps(obj, indent=4, default=json_util.default)

################################################################################
# Dex
#   Uses a QueryAnalyzer (with included LogParser) to analyze a MongoDB
#   query or logfile
################################################################################
class Dex:
    
    ############################################################################
    def __init__(self):
        self._query_analyzer = QueryAnalyzer()

    ############################################################################
    def analyze_query(self, db_uri, query, db_name, collection_name):
        """Analyzes a single query"""
        return self._query_analyzer._generate_query_report(db_uri,
                                                           query,
                                                           db_name,
                                                           collection_name)
    
    ############################################################################
    def analyze_logfile(self, db_uri, logfile_path, namespaces_list, verbose):
        """Analyzes queries from a given log file"""
        out = { 'results': [],
                'linesRecommended': 0,
                'uniqueRecommendations': 0,
                'linesProcessed': 0,                    
                'linesPassed': 0 }
        requested_namespaces = self._validate_namespaces(namespaces_list)
        log_parser = LogParser()
        recommendation_cache = []
        lines_passed = 0
        lines_ignored = 0
        # For each line in the logfile ... 
        with open(logfile_path) as file:
            for line in file:
                out['linesPassed'] += 1
                raw_query = log_parser.parse(line)  
                # For a comprehensible query ...
                if raw_query is not None:
                    out['linesProcessed'] += 1
                    namespace_tuple = self._tuplefy_namespace(raw_query['ns'])
                    # If the query is for a requested namespace ....
                    if self._namespace_requested(raw_query['ns'],
                                                 requested_namespaces):
                        db_name = namespace_tuple[0]
                        collection_name = namespace_tuple[1]
                        # Analyze the query, receiving its signature in return
                        try:
                            query_report = self.analyze_query(db_uri,
                                                              raw_query,
                                                              db_name,
                                                              collection_name)
                        except:
                            return 1
                        recommendation = query_report['recommendation']
                        if recommendation is not None:
                            out['linesRecommended'] += 1
                            # Cache the signature to suppress duplicate output
                            if recommendation not in recommendation_cache:
                                out['uniqueRecommendations'] += 1
                                recommendation_cache.append(recommendation)
                            
                                if verbose:
                                    sys.stderr.write(pretty_json(query_report) + '\n')
                                    out['results'].append(query_report)
                                else:
                                    sys.stderr.write(pretty_json(recommendation) + '\n')
                                    out['results'].append(recommendation)
        # Print summary statistics
        sys.stderr.write('Total lines read: %i\n' % (out['linesPassed']))
        sys.stderr.write('Understood query lines: %i\n' % (out['linesProcessed']))
        sys.stderr.write('Unique recommendations: %i\n' % (out['uniqueRecommendations']))
        sys.stderr.write('Lines impacted by recommendations: %i\n' % (out['linesRecommended']))
        sys.stdout.write(pretty_json(out))
        return 0      

    ############################################################################
    def _tuplefy_namespace(self, namespace):
        """Converts a mongodb namespace to a db, collection tuple"""
        namespace_split = namespace.split('.', 1)
        if len(namespace_split) is 1:
            # we treat a single element as a collection name.
            # this also properly tuplefies '*'
            namespace_tuple = ('*', namespace_split[0])            
        elif len(namespace_split) is 2:
            namespace_tuple = (namespace_split[0],namespace_split[1])
        else:
            return None                            
        return namespace_tuple
    
    ############################################################################
    # Need to add rejection of true regex attempts.
    def _validate_namespaces(self, input_namespaces):  
        """Converts a list of db namespaces to a list of namespace tuples,
            supporting basic commandline wildcards"""
        output_namespaces = []
        if input_namespaces is []:
            return output_namespaces
        elif '*' in input_namespaces:
            if len(input_namespaces) > 1:
                warning = 'Warning: Multiple namespaces are '
                warning += 'ignored when one namespace is "*"'
                print warning
            return output_namespaces
        else: 
            for namespace in input_namespaces:
                namespace_tuple = self._tuplefy_namespace(namespace)
                if namespace_tuple is None:
                    warning = 'Warning: Invalid namespace ' + namespace
                    warning += ' will be ignored'
                    print warning
                else:
                    if namespace_tuple not in output_namespaces:
                        output_namespaces.append(namespace_tuple)
                    else:
                        warning = 'Warning: Duplicate namespace ' + namespace
                        warning += ' will be ignored'
                        print warning                  
        return output_namespaces
                                   
    ############################################################################                             
    def _namespace_requested(self, namespace, requested_namespaces):
        """Checks whether the requested_namespaces contain the provided
            namespace"""
        namespace_tuple = self._tuplefy_namespace(namespace)
        if namespace is None:
            return False
        elif namespace_tuple[0] in IGNORE_DBS:
            return False
        elif namespace_tuple[1] in IGNORE_COLLECTIONS:
            return False
        elif len(requested_namespaces) is 0:
            return True
        else:
            return self._tuple_requested(namespace_tuple, requested_namespaces)
        return False

    ############################################################################
    def _tuple_requested(self, namespace_tuple, requested_namespaces):
        """Helper for _namespace_requested. Supports limited wildcards"""
        if namespace_tuple is None:
            return False
        for requested_namespace in requested_namespaces:   
            if (((requested_namespace[0] is '*') or
                 (requested_namespace[0] == namespace_tuple[0])) and
                ((requested_namespace[1] is '*') or
                 (requested_namespace == namespace_tuple[1]))):
                return True
        return False
                                    

################################################################################
# QueryAnalyzer
#   Maintains an internal cache of indexes to analyze queries against. Connects
#   to databases to populate cache.
################################################################################
class QueryAnalyzer:
    def __init__(self):        
        self._internal_map = {} 

    ############################################################################
    def _generate_query_report(self, db_uri, raw_query, db_name, collection_name):
        """Generates a comprehensive report on the raw query"""
        index_analysis = None
        recommendation = None
        parsed_query = self._mask_query(raw_query)
        namespace = raw_query['ns']
        
        index_cache_entry = self._ensure_index_cache(db_uri,
                                                     db_name,
                                                     collection_name)
        indexes = index_cache_entry['indexes']
        query_analysis = self._generate_query_analysis(parsed_query,
                                                       db_name,
                                                       collection_name)
        if query_analysis['supported']:
            index_analysis = self._generate_index_analysis(query_analysis,
                                                           indexes)
            if index_analysis['needsRecommendation']:
                recommendation = self._generate_recommendation(query_analysis,
                                                               db_name,
                                                               collection_name)

        # QUERY REPORT
        return {'parsed': parsed_query,
                'namespace': namespace,
                'queryAnalysis': query_analysis,
                'indexAnalysis': index_analysis,
                'recommendation': recommendation }  
    
    ############################################################################
    def _mask_query(self, parsed_query):
        return parsed_query
    #for key in query['query'].keys():
                    #   query['query'][key] = '<' + key '>'
    
    ############################################################################
    def _ensure_index_cache(self, db_uri, db_name, collection_name):
        """Adds a collections index entries to the cache if not present"""
        if db_name not in self.get_cache():
            self._internal_map[db_name] = {}
        if collection_name not in self._internal_map[db_name]:
            indexes = []
            try:
                connection = pymongo.Connection(db_uri)
                db = connection[db_name]
                indexes = db[collection_name].index_information()
            except:
                warning = 'Warning: unable to connect to ' + db_uri
                print warning
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

        if parsed_query.has_key('orderby'):
            sort_component = parsed_query['orderby']
            sort_seq = 0
            for key in sort_component.keys():
                sort_field = {'fieldName': key,
                              'fieldType': SORT_TYPE,
                              'seq': sort_seq}
                sort_fields.append(key)
                analyzed_fields.append(sort_field)
                field_count += 1
                sort_seq += 1

        query_component = parsed_query['query']
        for key in query_component.keys():
            if key not in sort_fields:
                field_type = UNSUPPORTED_TYPE
                if key not in COMPOSITE_QUERY_OPERATORS:
                    
                    try:
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
                else:
                    supported = False
                            
                analyzed_field = {'fieldName': key,
                                  'fieldType': field_type}
                analyzed_fields.append(analyzed_field)
                field_count += 1
                                
        # QUERY ANALYSIS
        return {'analyzedFields': analyzed_fields,
                'fieldCount': field_count,
                'supported': supported}
    
    ############################################################################
    def _generate_index_analysis(self, query_analysis, indexes):
        """Compares a query signature to the index cache to identify complete
            and partial indexes available to the query"""
        needs_recommendation = True
        full_indexes = []
        partial_indexes = []
        
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
        return {'fullIndexes': full_indexes,
                'partialIndexes': partial_indexes,
                'needsRecommendation': needs_recommendation }

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
        return {'coverage': coverage,
                'idealOrder': ideal_order,
                'queryFieldsCovered': query_fields_covered,
                'index': index,
                'supported': supported}
                        
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
                index_json += '\'' + query_field['fieldName'] + '\': 1'
        for query_field in query_analysis['analyzedFields']:
            if query_field['fieldType'] is SORT_TYPE:
                if len(index_json) is not 1:
                    index_json += ', '
                index_json += '\'' + query_field['fieldName'] + '\': 1'
        for query_field in query_analysis['analyzedFields']:
            if query_field['fieldType'] is RANGE_TYPE:
                if len(index_json) is not 1:
                    index_json += ', '
                index_json += '\'' + query_field['fieldName'] + '\': 1' 
        index_json += '}'
      
        command_string = 'db[\'' + collection_name + '\'].ensureIndex('
        command_string += index_json + ', '
        command_string += '{\'background\': ' + BACKGROUND_FLAG + '})'

        # RECOMMENDATION
        return {'namespace': db_name + '.' + collection_name,
                'index': index_json,
                'shellCommand': command_string }
                
    ############################################################################
    def get_cache(self):
        return self._internal_map
            
    ############################################################################
    def clear_cache(self):
        self._internal_map = {} 

################################################################################
# LogParser
#   Extracts queries from log lines using a list of QueryLineHandlers
################################################################################
class LogParser:
    def __init__(self):
        """Declares the QueryLineHandlers to use"""
        self._line_handlers = [self.StandardQueryHandler(), 
                               self.CmdQueryHandler(),
                               self.UpdateQueryHandler()]

    ############################################################################
    def parse(self, input):
        """Passes input to each QueryLineHandler in use"""
        for handler in self._line_handlers:
            try:
                query = handler.handle(input)
            except:
                query = None
            finally:
                if query is not None:
                    return query
        return None

    ############################################################################
    # Base QueryLineHandler class
    #   Knows how to yamlfy a logline query
    ############################################################################
    class QueryLineHandler:
        ########################################################################
        def _yamlfy_query(self, extracted_query):
            temp_query = yaml.load(extracted_query)
            if temp_query is not None:
                if temp_query.has_key('query'):
                    return temp_query
                else:
                    return { 'query': temp_query }
            else:
                return None
            
    ############################################################################
    # StandardQueryHandler
    #   QueryLineHandler implementation for general queries (incl. getmore)
    ############################################################################
    class StandardQueryHandler(QueryLineHandler):
        ########################################################################
        def __init__(self):
            self.name = 'Standard Query Log Line Handler'
            self._regex = '.{20}\[conn(?P<connection_id>\d+)\] '
            self._regex += '(?P<operation>\S+) (?P<ns>\S+\.\S+) query: '
            self._regex += '(?P<query>\{.*\}) (?P<options>(\S+:\S+ )*)'
            self._regex += '(?P<query_time>\d+)ms'
            self._rx = re.compile(self._regex)
    
        ########################################################################
        def handle(self, input):
            match = self._rx.match(input)
            if match is not None:
                query = self._yamlfy_query(match.group('query'))
                if query is not None:
                    query['ns'] =  match.group('ns')
                return query
            return None

    ############################################################################
    # CmdQueryHandler
    #   QueryLineHandler implementation for $cmd queries (count, findandmodify)
    ############################################################################
    class CmdQueryHandler(QueryLineHandler):
        ########################################################################
        def __init__(self):
            self.name = 'CMD Log Line Handler'
            self._regex = '.{20}\[conn(?P<connection_id>\d+)\] '
            self._regex += 'command (?P<db>\S+)\.\$cmd command: '
            self._regex += '(?P<query>\{.*\}) (?P<options>(\S+:\S+ )*)'
            self._regex += '(?P<query_time>\d+)ms'
            self._rx = re.compile(self._regex)
        
        ########################################################################
        def handle(self, input):
            match = self._rx.match(input)
            if match is not None:
                query = self._yamlfy_query(match.group('query'))
                if query is not None:
                    if query.has_key('count'):
                        query['ns'] = match.group('db') + '.'
                        query['ns'] += query['count']
                    elif query.has_key('findandmodify'):
                        if query.has_key('sort'):
                            query['orderby'] = query['sort']
                            del(query['sort'])
                        query['ns'] = match.group('db') + '.'
                        query['ns'] += query['findandmodify']
                    else:
                        return None
                return query
            return None

    ############################################################################
    # UpdateQueryHandler
    #   QueryLineHandler implementation for update queries
    ############################################################################
    class UpdateQueryHandler(QueryLineHandler):
        ########################################################################
        def __init__(self):
            self.name = 'Update Log Line Handler'
            self._regex = '.{20}\[conn(?P<connection_id>\d+)\] '
            self._regex += 'update (?P<ns>\S+\.\S+) query: '
            self._regex += '(?P<query>\{.*\}) update: (?P<update>\{.*\}) '
            self._regex += '(?P<options>(\S+:\S+ )*)(?P<query_time>\d+)ms'
            self._rx = re.compile(self._regex)
        
        ########################################################################
        def handle(self, input):
            match = self._rx.match(input)
            if match is not None:
                query = self._yamlfy_query(match.group('query'))
                if query is not None:
                    query['ns'] =  match.group('ns')
                return query
            return None


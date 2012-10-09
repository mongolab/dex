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
import os
import time
import datetime
import sys
import json
import re
import yaml
from bson import json_util

################################################################################
# Configuration
################################################################################

IGNORE_DBS = [  'local', 'admin']
IGNORE_COLLECTIONS = [u'system.namespaces',
                      u'system.profile',
                      u'system.users',
                      u'system.indexes']
BACKGROUND_FLAG = 'true'
WATCH_INTERVAL_SECONDS = 3.0
WATCH_DISPLAY_REFRESH_SECONDS = 30.0
DEFAULT_PROFILE_LEVEL = pymongo.SLOW_ONLY


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
    def __init__(self, db_uri, verbose, namespaces_list):
        self._query_analyzer = QueryAnalyzer()
        self._db_uri = db_uri
        self._verbose = verbose
        self._requested_namespaces = self._validate_namespaces(namespaces_list)
        self._recommendation_cache = []
        self._full_report = ReportAggregation()


    ############################################################################
    def analyze_query(self, db_uri, query, db_name, collection_name):
        """Analyzes a single query"""
        return self._query_analyzer._generate_query_report(db_uri,
                                                           query,
                                                           db_name,
                                                           collection_name)

    ############################################################################
    def _process_query(self, input, parser, run_stats):
        run_stats['linesPassed'] += 1
        raw_query = parser.parse(input)

        if raw_query is not None:
            run_stats['linesProcessed'] += 1
            namespace_tuple = self._tuplefy_namespace(raw_query['ns'])
            # If the query is for a requested namespace ....
            if self._namespace_requested(raw_query['ns']):
                db_name = namespace_tuple[0]
                collection_name = namespace_tuple[1]

                try:
                    query_report = self.analyze_query(self._db_uri,
                                                      raw_query,
                                                      db_name,
                                                      collection_name)
                except:
                    return 1
                recommendation = query_report['recommendation']
                if recommendation is not None:
                    self._full_report.add_report(query_report)
                    run_stats['linesRecommended'] += 1

    ############################################################################
    def analyze_profile(self):
        """Analyzes queries from a given log file"""
        run_stats = self._get_initial_run_stats()
        profile_parser = ProfileParser()
        databases = self._get_requested_databases()
        connection = pymongo.Connection(self._db_uri)

        if databases == []:
            try:
                databases = connection.database_names()
            except:
                message = "Error: Could not list databases on server. Please "\
                +         "check the auth components of your URI or provide "\
                +         "a namespace filter with -n.\n"
                sys.stderr.write(message)
                databases = []

            for ignore_db in IGNORE_DBS:
                if ignore_db in databases:
                    databases.remove(ignore_db)

        for database in databases:
            profile_entries = connection[database]['system.profile'].find()

            for profile_entry in profile_entries:
                self._process_query(profile_entry,
                                    profile_parser,
                                    run_stats)

        self._output_aggregated_report(sys.stdout, run_stats)

        return 0

    ############################################################################
    def watch_profile(self):
        """Analyzes queries from a given log file"""
        run_stats = self._get_initial_run_stats()
        profile_parser = ProfileParser()
        databases = self._get_requested_databases()
        connection = pymongo.Connection(self._db_uri)
        enabled_profile = False

        if databases == []:
            try:
                databases = connection.database_names()
            except:
                message = "Error: Could not list databases on server. Please "\
                +         "check the auth components of your URI.\n"
                sys.stderr.write(message)
                databases = []

            for ignore_db in IGNORE_DBS:
                if ignore_db in databases:
                    databases.remove(ignore_db)

        if len(databases) != 1:
            message = "Error: Please use namespaces (-n) to specify a single "\
            +         "database for profile watching.\n"
            sys.stderr.write(message)
            return 1

        database = databases[0]
        db = connection[database]

        initial_profile_level = db.profiling_level()

        if initial_profile_level is pymongo.OFF:
            message = "Profile level currently 0. Dex is setting profile "\
            +         "level 1. To run --watch at profile level 2, "\
            +         "enable profile level 2 before running Dex.\n"
            sys.stderr.write(message)
            db.set_profiling_level(DEFAULT_PROFILE_LEVEL)

        output_time = time.time() + WATCH_DISPLAY_REFRESH_SECONDS
        try:
            for profile_entry in self._tail_profile(db, WATCH_INTERVAL_SECONDS):
                self._process_query(profile_entry,
                                    profile_parser,
                                    run_stats)
                if time.time() >= output_time:
                    self._output_aggregated_report(sys.stderr, run_stats)
                    output_time = time.time() + WATCH_DISPLAY_REFRESH_SECONDS
        except KeyboardInterrupt:
            sys.stderr.write("Interrupt received\n")
        finally:
            self._output_aggregated_report(sys.stdout, run_stats)
            if initial_profile_level is pymongo.OFF:
                message = "Dex is resetting profile level to initial value " \
                +         "of 0. You may wish to drop the system.profile " \
                +         "collection.\n"
                sys.stderr.write(message)
                db.set_profiling_level(initial_profile_level)

        return 0

    ############################################################################
    def analyze_logfile(self, logfile_path):
        """Analyzes queries from a given log file"""
        run_stats = self._get_initial_run_stats()
        log_parser = LogParser()

        # For each line in the logfile ... 
        with open(logfile_path) as file:
            for line in file:
                self._process_query(line, log_parser, run_stats)
        self._output_aggregated_report(sys.stdout, run_stats)

        return 0

    ############################################################################
    def watch_logfile(self, logfile_path):
        """Analyzes queries from the tail of a given log file"""
        run_stats = self._get_initial_run_stats()
        log_parser = LogParser()

        # For each new line in the logfile ...
        output_time = time.time() + WATCH_DISPLAY_REFRESH_SECONDS
        try:
            for line in self._tail_file(open(logfile_path),
                                        WATCH_INTERVAL_SECONDS):
                self._process_query(line, log_parser, run_stats)
                if time.time() >= output_time:
                    self._output_aggregated_report(sys.stderr, run_stats)
                    output_time = time.time() + WATCH_DISPLAY_REFRESH_SECONDS
        except KeyboardInterrupt:
            sys.stderr.write("Interrupt received\n")
        finally:
            self._output_aggregated_report(sys.stdout, run_stats)

        return 0

    ############################################################################
    def _get_initial_run_stats(self):
        """Singlesource for initializing an output dict"""
        return { 'linesRecommended': 0,
                 'linesProcessed': 0,
                 'linesPassed': 0 }

    ############################################################################
    def _output_aggregated_report(self, out, run_stats):
        if self._verbose:
            results = self._full_report.get_aggregated_reports_verbose()
        else:
            results = self._full_report.get_aggregated_reports()

        output = { 'results': results,
                   'runStats': run_stats }
        out.write(pretty_json(output) + "\n")

    ############################################################################
    def _tail_file(self, file, interval):
        """Tails a file"""
        file.seek(0,2)
        while True:
            where = file.tell()
            line = file.readline()
            if not line:
                time.sleep(interval)
                file.seek(where)
            else:
                yield line

    ############################################################################
    def _tail_profile(self, db, interval):
        """Tails the system.profile collection"""
        latest_doc = None
        while latest_doc is None:
            time.sleep(interval)
            latest_doc = db['system.profile'].find_one()

        current_time = latest_doc['ts']

        while True:
            time.sleep(interval)
            cursor = db['system.profile'].find({'ts': {'$gte': current_time}}).sort('ts', pymongo.ASCENDING)
            for doc in cursor:
                current_time = doc['ts']
                yield doc


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
        if input_namespaces == []:
            return output_namespaces
        elif '*' in input_namespaces:
            if len(input_namespaces) > 1:
                warning = 'Warning: Multiple namespaces are '
                warning += 'ignored when one namespace is "*"\n'
                sys.stderr.write(warning)
            return output_namespaces
        else: 
            for namespace in input_namespaces:
                namespace_tuple = self._tuplefy_namespace(namespace)
                if namespace_tuple is None:
                    warning = 'Warning: Invalid namespace ' + namespace
                    warning += ' will be ignored\n'
                    sys.stderr.write(warning)
                else:
                    if namespace_tuple not in output_namespaces:
                        output_namespaces.append(namespace_tuple)
                    else:
                        warning = 'Warning: Duplicate namespace ' + namespace
                        warning += ' will be ignored\n'
                        sys.stderr.write(warning)
        return output_namespaces
                                   
    ############################################################################                             
    def _namespace_requested(self, namespace):
        """Checks whether the requested_namespaces contain the provided
            namespace"""
        if namespace is None:
            return False
        namespace_tuple = self._tuplefy_namespace(namespace)
        if namespace_tuple[0] in IGNORE_DBS:
            return False
        elif namespace_tuple[1] in IGNORE_COLLECTIONS:
            return False
        else:
            return self._tuple_requested(namespace_tuple)

    ############################################################################
    def _tuple_requested(self, namespace_tuple):
        """Helper for _namespace_requested. Supports limited wildcards"""
        if namespace_tuple is None:
            return False
        elif len(self._requested_namespaces) is 0:
            return True
        for requested_namespace in self._requested_namespaces:
            if (((requested_namespace[0] is '*') or
                 (requested_namespace[0].encode('utf-8') == namespace_tuple[0].encode('utf-8'))) and
                ((requested_namespace[1] is '*') or
                 (requested_namespace[1].encode('utf-8') == namespace_tuple[1].encode('utf-8')))):
                return True
        return False

    ############################################################################
    def _get_requested_databases(self):
        """Returns a list of databases requested, not including ignored dbs"""
        requested_databases = []
        if ((self._requested_namespaces is not None) and
            (self._requested_namespaces != [])):
            for requested_namespace in self._requested_namespaces:
                if requested_namespace[0] is '*':
                    return []
                elif requested_namespace[0] not in IGNORE_DBS:
                    requested_databases.append(requested_namespace[0])
        return requested_databases

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
        parsed_query = raw_query
        namespace = raw_query['ns']

        index_cache_entry = self._ensure_index_cache(db_uri,
                                                     db_name,
                                                     collection_name)
        indexes = index_cache_entry['indexes']
        query_analysis = self._generate_query_analysis(parsed_query,
                                                       db_name,
                                                       collection_name)
        if ((query_analysis['analyzedFields'] != []) and
            query_analysis['supported']):
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
    def _ensure_index_cache(self, db_uri, db_name, collection_name):
        """Adds a collections index entries to the cache if not present"""
        if db_uri is None:
            return {'indexes' : None}
        if db_name not in self.get_cache():
            self._internal_map[db_name] = {}
        if collection_name not in self._internal_map[db_name]:
            indexes = []
            try:
                connection = pymongo.Connection(db_uri)
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
# Parser
#   Provides a parse function that passes input to a round of handlers.
################################################################################
class Parser(object):
    def __init__(self, handlers):
        self._line_handlers = handlers

    def parse(self, input):
        """Passes input to each QueryLineHandler in use"""
        query = None
        for handler in self._line_handlers:
            try:
                query = handler.handle(input)
            except:
                query = None
            finally:
                if query is not None:
                    return query
        return None

################################################################################
# ProfileParser
#   Extracts queries from profile entries using a single ProfileEntryHandler
################################################################################
class ProfileParser(Parser):
    def __init__(self):
        """Declares the QueryLineHandlers to use"""
        super(ProfileParser, self).__init__([self.ProfileEntryHandler()])

    ############################################################################
    # Base ProfileEntryHandler class
    #   Knows how to yamlfy a logline query
    ############################################################################
    class ProfileEntryHandler:
        ########################################################################
        def handle(self, input):
            raw_query = {}
            if ((input is not None) and
                (input.has_key('op'))):
                if input['op'] == 'insert':
                    return None
                elif input['op'] == 'query':
                    if input['query'].has_key('$query'):
                        raw_query['query'] = input['query']['$query']
                        if input['query'].has_key('$orderby'):
                            raw_query['orderby'] = input['query']['$orderby']
                    else:
                        raw_query['query'] = input['query']
                    raw_query['millis'] = input['millis']
                    raw_query['ns'] = input['ns']
                    return raw_query
                elif input['op'] == 'update':
                    raw_query['query'] = input['query']
                    if input.has_key('updateobj'):
                        if input['updateobj'].has_key('orderby'):
                            raw_query['orderby'] = input['updateobj']['orderby']
                    raw_query['millis'] = input['millis']
                    raw_query['ns'] = input['ns']
                    return raw_query
                elif ((input['op'] == 'command') and
                      (input['command'].has_key('count'))):

                    raw_query = { 'query': input['command']['query'] }
                    db = input['ns'][0:input['ns'].rfind('.')]
                    raw_query['millis'] = input['millis']
                    raw_query['ns'] = db + "." + input['command']['count']
                    return raw_query
            else:
                return None



################################################################################
# LogParser
#   Extracts queries from log lines using a list of QueryLineHandlers
################################################################################
class LogParser(Parser):
    def __init__(self):
        """Declares the QueryLineHandlers to use"""
        super(LogParser, self).__init__([self.StandardQueryHandler(),
                                         self.CmdQueryHandler(),
                                         self.UpdateQueryHandler()])

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
            self._regex += '(?P<query>\{.*\}) (?P<options>(\S+ )*)'
            self._regex += '(?P<query_time>\d+)ms'
            self._rx = re.compile(self._regex)
    
        ########################################################################
        def handle(self, input):
            match = self._rx.match(input)
            if match is not None:
                query = self._yamlfy_query(match.group('query'))
                if query is not None:
                    query['millis'] = match.group('query_time')
                    query['ns'] =  match.group('ns')
                    if query["query"].has_key("$orderby"):
                        query["orderby"] = query["query"]["$orderby"]
                        del(query["query"]["$orderby"])
                    if query['query'].has_key("$query"):
                        query["query"] = query["query"]["$query"]
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
            self._regex += '(?P<query>\{.*\}) (?P<options>(\S+ )*)'
            self._regex += '(?P<query_time>\d+)ms'
            self._rx = re.compile(self._regex)
        
        ########################################################################
        def handle(self, input):
            match = self._rx.match(input)
            if match is not None:
                query = self._yamlfy_query(match.group('query'))
                if query is not None:
                    query['millis'] = match.group('query_time')
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
            self._regex += '(?P<options>(\S+ )*)(?P<query_time>\d+)ms'
            self._rx = re.compile(self._regex)

        ########################################################################
        def handle(self, input):
            match = self._rx.match(input)
            if match is not None:
                query = self._yamlfy_query(match.group('query'))
                if query is not None:
                    query['ns'] =  match.group('ns')
                    query['millis'] = match.group('query_time')
                return query
            return None

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
        existing_report = self._get_existing_Report(report)
        if existing_report is not None:
            self._merge_report(existing_report, report)
        else:
            self._reports.append(self._get_initial_report(report))

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
    def _get_existing_Report(self, report):
        """Returns the aggregated report that matches report"""
        for existing_report in self._reports:
            if existing_report['namespace'] == report['namespace']:
                if existing_report['recommendation'] == report['recommendation']:
                    return existing_report
        return None

    ############################################################################
    def _get_existing_query(self, report, queryAnalysis):
        """Returns the query in report that matches queryAnalysis"""
        mask = self._get_query_mask(queryAnalysis)
        for query in report['queriesCovered']:
            if mask == query['queryMask']:
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
            current_sig['avgTimeMillis'] = current_sig['totalTimeMillis'] / \
                                           current_sig['queryCount']

        else:
            target['queriesCovered'].append(self._get_initial_query(new))

        target['totalTimeMillis'] += query_millis
        target['queryCount'] += 1
        target['avgTimeMillis'] = target['totalTimeMillis'] / \
                                  target['queryCount']

    ############################################################################
    def _get_initial_report(self, report):
        """Returns a new aggregated report document"""
        return {
            'queriesCovered' : [ self._get_initial_query(report)],
            'totalTimeMillis' : int(report['parsed']['millis']),
            'avgTimeMillis' : int(report['parsed']['millis']),
            'queryCount' : 1,
            'recommendation' : report['recommendation'],
            'namespace' : report['namespace']
        }

    ############################################################################
    def _get_initial_query(self, report):
        """Returns a new query query document from the report"""
        initial_millis = int(report['parsed']['millis'])
        query = { 'queryMask' : self._get_query_mask(report['queryAnalysis']),
                  'totalTimeMillis': initial_millis,
                  'queryCount' : 1,
                  'avgTimeMillis': initial_millis }
        return query

    ############################################################################
    def _get_abbreviated_report(self, report):
        """Returns a minimum of fields from the report"""
        return { 'namespace' : report['namespace'],
                 'index' : report['recommendation']['index'],
                 'avgTimeMillis' : report['avgTimeMillis'],
                 'queryCount': report['queryCount'],
                 'totalTimeMillis': report['totalTimeMillis']}

    ############################################################################
    def _get_query_mask(self, queryAnalysis):
        """Converts a queryAnalysis to a query mask"""
        qmask = "'q': {"
        smask = "'s': {"
        qfirst = True
        sfirst = True
        for field in queryAnalysis['analyzedFields']:
            if field['fieldType'] is not SORT_TYPE:
                if qfirst:
                    qmask += "'" + field['fieldName'] + "': "
                    qfirst = False
                else:
                    qmask += ", '" + field['fieldName'] + "': "
                qmask += "'<" + field['fieldName'] + ">' "

            else:
                if sfirst:
                    smask += "'" + field['fieldName'] + "': "
                    sfirst = False
                else:
                    smask += ", '" + field['fieldName'] + "': "
                smask += "<sort-order>"

        if sfirst:
            return "{" + qmask + "}}"
        else:
            return "{" + qmask + "}, " + smask + "}}"



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
import sys
import time
from utils import pretty_json
from analyzer import QueryAnalyzer, ReportAggregation
from parsers import LogParser, ProfileParser, get_line_time
from datetime import datetime
from datetime import timedelta
import traceback
try:
    from collections import OrderedDict
except ImportError:
    from ordereddict import OrderedDict

################################################################################
# Configuration
################################################################################

IGNORE_DBS = ['local', 'admin', 'config']
IGNORE_COLLECTIONS = [u'system.namespaces',
                      u'system.profile',
                      u'system.users',
                      u'system.indexes']
WATCH_INTERVAL_SECONDS = 3.0
WATCH_DISPLAY_REFRESH_SECONDS = 30.0
DEFAULT_PROFILE_LEVEL = pymongo.SLOW_ONLY


################################################################################
# Dex
#   Uses a QueryAnalyzer (with included LogParser) to analyze a MongoDB
#   query or logfile
################################################################################
class Dex:

    ############################################################################
    def __init__(self, db_uri, verbose, namespaces_list, slowms, check_indexes, timeout):
        self._check_indexes = check_indexes
        self._query_analyzer = QueryAnalyzer(check_indexes)
        self._db_uri = db_uri
        self._slowms = slowms
        self._verbose = verbose
        self._requested_namespaces = self._validate_namespaces(namespaces_list)
        self._recommendation_cache = []
        self._report = ReportAggregation()
        self._start_time = None
        self._timeout_time = None
        self._timeout = timeout
        self._run_stats = self._get_initial_run_stats()
        self._first_line = True

    ############################################################################
    def generate_query_report(self, db_uri, query, db_name, collection_name):
        """Analyzes a single query"""
        return self._query_analyzer.generate_query_report(db_uri,
                                                          query,
                                                          db_name,
                                                          collection_name)

    ############################################################################
    def _process_query(self, input, parser):
        self._run_stats['linesRead'] += 1

        line_time = get_line_time(input)

        if line_time is not None:
            if ((self._run_stats['timeRange']['start'] is None) or
                (self._run_stats['timeRange']['start'] > line_time)):
                self._run_stats['timeRange']['start'] = line_time
            if ((self._run_stats['timeRange']['end'] is None) or
                (self._run_stats['timeRange']['end'] < line_time)):
                self._run_stats['timeRange']['end'] = line_time

        parsed = parser.parse(input)

        if parsed is not None:
            if parsed['supported']:
                self._run_stats['linesAnalyzed'] += 1
                namespace_tuple = self._tuplefy_namespace(parsed['ns'])
                # If the query is for a requested namespace ....
                if self._namespace_requested(parsed['ns']):
                    db_name = namespace_tuple[0]
                    collection_name = namespace_tuple[1]
                    query_report = None
                    if parsed['stats']['millis'] >= self._slowms:
                        try:
                            query_report = self.generate_query_report(self._db_uri,
                                                                      parsed,
                                                                      db_name,
                                                                      collection_name)
                        except Exception as e:
                            #print traceback.print_exc()
                            return 1
                    if query_report is not None:
                        if query_report['recommendation'] is not None:
                            self._run_stats['linesWithRecommendations'] += 1
                        self._report.add_query_occurrence(query_report)
            else:
                self._run_stats['unparsableLineInfo']['unparsableLines'] += 1
                self._run_stats['unparsableLineInfo']['unparsableLinesWithTime'] += 1
                self._run_stats['unparsableLineInfo']['unparsedTimeMillis'] += int(parsed['stats']['millis'])
                self._run_stats['unparsableLineInfo']['unparsedAvgTimeMillis'] = self._run_stats['unparsableLineInfo']['unparsedTimeMillis'] / self._run_stats['unparsableLineInfo']['unparsableLinesWithTime']
        else:
            self._run_stats['unparsableLineInfo']['unparsableLines'] += 1
            self._run_stats['unparsableLineInfo']['unparsableLinesWithoutTime'] += 1

    ############################################################################
    def analyze_profile(self):
        """Analyzes queries from a given log file"""
        profile_parser = ProfileParser()
        databases = self._get_requested_databases()
        connection = pymongo.MongoClient(self._db_uri,
                                         document_class=OrderedDict,
                                         read_preference=pymongo.ReadPreference.PRIMARY_PREFERRED)

        if databases == []:
            try:
                databases = connection.database_names()
            except:
                message = "Error: Could not list databases on server. Please " \
                          +         "check the auth components of your URI or provide " \
                          +         "a namespace filter with -n.\n"
                sys.stderr.write(message)
                databases = []

            for ignore_db in IGNORE_DBS:
                if ignore_db in databases:
                    databases.remove(ignore_db)

        for database in databases:

            db = connection[database]

            profile_entries = db['system.profile'].find()

            for profile_entry in profile_entries:
                self._process_query(profile_entry,
                                    profile_parser)

        self._output_aggregated_report(sys.stdout)

        return 0

    ############################################################################
    def watch_profile(self):
        """Analyzes queries from a given log file"""
        profile_parser = ProfileParser()
        databases = self._get_requested_databases()
        connection = pymongo.MongoClient(self._db_uri,
                                         document_class=OrderedDict,
                                         read_preference=pymongo.ReadPreference.PRIMARY_PREFERRED)
        enabled_profile = False

        if databases == []:
            try:
                databases = connection.database_names()
            except:
                message = "Error: Could not list databases on server. Please " \
                          +         "check the auth components of your URI.\n"
                sys.stderr.write(message)
                databases = []

            for ignore_db in IGNORE_DBS:
                if ignore_db in databases:
                    databases.remove(ignore_db)

        if len(databases) != 1:
            message = "Error: Please use namespaces (-n) to specify a single " \
                      +         "database for profile watching.\n"
            sys.stderr.write(message)
            return 1

        database = databases[0]
        db = connection[database]

        initial_profile_level = db.profiling_level()

        if initial_profile_level is pymongo.OFF:
            message = "Profile level currently 0. Dex is setting profile " \
                      +         "level 1. To run --watch at profile level 2, " \
                      +         "enable profile level 2 before running Dex.\n"
            sys.stderr.write(message)
            db.set_profiling_level(DEFAULT_PROFILE_LEVEL)

        output_time = time.time() + WATCH_DISPLAY_REFRESH_SECONDS
        try:
            for profile_entry in self._tail_profile(db, WATCH_INTERVAL_SECONDS):
                self._process_query(profile_entry,
                                    profile_parser)
                if time.time() >= output_time:
                    self._output_aggregated_report(sys.stderr)
                    output_time = time.time() + WATCH_DISPLAY_REFRESH_SECONDS
        except KeyboardInterrupt:
            sys.stderr.write("Interrupt received\n")
        finally:
            self._output_aggregated_report(sys.stdout)
            if initial_profile_level is pymongo.OFF:
                message = "Dex is resetting profile level to initial value " \
                          +         "of 0. You may wish to drop the system.profile " \
                          +         "collection.\n"
                sys.stderr.write(message)
                db.set_profiling_level(initial_profile_level)

        return 0

    ############################################################################
    def analyze_logfile(self, logfile_path):
        self._run_stats['logSource'] = logfile_path
        """Analyzes queries from a given log file"""
        with open(logfile_path) as obj:
            self.analyze_logfile_object(obj)

        self._output_aggregated_report(sys.stdout)

        return 0

    ############################################################################
    def analyze_logfile_object(self, file_object):
        """Analyzes queries from a given log file"""
        log_parser = LogParser()

        if self._start_time is None:
            self._start_time = datetime.now()
            if self._timeout != 0:
                self._end_time = self._start_time + timedelta(minutes=self._timeout)
            else:
                self._end_time = None

        # For each line in the logfile ...
        for line in file_object:
            if self._end_time is not None and datetime.now() > self._end_time:
                self._run_stats['timedOut'] = True
                self._run_stats['timeoutInMinutes'] = self._timeout
                break
            self._process_query(line, log_parser)

        return 0

    ############################################################################
    def watch_logfile(self, logfile_path):
        """Analyzes queries from the tail of a given log file"""
        self._run_stats['logSource'] = logfile_path
        log_parser = LogParser()

        # For each new line in the logfile ...
        output_time = time.time() + WATCH_DISPLAY_REFRESH_SECONDS
        try:
            firstLine = True
            for line in self._tail_file(open(logfile_path),
                                        WATCH_INTERVAL_SECONDS):
                if firstLine:
                    self._run_stats['timeRange']['start'] = get_line_time(line)
                self._process_query(line, log_parser)
                self._run_stats['timeRange']['end'] = get_line_time(line)
                if time.time() >= output_time:
                    self._output_aggregated_report(sys.stderr)
                    output_time = time.time() + WATCH_DISPLAY_REFRESH_SECONDS
        except KeyboardInterrupt:
            sys.stderr.write("Interrupt received\n")
        finally:
            self._output_aggregated_report(sys.stdout)

        return 0

    ############################################################################
    def _get_initial_run_stats(self):
        """Singlesource for initializing an output dict"""
        return OrderedDict([('linesWithRecommendations', 0),
                            ('linesAnalyzed', 0),
                            ('linesRead', 0),
                            ('dexTime', datetime.utcnow()),
                            ('logSource', None),
                            ('timeRange', OrderedDict([('start', None),
                                                       ('end', None)])),
                            ('unparsableLineInfo', OrderedDict([('unparsableLines', 0),
                                                                ('unparsableLinesWithoutTime', 0),
                                                                ('unparsableLinesWithTime', 0),
                                                                ('unparsedTimeMillis', 0),
                                                                ('unparsedAvgTimeMillis', 0)]))])

    ############################################################################
    def _make_aggregated_report(self):
        output = OrderedDict([('runStats', self._run_stats),
                              ('results', self._report.get_reports())])
        return output

    ############################################################################
    def _output_aggregated_report(self, out):
        out.write(pretty_json(self._make_aggregated_report()).replace('"', "'").replace("\\'", '"') + "\n")

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
                if not isinstance(namespace, unicode):
                    namespace = unicode(namespace)
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
        if not isinstance(namespace_tuple[0], unicode):
            encoded_db = unicode(namespace_tuple[0])
        else:
            encoded_db = namespace_tuple[0]
        if not isinstance(namespace_tuple[1], unicode):
            encoded_coll = unicode(namespace_tuple[1])
        else:
            encoded_coll = namespace_tuple[1]

        if namespace_tuple is None:
            return False
        elif len(self._requested_namespaces) is 0:
            return True
        for requested_namespace in self._requested_namespaces:
            if  ((((requested_namespace[0]) == u'*') or
                 (encoded_db == requested_namespace[0])) and
                (((requested_namespace[1]) == u'*') or
                 (encoded_coll == requested_namespace[1]))):
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

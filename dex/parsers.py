__author__ = 'eric'

import re
from utils import pretty_json, small_json, yamlfy
from time import strptime, mktime
from datetime import datetime
import traceback

try:
    from collections import OrderedDict
except ImportError:
    from ordereddict import OrderedDict


################################################################################
# Query masking and scrubbing functions
################################################################################

def scrub(e):
    if isinstance(e, dict):
        return scrub_doc(e)
    elif isinstance(e, list):
        return scrub_list(e)
    else:
        return None


def scrub_doc(d):
    for k in d:
        if k in ['$in', '$nin', '$all']:
            d[k] = ["<val>"]
        else:
            d[k] = scrub(d[k])
        if d[k] is None:
            d[k] = "<val>"
    return d


def scrub_list(a):
    v = []
    for e in a:
        e = scrub(e)
        if e is not None:
            v.append(scrub(e))
    return sorted(v)


ts_rx = re.compile('^(?P<ts>[a-zA-Z]{3} [a-zA-Z]{3} {1,2}\d+ \d{2}:\d{2}:\d{2}).*')
def get_line_time(line):
    ts = None
    match = ts_rx.match(line)
    if match:
        year = datetime.utcnow().year
        timestamp = mktime(strptime(match.group('ts') + ' ' + str(year), '%a %b %d %H:%M:%S %Y'))
        ts = datetime.fromtimestamp(timestamp)
    return ts


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
            except Exception as e:
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

    def get_line_time(self, input):
        return input['ts'] if 'ts' in input else None

    ############################################################################
    # Base ProfileEntryHandler class
    #   Knows how to yamlfy a logline query
    ############################################################################
    class ProfileEntryHandler:
        ########################################################################
        def handle(self, input):
            result = OrderedDict()
            query = None
            orderby = None

            if (input is not None) and (input.has_key('op')):
                if input['op'] == 'query':
                    if input['query'].has_key('$query'):
                        query = input['query']['$query']
                        if input['query'].has_key('$orderby'):
                            orderby = input['query']['$orderby']
                    else:
                        query = input['query']
                    result['ns'] = input['ns']
                elif input['op'] == 'update':
                    query = input['query']
                    if input.has_key('updateobj'):
                        if input['updateobj'].has_key('orderby'):
                            orderby = input['updateobj']['orderby']
                    result['ns'] = input['ns']
                elif ((input['op'] == 'command') and
                          ((input['command'].has_key('count')) or
                               (input['command'].has_key('findAndModify')))):
                    query = input['command']['query']
                    db = input['ns'][0:input['ns'].rfind('.')]
                    result['ns'] = db + "." + input['command']['count']
                else:
                    return None

                toMask = OrderedDict()

                if orderby is not None:
                    result['orderby'] = orderby
                    toMask['$orderby'] = orderby
                result['query'] = scrub(query)
                toMask['$query'] = query

                result['queryMask'] = small_json(toMask)
                result['stats'] = {'millis': input['millis']}
                return result
            else:
                return None


################################################################################
# LogParser
#   Extracts queries from log lines using a list of QueryLineHandlers
################################################################################
class LogParser(Parser):
    def __init__(self):
        """Declares the QueryLineHandlers to use"""
        super(LogParser, self).__init__([CmdQueryHandler(),
                                         UpdateQueryHandler(),
                                         StandardQueryHandler(),
                                         TimeLineHandler()])


############################################################################
# Base QueryLineHandler class
#   Knows how to yamlfy a logline query
############################################################################
class QueryLineHandler:
    ########################################################################
    def parse_query(self, extracted_query):
        return yamlfy(extracted_query)

    def handle(self, line):

        result = self.do_handle(line)
        if result is not None:
            result['ts'] = get_line_time(line)
            return result

    def do_handle(self, line):
        return None

    def parse_line_stats(self, stat_string):
        line_stats = {}
        split = stat_string.split(" ")

        for stat in split:
            if stat is not "" and stat is not None and stat != "locks(micros)":
                stat_split = stat.split(":")
                if (stat_split is not None) and (stat_split is not "") and (len(stat_split) is 2):
                    try:
                        line_stats[stat_split[0]] = int(stat_split[1])
                    except:
                        pass

        return line_stats

    def standardize_query(self, query_yaml):
        if len(query_yaml.keys()) == 1:
            if '$query' in query_yaml:
                return scrub(query_yaml)
            if 'query' in query_yaml:
                return OrderedDict([('$query', scrub(query_yaml['query']))])

        if len(query_yaml.keys()) == 2:
            query = None
            orderby = None

            if 'query' in query_yaml:
                query = query_yaml['query']
            elif '$query' in query_yaml:
                query = query_yaml['$query']

            if 'orderby' in query_yaml:
                orderby = query_yaml['orderby']
            elif '$orderby' in query_yaml:
                orderby = query_yaml['$orderby']

            if query is not None and orderby is not None:
                return OrderedDict([('$query', scrub(query)),
                                    ('$orderby', orderby)])

        return OrderedDict([('$query', scrub(query_yaml))])



############################################################################
# StandardQueryHandler
#   QueryLineHandler implementation for general queries (incl. getmore)
############################################################################
class StandardQueryHandler(QueryLineHandler):
    ########################################################################
    def __init__(self):
        self.name = 'Standard Query Log Line Handler'
        self._regex = '.*\[(?P<connection>\S*)\] '
        self._regex += '(?P<operation>\S+) (?P<ns>\S+\.\S+) query: '
        self._regex += '(?P<query>\{.*\}) (?P<stats>(\S+ )*)'
        self._regex += '(?P<query_time>\d+)ms'
        self._rx = re.compile(self._regex)

    ########################################################################
    def do_handle(self, input):
        match = self._rx.match(input)
        if match is not None:
            parsed = self.parse_query(match.group('query'))
            if parsed is not None:
                result = OrderedDict()
                scrubbed = self.standardize_query(parsed)
                result['query'] = scrubbed['$query']
                if '$orderby' in scrubbed:
                    result['orderby'] = scrubbed['$orderby']
                result['queryMask'] = small_json(scrubbed)
                result['ns'] = match.group('ns')
                result['stats'] = self.parse_line_stats(match.group('stats'))
                result['stats']['millis'] = match.group('query_time')
                result['supported'] = True
                return result
        return None


############################################################################
# CmdQueryHandler
#   QueryLineHandler implementation for $cmd queries (count, findandmodify)
############################################################################
class CmdQueryHandler(QueryLineHandler):
    ########################################################################
    def __init__(self):
        self.name = 'CMD Log Line Handler'
        self._regex = '.*\[conn(?P<connection_id>\d+)\] '
        self._regex += 'command (?P<db>\S+)\.\$cmd command: '
        self._regex += '(?P<query>\{.*\}) (?P<stats>(\S+ )*)'
        self._regex += '(?P<query_time>\d+)ms'
        self._rx = re.compile(self._regex)

    ########################################################################
    def do_handle(self, input):
        match = self._rx.match(input)
        if match is not None:
            parsed = self.parse_query(match.group('query'))
            if parsed is not None:
                result = OrderedDict()
                result['stats'] = self.parse_line_stats(match.group('stats'))
                result['stats']['millis'] = match.group('query_time')

                command = parsed.keys()[0]

                toMask = OrderedDict()

                result['command'] = command
                result['supported'] = True
                if command.lower() == 'count':
                    result['ns'] = match.group('db') + '.'
                    result['ns'] += parsed[command]
                    query = self.standardize_query(parsed['query'])
                    result['query'] = query['$query']
                    toMask = query
                elif command.lower() == 'findandmodify':
                    if 'sort' in parsed:
                        result['orderby'] = parsed['sort']
                        toMask['$orderby'] = parsed['sort']
                    result['ns'] = match.group('db') + '.'
                    result['ns'] += parsed[command]
                    query = self.standardize_query(parsed['query'])
                    result['query'] = query['$query']
                    if 'sort' in parsed:
                        result['orderby'] = parsed['sort']
                        toMask['$orderby'] = parsed['sort']
                    toMask['$query'] = query
                elif command.lower() == 'geonear':
                    result['ns'] = match.group('db') + '.'
                    result['ns'] += parsed[command]
                    query = self.standardize_query(parsed['search'])
                    result['query'] = query
                    toMask = query
                else:
                    result['supported'] = False
                    result['ns'] = match.group('db') + '.$cmd'

                result['command'] = command
                toMask['$cmd'] = command
                result['queryMask'] = small_json(toMask)

                return result
        return None


############################################################################
# UpdateQueryHandler
#   QueryLineHandler implementation for update queries
############################################################################
class UpdateQueryHandler(QueryLineHandler):
    ########################################################################
    def __init__(self):
        self.name = 'Update Log Line Handler'
        self._regex = '.*\[conn(?P<connection_id>\d+)\] '
        self._regex += 'update (?P<ns>\S+\.\S+) query: '
        self._regex += '(?P<query>\{.*\}) update: (?P<update>\{.*\}) '
        self._regex += '(?P<stats>(\S+ )*)(?P<query_time>\d+)ms'
        self._rx = re.compile(self._regex)

    ########################################################################
    def do_handle(self, input):

        match = self._rx.match(input)
        if match is not None:
            parsed = self.parse_query(match.group('query'))
            if parsed is not None:
                result = OrderedDict()
                scrubbed = self.standardize_query(parsed)
                result['query'] = scrubbed['$query']
                if '$orderby' in scrubbed:
                    result['orderby'] = scrubbed['$orderby']
                result['queryMask'] = small_json(scrubbed)
                result['ns'] = match.group('ns')
                result['stats'] = self.parse_line_stats(match.group('stats'))
                result['stats']['millis'] = match.group('query_time')
                result['supported'] = True
                return result
        return None

############################################################################
# Empty TimeLineHandler class
#   Last Resort for unparsed lines
############################################################################
class TimeLineHandler(QueryLineHandler):
    ########################################################################
    def __init__(self):
        self.name = 'Standard Query Log Line Handler'
        self._regex = '.*(?P<query_time>\d+)ms'
        self._rx = re.compile(self._regex)

    ########################################################################
    def do_handle(self, input):
        match = self._rx.match(input)
        if match is not None:
            return {'ns': "?",
                    'stats': {"millis": match.group('query_time')},
                    'supported': False,
                    'queryMask': None
            }
        return None





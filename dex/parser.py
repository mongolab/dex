__author__ = 'eric'

import re
import yaml
import yaml.constructor
try:
    from collections import OrderedDict
except ImportError:
    from ordereddict import OrderedDict

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

    ############################################################################
    # Base ProfileEntryHandler class
    #   Knows how to yamlfy a logline query
    ############################################################################
    class ProfileEntryHandler:
        ########################################################################
        def handle(self, input):
            raw_query = OrderedDict({})
            if ((input is not None) and
                    (input.has_key('op'))):
                if input['op'] == 'insert':
                    return None
                elif input['op'] == 'query':
                    if input['query'].has_key('$query'):
                        raw_query['query'] = input['query']['$query']
                        if input['query'].has_key('$orderby'):
                            orderby = input['query']['$orderby']
                            raw_query['orderby'] = orderby
                    else:
                        raw_query['query'] = input['query']
                    raw_query['millis'] = input['millis']
                    raw_query['ns'] = input['ns']
                    return raw_query
                elif input['op'] == 'update':
                    raw_query['query'] = input['query']
                    if input.has_key('updateobj'):
                        if input['updateobj'].has_key('orderby'):
                            orderby = input['updateobj']['orderby']
                            raw_query['orderby'] = orderby
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
            temp_query = yaml.load(extracted_query, OrderedDictYAMLLoader)
            if temp_query is not None:
                if temp_query.has_key('query'):
                    return temp_query
                else:
                    return {'query': temp_query}
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
            #self._regex = '(?P<ts>[a-zA-Z]{3} [a-zA-Z]{3} {1,2}\d+ \d{2}:\d{2}:\d{2})'
            self._regex = '.*\[(?P<connection>\S*)\] '
            self._regex += '(?P<operation>\S+) (?P<ns>\S+\.\S+) query: '
            self._regex += '(?P<query>\{.*\}) (?P<stats>(\S+ )*)'
            self._regex += '(?P<query_time>\d+)ms'
            self._rx = re.compile(self._regex)

        ########################################################################
        def handle(self, input):
            match = self._rx.match(input)
            if match is not None:
                query = self._yamlfy_query(match.group('query'))
                if query is not None:
                    #query['time'] = datetime.strptime(match.group('ts'), "%a %b %d %H:%M:%S")
                    query['millis'] = match.group('query_time')
                    query['ns'] =  match.group('ns')
                    if query["query"].has_key("$orderby"):
                        query["orderby"] = query["query"]["$orderby"]
                        del(query["query"]["$orderby"])
                    if query['query'].has_key("$query"):
                        query["query"] = query["query"]["$query"]
                    query['stats'] = parse_line_stats(match.group('stats'))
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
            self._regex = '.*\[conn(?P<connection_id>\d+)\] '
            self._regex += 'command (?P<db>\S+)\.\$cmd command: '
            self._regex += '(?P<query>\{.*\}) (?P<stats>(\S+ )*)'
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
                    query['stats'] = parse_line_stats(match.group('stats'))
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
            self._regex = '.*\[conn(?P<connection_id>\d+)\] '
            self._regex += 'update (?P<ns>\S+\.\S+) query: '
            self._regex += '(?P<query>\{.*\}) update: (?P<update>\{.*\}) '
            self._regex += '(?P<stats>(\S+ )*)(?P<query_time>\d+)ms'
            self._rx = re.compile(self._regex)

        ########################################################################
        def handle(self, input):
            match = self._rx.match(input)
            if match is not None:
                query = self._yamlfy_query(match.group('query'))
                if query is not None:
                    query['ns'] =  match.group('ns')
                    query['millis'] = match.group('query_time')
                    query['stats'] = parse_line_stats(match.group('stats'))
                return query
            return None

# From https://gist.github.com/844388
class OrderedDictYAMLLoader(yaml.Loader):
    """
    A YAML loader that loads mappings into ordered dictionaries.
    """

    def __init__(self, *args, **kwargs):
        yaml.Loader.__init__(self, *args, **kwargs)

        self.add_constructor(u'tag:yaml.org,2002:map', type(self).construct_yaml_map)
        self.add_constructor(u'tag:yaml.org,2002:omap', type(self).construct_yaml_map)

    def construct_yaml_map(self, node):
        data = OrderedDict()
        yield data
        value = self.construct_mapping(node)
        data.update(value)

    def construct_mapping(self, node, deep=False):
        if isinstance(node, yaml.MappingNode):
            self.flatten_mapping(node)
        else:
            raise yaml.constructor.ConstructorError(None, None,
                                                    'expected a mapping node, but found %s' % node.id, node.start_mark)

        mapping = OrderedDict()
        for key_node, value_node in node.value:
            key = self.construct_object(key_node, deep=deep)
            try:
                hash(key)
            except TypeError, exc:
                raise yaml.constructor.ConstructorError('while constructing a mapping',
                                                        node.start_mark, 'found unacceptable key (%s)' % exc, key_node.start_mark)
            value = self.construct_object(value_node, deep=deep)
            mapping[key] = value
        return mapping


def parse_line_stats(stat_string):
    line_stats = {}

    split = stat_string.split(" ")
    
    for stat in split:
        if stat is not "" and stat is not None and stat is not "locks(micros)":
            stat_split = stat.split(":")
            if (stat_split is not None) and (stat_split is not "") and (len(stat_split) is 2):
                try:
                    line_stats[stat_split[0]] = int(stat_split[1])
                except:
                    pass

    return line_stats





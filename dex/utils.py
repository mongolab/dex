__author__ = 'eric'

import json
from bson import json_util
import yaml
import yaml.constructor
from datetime import datetime, date

try:
    from collections import OrderedDict
except ImportError:
    from ordereddict import OrderedDict


################################################################################
# Utilities
################################################################################
def pretty_json(obj):
    return json.dumps(obj, indent=4, default=_custom_json_hook)


def _custom_json_hook(obj):
    if type(obj) in [datetime, date]:
        return {"$date": obj.strftime("%Y-%m-%dT%H:%M:%S.000Z")}
    else:
        return json_util.default(obj)


def validate_yaml(string):
    try:
        yamlfy(string)
    except:
        return False
    else:
        return True


def small_json(obj):
    return json.dumps(obj, sort_keys=True, separators=(',',':'))


def yamlfy(string):
    return yaml.load(string, OrderedDictYAMLLoader)


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

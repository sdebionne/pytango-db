"""Yaml backend for tango.databaseds.database.
"""

import logging
import os
import weakref

from . import _abstract
from ruamel.yaml import YAML
from collections.abc import MutableSequence, MutableMapping

_logger = logging.getLogger(__name__)


class NodeList(list):
    def __init__(self, sequence=None, parent=None):
        super(NodeList, self).__init__()
        if sequence:
            self.extend(sequence)
        if parent is not None:
            self.__parent = weakref.ref(parent)
        else:
            self.__parent = None

    def _patch(self, value):
        if isinstance(value, MutableMapping):
            if not isinstance(value, Node):
                value = Node(value, parent=self.parent)
        elif isinstance(value, MutableSequence):
            if not isinstance(value, NodeList):
                value = NodeList(value, parent=self.parent)
        return value

    def __getitem__(self, key):
        value = super(NodeList, self).__getitem__(key)
        value2 = self._patch(value)
        if value is not value2:
            self[key] = value2
        return value2

    def __iter__(self):
        for i in range(0, len(self)):
            yield self[i]

    @property
    def parent(self):
        parent = self.__parent
        if parent is None:
            return None
        return parent()


class Node(MutableMapping):
    def __init__(self, *args, parent=None, **kwargs):
        self.__dict = {}
        self.__dict.update(*args, **kwargs)
        if parent is not None:
            self.__parent = weakref.ref(parent)
        else:
            self.__parent = None

    def _patch(self, value):
        if isinstance(value, MutableMapping):
            if not isinstance(value, Node):
                value = Node(value, parent=self)
        elif isinstance(value, MutableSequence):
            if not isinstance(value, NodeList):
                value = NodeList(value, parent=self)
        return value

    def __getitem__(self, key):
        value = self.__dict.__getitem__(key)
        value2 = self._patch(value)
        if value is not value2:
            self.__dict[key] = value2
        return value2

    def __setitem__(self, key, value):
        self.__dict.__setitem__(key, value)

    def __delitem__(self, key):
        self.__dict.__delitem__(key)

    def __contains__(self, key):
        return self.__dict.__contains__(key)

    def pop(self, key, *args, **kwargs):
        value = self.__dict.pop(key, *args, **kwargs)
        value2 = self._patch(value)
        return value2

    def get(self, key, default=None):
        value = self.__dict.get(key, default)
        value2 = self._patch(value)
        if value is not value2:
            if key in self.__dict:
                self.__dict[key] = value2
        return value

    def setdefault(self, key, *args, **kwargs):
        return self.__dict.setdefault(key, *args, **kwargs)

    def update(self, arg={}, **kwargs):
        self.__dict.update(arg)
        if kwargs:
            self.__dict.update(kwargs)

    def __iter__(self):
        for i in self.__dict:
            yield i

    def __len__(self):
        return len(self.__dict)

    @property
    def parent(self):
        parent = self.__parent
        if parent is None:
            return None
        return parent()

    def __hash__(self):
        return id(self).__hash__()

    set = update

    def get_all(self):
        return dict(self.__dict)

    def save(self):
        pass


class FiltrableDict(dict):
    def keys(self, filter_key=None):
        if filter is None:
            return super(FiltrableDict, self).keys()
        ks = super(FiltrableDict, self).keys()
        return _abstract.list_filter(filter_key, ks)


class YamlDataSource(_abstract.DataSource):
    def __init__(self, personal_name, db_path):
        yaml_root = db_path
        if not os.path.exists(yaml_root):
            raise RuntimeError(f"Path '{yaml_root}' do not exists")
        if not os.path.isdir(yaml_root):
            raise RuntimeError(f"Path '{yaml_root}' is not a directory")
        self._yaml_root = yaml_root
        _abstract.DataSource.__init__(self, personal_name)

    def _init_db(self):
        nodes = []
        for meta in self._iter_data_source():
            if isinstance(meta, MutableSequence):
                n = NodeList(meta)
                nodes.append(n)
            else:
                n = Node(meta)
                nodes.append(n)
        self._nodes = nodes
        self._devices_info = FiltrableDict()
        self._class_attribute = {}
        self._property_attr_device = {}
        self._aliases = {}

    def _get_root_node(self):
        return self._nodes

    def create_empty(self, parent=None, path=None):
        return Node(parent=parent)

    def create_device(self, device_info, parent=None):
        return Node(device_info, parent=parent)

    def _iter_data_source(self):
        parser = YAML(pure=True)
        for root, _dirs, files in os.walk(self._yaml_root, followlinks=True):
            if "__init__.yml" in files:
                files.remove("__init__.yml")
                files.insert(0, "__init__.yml")
            for file in files:
                filename, file_extension = os.path.splitext(file)
                if not (file_extension in [".yml", ".yaml"]):
                    continue
                path = os.path.join(root, file)
                _logger.debug("Read Yaml filename %s", path)
                with open(path, "rt", encoding="utf-8") as f:
                    meta = parser.load(f)
                    yield meta

    def get_node(self, refname):
        node = self.tango_name_2_node[refname]
        if not isinstance(node, Node):
            node = Node(node)
            self.tango_name_2_node[refname] = node
        return node

    def create_class_filename(self, class_name):
        _logger.error("create_class_filename '%s' is not implemented", class_name)
        node = Node()
        return node

    def create_server_filename(self, server_name):
        _logger.error("create_server_filename '%s' is not implemented", server_name)
        node = Node()
        return node

    def get_attr_alias_mapping(self):
        return self._aliases

    def get_class_attribute_list(self, class_name, wildcard):
        return []

    def get_class_attribute(self, klass_name, attr_name):
        key_name = "%s.%s" % (klass_name, attr_name)
        attrs = self._class_attribute.get(key_name, None)
        if attrs is None:
            attrs = Node()
            # attrs = {}
            self._class_attribute[key_name] = attrs
        return attrs

    def get_property_attr_device(self, dev_name):
        key_name = dev_name.lower().replace("/", ".")
        attrs = self._property_attr_device.get(key_name, None)
        if attrs is None:
            attrs = {}
            self._property_attr_device[key_name] = attrs
        if not isinstance(attrs, Node):
            attrs = Node(attrs)
        return attrs

    def get_devices_info(self):
        return self._devices_info

    def get_exported_device_info(self, dev_name):
        info = self._devices_info.get(dev_name)
        if info is None:
            info = Node()
            self._devices_info[dev_name] = info
        return info

    def get_exported_devices_keys(self, key_filter):
        return self._devices_info.keys(key_filter)

    def __str__(self):
        result = []
        for i in self._get_root_node():
            result.append(str(i))
        return "; ".join(result)


class yaml(_abstract.dbapi):

    DB_API_NAME = "yaml"

    def _create_data_source(self, personal_name, db_path):
        return YamlDataSource(personal_name, db_path)


def get_db(personal_name="2", db_path="tango", **keys):
    return yaml(personal_name=personal_name, db_path=db_path)


def get_wildcard_replacement():
    return False

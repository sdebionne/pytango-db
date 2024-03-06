import logging
import re
import typing
import weakref
import datetime
import abc
from collections.abc import MutableSequence, MutableMapping
from tango.databaseds import db_errors
import tango

_logger = logging.getLogger(__name__)
# _logger.setLevel(logging.DEBUG)

th_exc = tango.Except.throw_exception

if _logger.isEnabledFor(logging.DEBUG):

    def _debug(funct):
        def f(self, *args, **kwargs):
            _logger.debug("%s: %s %s", funct.__name__, args, kwargs)
            try:
                returnVal = funct(self, *args, **kwargs)
            except Exception:
                _logger.critical("Exception during %s", funct.__name__, exc_info=True)
                raise
            if returnVal is not None:
                _logger.debug("return %s -> %s", funct.__name__, returnVal)
            else:
                _logger.debug("return %s", funct.__name__)
            return returnVal

        return f

else:

    def _debug(funct):
        return funct


def list_filter(pattern: str, l: typing.List[str]) -> typing.List[str]:
    """
    Filter a list of string with a pattern which can contain wildcare `*`.

    The case is not taken into account.

    Arguments:
        pattern: A string pattern with wildcare `*`. This character is supposed
                 to be the only special character.
        l: A list of string identifiers without special characters

    Returns:
        A list of string matching the pattern.
    """
    pattern = pattern.replace("*", ".*")
    m = re.compile(pattern, re.IGNORECASE)
    return [x for x in l if x is not None and m.fullmatch(x)]


class CaseInsensitiveDict(weakref.WeakValueDictionary):
    @staticmethod
    def _k(key):
        return key.lower() if isinstance(key, str) else key

    def __init__(self, *args, **kwargs):
        weakref.WeakValueDictionary.__init__(self)
        self.update(*args, **kwargs)

    def __getitem__(self, key):
        return weakref.WeakValueDictionary.__getitem__(self, self._k(key))

    def __setitem__(self, key, value):
        weakref.WeakValueDictionary.__setitem__(self, self._k(key), value)

    def __delitem__(self, key):
        return weakref.WeakValueDictionary.__delitem__(self, self._k(key))

    def __contains__(self, key):
        return weakref.WeakValueDictionary.__contains__(self, self._k(key))

    def pop(self, key, *args, **kwargs):
        return weakref.WeakValueDictionary.pop(self, self._k(key), *args, **kwargs)

    def get(self, key, *args, **kwargs):
        return weakref.WeakValueDictionary.get(self, self._k(key), *args, **kwargs)

    def setdefault(self, key, *args, **kwargs):
        return CaseInsensitiveDict.setdefault(self, self._k(key), *args, **kwargs)

    def update(self, arg={}, **kwargs):
        for key, value in dict(arg).items():
            self[key] = value
        if kwargs:
            self.update(kwargs)


class DataSource(abc.ABC):
    def __init__(self, personal_name):
        # Tango indexing
        self._strong_node_ref = set()
        self._personal_2_node = CaseInsensitiveDict()
        self._tango_name_2_node = CaseInsensitiveDict()
        self._class_name_2_node = CaseInsensitiveDict()

        self._init_db()
        self._parse_all()
        self._bootstrap_db(personal_name)

    @abc.abstractmethod
    def _init_db(self):
        ...

    def _bootstrap_db(self, personal_name):
        """Auto reference from the database to the database"""
        self._beacon_dserver_node = self.create_empty()
        self._beacon_dserver_node["server"] = "DataBaseds"
        self._beacon_dserver_node["personal_name"] = personal_name
        tango_name = "sys/database/%s" % personal_name
        database_device_node = self.create_empty(self._beacon_dserver_node)
        database_device_node["class"] = "DataBase"
        database_device_node["tango_name"] = tango_name
        self._beacon_dserver_node["device"] = [database_device_node]
        self._beacon_dserver_node["tango_name"] = tango_name
        self._tango_name_2_node[tango_name] = database_device_node
        tango_name_ds = "dserver/databaseds/%s" % personal_name
        self._tango_name_2_node[tango_name_ds] = self._beacon_dserver_node
        server_name = "DataBaseds/%s" % personal_name
        self._personal_2_node[server_name] = self._beacon_dserver_node

    def _parse_all(self):
        root = self._get_root_node()
        self._parse_node(root)

    def _parse_node(self, node):
        """
        Parse a dict dispatching it into the right parser kind.
        """
        if isinstance(node, MutableSequence):
            for n in node:
                self._parse_node(n)
        elif isinstance(node, MutableMapping):
            if "server" in node:
                self._parse_tango_server(node)
            elif "class" in node:
                self._parse_tango_class(node)
            else:
                _logger.error("Content unsupported. Found:\n%s", dict(node))

    def _parse_tango_class(self, node):
        """
        Parse a dictionary as a description of Tango device class

        This indexes the content to the data source.
        """
        class_name = node.get("class")
        self._class_name_2_node[class_name] = node

    def _parse_tango_server(self, node):
        """
        Parse a dictionary as Tango server description content.

        This indexes the content to the data source.
        """
        personal_name = node.get("personal_name")
        if personal_name is not None:
            personal_name = personal_name.lower()

        server = node.get("server")
        if server is None:
            _logger.error(
                "Tango server personal_name=%s didn't specify server key (executable name)",
                personal_name,
            )
            return

        dserver_name = "%s/%s" % (server, personal_name)
        self._personal_2_node[dserver_name] = node
        self._tango_name_2_node["dserver/%s" % dserver_name.lower()] = node
        self._strong_node_ref.add(node)

        devices = node.get("device")
        for device_info in devices:
            device_node = self.create_device(device_info, parent=node)
            if device_node is None:
                continue

            tango_name = device_node.get("tango_name")
            if tango_name is not None:
                tango_name = tango_name.lower()
                self._tango_name_2_node[tango_name] = device_node

            alias = device_node.get("alias")
            if alias is not None:
                self._tango_name_2_node[alias] = device_node

            self._strong_node_ref.add(device_node)

    def get_class_property(self, klass_name, prop_name):
        # key_name = 'tango.class.properties.%s.%s' % (klass_name,prop_name)
        # return settings.QueueSetting(key_name)
        node = self._class_name_2_node.get(klass_name, dict())
        properties = node.get("properties", dict())
        return properties.get(prop_name, None)

    def get_property_node(self, dev_name):
        dev_name = dev_name.lower()
        device_node = self._tango_name_2_node.get(dev_name)
        if device_node is None:
            return None

        properties = device_node.get("properties")
        if isinstance(properties, str):  # reference
            properties_key = properties.split("/")
            node_refname = properties_key[0]
            property_node = self.get_node(node_refname)
            if properties_key == node_refname:
                properties = property_node
            else:
                for key in properties_key[1:]:
                    property_node = property_node.get(key)
                    if property_node is None:
                        break
                properties = property_node
        return properties

    def get_class_name(self, class_name):
        class_node = self._class_name_2_node.get(class_name)
        if class_node is None:
            class_node = self.create_class_filename(class_name)
            class_node["class"] = class_name
            self._strong_node_ref.add(class_node)
            self._class_name_2_node[class_name] = class_node
        return class_node

    @property
    def strong_node_ref(self):
        return self._strong_node_ref

    @property
    def personal_2_node(self):
        return self._personal_2_node

    @property
    def tango_name_2_node(self):
        return self._tango_name_2_node

    @property
    def class_name_2_node(self):
        return self._class_name_2_node

    @abc.abstractmethod
    def _get_root_node(self):
        ...

    @abc.abstractmethod
    def create_empty(self, parent=None, path=None):
        ...

    @abc.abstractmethod
    def create_server_filename(self, server_name):
        ...

    @abc.abstractmethod
    def create_class_filename(self, class_name):
        ...

    @abc.abstractmethod
    def get_node(self, refname):
        ...

    @abc.abstractmethod
    def get_attr_alias_mapping(self):
        ...

    @abc.abstractmethod
    def create_device(self, device_info, parent=None):
        ...

    @abc.abstractmethod
    def get_class_attribute_list(self, class_name, wildcard):
        ...

    @abc.abstractmethod
    def get_class_attribute(self, klass_name, attr_name):
        ...

    @abc.abstractmethod
    def get_property_attr_device(self, dev_name):
        ...

    @abc.abstractmethod
    def get_exported_device_info(self, dev_name):
        ...

    @abc.abstractmethod
    def get_exported_devices_keys(self, key_filter):
        ...


class dbapi(abc.ABC):

    DB_API_NAME = "beacon"

    def __init__(self, personal_name="2", db_path="tango", **keys):
        self._source = self._create_data_source(personal_name, db_path)

    @abc.abstractmethod
    def _create_data_source(self, personal_name, db_access):
        ...

    # TANGO API
    @_debug
    def get_stored_procedure_release(self):
        return "release 0.0"

    @_debug
    def add_device(self, server_name, dev_info, klass_name, alias=None):
        tango_name, _ = dev_info
        tango_name = tango_name.lower()
        device_node = self._source.tango_name_2_node.get(tango_name)
        if device_node is not None:  # There is a problem?
            return
        server_exe_name, personal_name = server_name.split("/")
        personal_name = personal_name.lower()
        server_name = "%s/%s" % (server_exe_name, personal_name)
        server_node = self._source.personal_2_node.get(server_name)
        if server_node is None:
            server_node = self._source.create_server_filename(server_name)
            server_node["server"] = server_exe_name
            server_node["personal_name"] = personal_name
            self._source.personal_2_node[server_name] = server_node
            self._source.tango_name_2_node[
                "dserver/%s" % server_name.lower()
            ] = server_node
            self._source.strong_node_ref.add(server_node)

        device_node = self._source.create_empty(server_node)
        self._source.strong_node_ref.add(device_node)
        device_node["tango_name"] = tango_name
        device_node["class"] = klass_name
        if alias is not None:
            device_node["alias"] = alias
        device_node_list = server_node.get("device", [])
        device_node_list.append(device_node)
        server_node["device"] = device_node_list
        self._source.tango_name_2_node[tango_name] = device_node

        server_node.save()

    @_debug
    def delete_attribute_alias(self, alias):
        attr_alias = self._source.get_attr_alias_mapping()
        del attr_alias[alias]

    @_debug
    def delete_class_attribute(self, klass_name, attr_name):
        class_attribute = self._source.get_class_attribute(klass_name, attr_name)
        class_attribute.clear()

    @_debug
    def delete_class_attribute_property(self, klass_name, attr_name, prop_name):
        class_attribute = self._source.get_class_attribute(klass_name, attr_name)
        del class_attribute[prop_name]

    @_debug
    def delete_class_property(self, klass_name, prop_name):
        # TODO: not supported yet
        # class_property = self._source.get_class_property(klass_name,prop_name)
        # class_property.clear()
        pass

    @_debug
    def delete_device(self, dev_name):
        dev_name = dev_name.lower()

        device_node = self._source.tango_name_2_node.pop(dev_name, None)
        if device_node is None:
            return

        server_node = device_node.parent
        if server_node is None:  # weird
            return
        device_list = server_node.get("device", [])
        device_list.remove(device_node)
        server_node.save()

        prop_attr_device = self._source.get_property_attr_device(dev_name)
        prop_attr_device.clear()

    @_debug
    def delete_device_alias(self, dev_alias):
        device_node = self._source.tango_name_2_node.pop(dev_alias)
        if device_node is None:
            return

        server_node = device_node.parent
        if server_node is None:  # weird
            return
        del device_node["alias"]
        server_node.save()

    @_debug
    def delete_device_attribute(self, dev_name, attr_name):
        prop_attr_device = self._source.get_property_attr_device(dev_name)
        del prop_attr_device[attr_name]

    @_debug
    def delete_device_attribute_property(self, dev_name, attr_name, prop_name):
        prop_attr_device = self._source.get_property_attr_device(dev_name)
        d = prop_attr_device.get(attr_name)
        if d is not None:
            del d[prop_name]
            prop_attr_device[attr_name] = d

    @_debug
    def delete_device_property(self, dev_name, prop_name):
        properties = self._source.get_property_node(dev_name)
        if properties is None:
            return
        try:
            del properties[prop_name]
            properties.save()
        except KeyError:
            pass

    @_debug
    def delete_property(self, obj_name, prop_name):
        _logger.warning(
            "Not implemented delete_property(obj_name=%s, prop_name=%s)",
            obj_name,
            prop_name,
        )

    @_debug
    def delete_server(self, server_name):
        server_node = self._source.personal_2_node.get(server_name)
        if server_node is None:
            return

        server_node.clear()
        server_node.save()

    @_debug
    def delete_server_info(self, server_instance):
        _logger.warning(
            "Not implemented delete_server_info(server_instance=%s)", (server_instance)
        )

    @_debug
    def export_device(self, dev_name, IOR, host, pid, version):
        dev_name = dev_name.lower()
        device_node = self._source.tango_name_2_node.get(dev_name)
        if device_node is None:
            th_exc(
                db_errors.DB_DeviceNotDefined,
                "device " + dev_name + " not defined in the database!",
                "DataBase::ExportDevice()",
            )

        export_device_info = self._source.get_exported_device_info(dev_name)
        start_time = datetime.datetime.now()
        export_device_info.set(
            {
                "IOR": IOR,
                "host": host,
                "pid": int(pid),
                "version": version,
                "start-time": "%s" % start_time,
            }
        )

    @_debug
    def export_event(self, event, IOR, host, pid, version):
        _logger.warning(
            "Not implemented export_event(event=%s, IOR=%s, host=%s, pid=%s, version=%s)",
            event,
            IOR,
            host,
            pid,
            version,
        )

    @_debug
    def get_alias_device(self, dev_alias):
        device_node = self._source.tango_name_2_node.get(dev_alias)
        if device_node is None:
            th_exc(
                db_errors.DB_DeviceNotDefined,
                "No device found for alias '" + dev_alias + "'",
                "DataBase::GetAliasDevice()",
            )

        return device_node.get("tango_name")

    @_debug
    def get_attribute_alias(self, attr_alias_name):
        attr_alias = self._source.get_attr_alias_mapping()
        attr_alias_info = attr_alias.get(attr_alias_name)
        if attr_alias_info is None:
            th_exc(
                db_errors.DB_SQLError,
                "No attribute found for alias '" + attr_alias + "'",
                "DataBase::GetAttributeAlias()",
            )
        return attr_alias_info.get("name")

    @_debug
    def get_attribute_alias_list(self, attr_alias_name):
        attr_alias = self._source.get_attr_alias_mapping()
        attr_alias_info = attr_alias.get(attr_alias_name)
        if attr_alias_info is None:
            return []
        else:
            return [attr_alias_info.get("name")]

    @_debug
    def get_class_attribute_list(self, class_name, wildcard):
        return self._source.get_class_attribute_list(class_name, wildcard)

    @_debug
    def get_class_attribute_property(self, class_name, attributes):
        result = [class_name, str(len(attributes))]
        for att_name in attributes:
            class_attribute_properties = self._source.get_class_attribute(
                class_name, att_name
            )
            attr_property = [x for p in class_attribute_properties.items() for x in p]
            result.extend([att_name, str(len(attr_property) / 2)] + attr_property)
        return result

    @_debug
    def get_class_attribute_property2(self, class_name, attributes):
        result = [class_name, str(len(attributes))]
        for attr_name in attributes:
            class_properties = self._source.get_class_attribute(class_name, attr_name)
            attr_property = []
            for (name, values) in class_properties.items():
                if isinstance(values, MutableSequence):
                    attr_property.extend(
                        [name, str(len(values))] + [str(x) for x in values]
                    )
                else:
                    attr_property.extend([name, "1", str(values)])
            nb = len(class_properties.items())
            result.extend([attr_name, str(nb)] + attr_property)
        return result

    @_debug
    def get_class_attribute_property_hist(self, class_name, attribute, prop_name):
        # TODO: not supported yet
        return []

    @_debug
    def get_class_for_device(self, dev_name):
        dev_name = dev_name.lower()
        device_node = self._source.tango_name_2_node.get(dev_name)
        if device_node is None:
            th_exc(
                db_errors.DB_IncorrectArguments,
                "Device not found for " + dev_name,
                "Database.GetClassForDevice",
            )
        class_name = device_node.get("class")
        if class_name is None:
            th_exc(
                db_errors.DB_IncorrectArguments,
                "Class not found for " + dev_name,
                "Database.GetClassForDevice",
            )
        return class_name

    @_debug
    def get_class_inheritance_for_device(self, dev_name):
        class_name = self.get_class_for_device(dev_name)
        props = self.get_class_property(class_name, "InheritedFrom")
        return [class_name] + props[4:]

    @_debug
    def get_class_list(self, wildcard):
        server_names = list(self._source.personal_2_node.keys())
        result = self._get_device_classes(server_names)
        result = list_filter(wildcard, result)
        result.sort()
        return result

    @_debug
    def get_class_property(self, class_name, properties):
        result = [class_name, str(len(properties))]
        for prop_name in properties:
            properties_array = []
            values = self._source.get_class_property(class_name, prop_name)
            if values is None:
                properties_array.extend([prop_name, "0"])
            elif isinstance(values, MutableSequence):
                values = [str(x) for x in values]
                properties_array.extend([prop_name, str(len(values))] + values)
            else:
                properties_array.extend([prop_name, "1", str(values)])
            result.extend(properties_array)
        return result

    @_debug
    def get_class_property_hist(self, class_name, prop_name):
        # TODO: not supported yet
        return []

    @_debug
    def get_class_property_list(self, class_name):
        properties = self._source.class_name_2_node.get(class_name, dict()).get(
            "properties", dict()
        )
        return [k for k, v in properties.items() if not isinstance(v, MutableMapping)]

    @_debug
    def get_device_alias(self, dev_name):
        dev_name = dev_name.lower()
        device_node = self._source.tango_name_2_node.get(dev_name)
        if device_node is None:
            th_exc(
                db_errors.DB_DeviceNotDefined,
                "No alias found for device '" + dev_name + "'",
                "DataBase::GetDeviceAlias()",
            )
        alias = device_node.get("alias")
        if alias is None:
            th_exc(
                db_errors.DB_DeviceNotDefined,
                "No alias found for device '" + dev_name + "'",
                "DataBase::GetDeviceAlias()",
            )
        return alias

    @_debug
    def get_device_alias_list(self, alias):
        alias_list = [
            node.get("alias") for node in list(self._source.tango_name_2_node.values())
        ]
        return list_filter(alias, alias_list)

    @_debug
    def get_device_attribute_list(self, dev_name, attribute):
        prop_attr_device = self._source.get_property_attr_device(dev_name)
        return list_filter(attribute, list(prop_attr_device.keys()))

    @_debug
    def get_device_attribute_property(self, dev_name, attributes):
        prop_attr_device = self._source.get_property_attr_device(dev_name)
        result = [dev_name, str(len(attributes))]
        for attr_name in attributes:
            prop_attr = prop_attr_device.get(attr_name)
            if prop_attr is None:
                result.extend([attr_name, "0"])
            else:
                result.extend(
                    [attr_name, str(len(prop_attr))]
                    + [str(x) for p in prop_attr.items() for x in p]
                )
        return result

    @_debug
    def get_device_attribute_property2(self, dev_name, attributes):
        prop_attr_device_handler = self._source.get_property_attr_device(dev_name)
        result = [dev_name, str(len(attributes))]
        prop_attr_device = prop_attr_device_handler.get_all()
        for attr_name in attributes:
            prop_attr = prop_attr_device.get(attr_name)
            if prop_attr is None:
                result.extend((attr_name, "0"))
            else:
                result.extend((attr_name, str(len(prop_attr))))
                for name, values in prop_attr.items():
                    if isinstance(values, MutableSequence):
                        result.extend(
                            [name, str(len(values))] + [str(x) for x in values]
                        )
                    else:
                        result.extend((name, "1", str(values)))
        return result

    @_debug
    def get_device_attribute_property_hist(self, dev_name, attribute, prop_name):
        # TODO: not supported yet
        return []

    @_debug
    def get_device_class_list(self, server_name):
        server_node = self._source.personal_2_node.get(server_name)
        if server_node is None:
            return []
        devices = server_node.get("device")
        if isinstance(devices, MutableSequence):
            name_class = [(n.get("tango_name"), n.get("class")) for n in devices]
        else:
            name_class = [(devices.get("tango_name"), devices.get("class"))]

        return [x for p in name_class for x in p]

    @_debug
    def get_device_domain_list(self, wildcard):
        filtered_names = list_filter(
            wildcard,
            [
                n.get("tango_name")
                for n in list(self._source.tango_name_2_node.values())
            ],
        )
        res = list(set([x.split("/")[0] for x in filtered_names]))
        res.sort()
        return res

    @_debug
    def get_device_exported_list(self, wildcard):
        return self._source.get_exported_devices_keys(wildcard)

    @_debug
    def get_device_family_list(self, wildcard):
        filtered_names = list_filter(
            wildcard,
            [
                n.get("tango_name")
                for n in list(self._source.tango_name_2_node.values())
            ],
        )
        return list(set([x.split("/")[1] for x in filtered_names]))

    def get_device_info(self, dev_name):
        dev_name = dev_name.lower()
        device_info = self._source.get_exported_device_info(dev_name)
        device_node = self._source.tango_name_2_node.get(dev_name)
        result_long = []
        result_str = []

        info = device_info.get_all()
        if device_node:
            if dev_name.startswith("dserver"):
                server_node = device_node
            else:
                server_node = device_node.parent
            tango_name = (
                server_node.get("server", "")
                + "/"
                + server_node.get("personal_name", "")
            )
            result_str.extend(
                (
                    dev_name,
                    info.get("IOR", ""),
                    str(info.get("version", "0")),
                    tango_name,
                    info.get("host", "?"),
                    info.get("start-time", "?"),
                    "?",
                    device_node.get("class", "DServer"),
                )
            )
            result_long.extend((info and 1 or 0, info.get("pid", -1)))
        return (result_long, result_str)

    @_debug
    def get_device_list(self, server_name, class_name):
        if server_name == "*":
            r_list = list()
            for server_node in list(self._source.personal_2_node.values()):
                device_list = server_node.get("device")
                r_list.extend(self._get_tango_name_from_class(device_list, class_name))
            return r_list

        server_node = self._source.personal_2_node.get(server_name)
        if server_node is None:
            return []
        device_list = server_node.get("device")
        return self._get_tango_name_from_class(device_list, class_name)

    def _get_tango_name_from_class(self, device_list, class_name):
        m = re.compile(class_name.replace("*", ".*"), re.IGNORECASE)
        if isinstance(device_list, MutableSequence):
            return [
                x.get("tango_name") for x in device_list if m.match(x.get("class", ""))
            ]
        elif isinstance(device_list, MutableMapping) and m.match(
            device_list.get("class", "")
        ):
            return [device_list.get("tango_name")]
        else:
            return []

    @_debug
    def get_device_wide_list(self, wildcard):
        return list_filter(wildcard, list(self._source.tango_name_2_node.keys()))

    @_debug
    def get_device_member_list(self, wildcard):
        wildcard = wildcard.lower()
        filtered_names = list_filter(
            wildcard, list(self._source.tango_name_2_node.keys())
        )
        return list(set([x.split("/")[-1] for x in filtered_names]))

    @_debug
    def get_device_property(self, dev_name, properties_query_list):
        properties = self._source.get_property_node(dev_name)

        if properties is None:
            result = [dev_name, str(len(properties_query_list))]
            for p_name in properties_query_list:
                result.extend([p_name, "0", ""])
            return result
        else:
            nb_properties = 0
            properties_array = []
            properties_key = list(properties.keys())
            for property_name in properties_query_list:
                ask_keys = list_filter(property_name, properties_key)
                if not ask_keys:
                    properties_array.extend([property_name, "0", ""])
                    nb_properties += 1

                for key in ask_keys:
                    values = properties.get(key, "")
                    if isinstance(values, MutableSequence):
                        values = [str(x) for x in values]
                        properties_array.extend(
                            [property_name, str(len(values))] + values
                        )
                    else:
                        properties_array.extend([property_name, "1", str(values)])

                nb_properties += len(ask_keys)
            return [dev_name, str(nb_properties)] + properties_array

    @_debug
    def get_device_property_list(self, device_name, prop_filter):
        properties = self._source.get_property_node(device_name)
        if properties is None:
            return []
        else:
            return list_filter(
                prop_filter,
                [k for k, v in properties.items() if not isinstance(v, MutableMapping)],
            )

    @_debug
    def get_device_property_hist(self, device_name, prop_name):
        return []

    @_debug
    def get_device_server_class_list(self, server_name):
        server_name = server_name.lower()
        server_node = self._source.personal_2_node.get(server_name)
        if server_node is None:
            return []
        else:
            devices = server_node.get("device")
            if isinstance(devices, MutableSequence):
                return [x.get("class") for x in devices]
            else:
                return [devices.get("class")]

    @_debug
    def get_exported_device_list_for_class(self, wildcard):
        result = []
        exported_devices = self._source.get_exported_devices_keys("*")
        m = re.compile(wildcard.replace("*", ".*"), re.IGNORECASE)
        for dev_name in exported_devices:
            dev_node = self._source.tango_name_2_node.get(dev_name)
            if dev_node:
                dev_class_name = dev_node.get("class", "")
                if m.match(dev_class_name):
                    result.append(dev_name)
        return result

    @_debug
    def get_host_list(self, host_name):
        source = self._source
        host_list = {
            source.get_exported_device_info(key_name).get("host")
            for key_name in source.get_exported_devices_keys("*")
        }
        return list_filter(host_name, host_list)

    @_debug
    def get_host_server_list(self, host_name):
        source = self._source
        result = []
        wildcard = host_name.replace("*", ".*")
        m = re.compile(wildcard)
        exported_devices = self._source.get_exported_devices_keys("*")
        for dev_name in exported_devices:
            host = source.get_exported_device_info(dev_name).get("host")
            if not m.match(host):
                continue
            dev_node = self._source.tango_name_2_node.get(dev_name)
            if dev_node is None:
                continue
            if "server" in dev_node:
                continue
            server_node = dev_node.parent
            if server_node is None:
                continue
            server_name = server_node.get("server")
            server_instance = server_node.get("personal_name")
            if not server_name or not server_instance:
                continue
            result.append("%s/%s" % (server_name, server_instance))
        return result

    @_debug
    def get_host_servers_info(self, host_name):
        # Don't know what it is?
        return []

    @_debug
    def get_instance_name_list(self, server_name):
        server_list = self.get_server_list(server_name + "*")
        result = []
        for server in server_list:
            names = server.split("/")
            result.append(names[1])
        return result

    @_debug
    def get_object_list(self, name):
        return []

    @_debug
    def get_property(self, object_name, properties):
        result = [object_name, str(len(properties))]
        for prop in properties:
            result.extend([prop, "0", ""])
        return result

    @_debug
    def get_property_hist(self, object_name, prop_name):
        # TODO: not supported yet
        return []

    @_debug
    def get_property_list(self, object_name, wildcard):
        # TODO: not supported yet
        return []

    @_debug
    def get_server_info(self, server_name):
        # TODO: not supported yet
        return ["", "", ""]

    @_debug
    def get_server_list(self, wildcard):
        return list_filter(wildcard, list(self._source.personal_2_node.keys()))

    @_debug
    def get_server_name_list(self, wildcard):
        res = list(
            set(
                list_filter(
                    wildcard,
                    [
                        x.split("/")[0]
                        for x in list(self._source.personal_2_node.keys())
                    ],
                )
            )
        )
        res.sort()
        return res

    @_debug
    def get_server_class_list(self, wildcard):
        server_names = list_filter(wildcard, list(self._source.personal_2_node.keys()))
        result = self._get_device_classes(server_names)
        result = list(result)
        result.sort()
        return result

    def _get_device_classes(self, server_names):
        result = set()
        for ser_name in server_names:
            server_node = self._source.personal_2_node.get(ser_name)
            for device_node in server_node.get("device", []):
                class_name = device_node.get("class")
                if class_name is not None:
                    result.add(class_name)
        result.add("DServer")
        return result

    @_debug
    def import_device(self, dev_name):
        dev_node = self._source.tango_name_2_node.get(dev_name)
        if dev_node is not None:
            return self.get_device_info(dev_name)
        else:
            th_exc(
                db_errors.DB_DeviceNotDefined,
                "device " + dev_name + " not defined in the database!",
                "DataBase::ImportDevice()",
            )

    @_debug
    def import_event(self, event_name):
        th_exc(
            db_errors.DB_DeviceNotDefined,
            "event " + event_name + " not defined in the database!",
            "DataBase::ImportEvent()",
        )

    @_debug
    def info(self):
        return ["Beacon Beacon files"]

    @_debug
    def put_attribute_alias(self, attribute_name, attr_alias_name):
        attr_alias = self._source.get_attr_alias_mapping()
        attr_alias_info = attr_alias.get(attr_alias_name)
        if attr_alias_info is not None:
            th_exc(
                db_errors.DB_SQLError,
                "alias " + attr_alias_name + " already exists!",
                "DataBase::DbPutAttributeAlias()",
            )
        attr_alias[attr_alias_name] = attribute_name

    @_debug
    def put_class_attribute_property(self, class_name, nb_attributes, attr_prop_list):
        attr_id = 0
        for _ in range(nb_attributes):
            attr_name, nb_properties = (
                attr_prop_list[attr_id],
                int(attr_prop_list[attr_id + 1]),
            )
            attr_id += 2
            class_properties = self._source.get_class_attribute(class_name, attr_name)
            new_values = {}
            for prop_id in range(attr_id, attr_id + nb_properties * 2, 2):
                prop_name, prop_val = (
                    attr_prop_list[prop_id],
                    attr_prop_list[prop_id + 1],
                )
                new_values[prop_name] = prop_val
            attr_id += nb_properties * 2
            class_properties.set(new_values)

    @_debug
    def put_class_attribute_property2(self, class_name, nb_attributes, attr_prop_list):
        attr_id = 0
        for _ in range(nb_attributes):
            attr_name, nb_properties = (
                attr_prop_list[attr_id],
                int(attr_prop_list[attr_id + 1]),
            )
            attr_id += 2
            class_properties = self._source.get_class_attribute(class_name, attr_name)
            new_values = {}
            for _prop_id in range(nb_properties):
                prop_name, prop_number = (
                    attr_prop_list[attr_id],
                    int(attr_prop_list[attr_id + 1]),
                )
                attr_id += 2
                prop_values = []
                for _prop_sub_id in range(prop_number):
                    prop_values.append(attr_prop_list[attr_id])
                    attr_id += 1
                if len(prop_values) == 1:
                    prop_values = prop_values[0]
                new_values[prop_name] = prop_values
            class_properties.set(new_values)

    @_debug
    def put_class_property(self, class_name, nb_properties, attr_prop_list):
        attr_id = 0
        class_node = self._source.get_class_name(class_name)
        properties = class_node.get("properties", dict())
        for _ in range(nb_properties):
            prop_name, nb_values = (
                attr_prop_list[attr_id],
                int(attr_prop_list[attr_id + 1]),
            )
            attr_id += 2
            if nb_values == 1:
                properties[prop_name] = attr_prop_list[attr_id]
            else:
                properties[prop_name] = list(
                    attr_prop_list[attr_id : attr_id + nb_values]
                )
            attr_id += nb_values
        class_node["properties"] = properties
        class_node.save()

    @_debug
    def put_device_alias(self, device_name, device_alias):
        device_node = self._source.tango_name_2_node.get(device_name)
        device_node["alias"] = device_alias
        device_node.save()

    @_debug
    def put_device_attribute_property(self, device_name, nb_attributes, attr_prop_list):
        attr_id = 0
        prop_attr_device = self._source.get_property_attr_device(device_name)
        for _ in range(nb_attributes):
            attr_name, prop_nb = (
                attr_prop_list[attr_id],
                int(attr_prop_list[attr_id + 1]),
            )
            attr_id += 2
            new_values = {}
            for prop_id in range(attr_id, attr_id + prop_nb * 2, 2):
                prop_name, prop_val = (
                    attr_prop_list[prop_id],
                    attr_prop_list[prop_id + 1],
                )
                new_values[prop_name] = prop_val
            prop_attr_device[attr_name] = new_values
            attr_id += prop_nb * 2

    @_debug
    def put_device_attribute_property2(
        self, device_name, nb_attributes, attr_prop_list
    ):
        attr_id = 0
        prop_attr_device = self._source.get_property_attr_device(device_name)
        for _ in range(nb_attributes):
            attr_name, prop_nb = (
                attr_prop_list[attr_id],
                int(attr_prop_list[attr_id + 1]),
            )
            attr_id += 2
            new_values = {}
            for _prop_id in range(prop_nb):
                prop_name, prop_nb = (
                    attr_prop_list[attr_id],
                    int(attr_prop_list[attr_id + 1]),
                )
                attr_id += 2
                prop_values = []
                for _prop_sub_id in range(prop_nb):
                    prop_values.append(attr_prop_list[attr_id])
                    attr_id += 1
                if len(prop_values) == 1:
                    prop_values = prop_values[0]
                new_values[prop_name] = prop_values
            prop_attr_device[attr_name] = new_values

    @_debug
    def put_device_property(self, device_name, nb_properties, attr_prop_list):
        device_name = device_name.lower()
        device_node = self._source.tango_name_2_node.get(device_name)
        old_properties = device_node.get("properties")
        if isinstance(old_properties, str):  # reference
            properties_key = old_properties.split("/")
            node_refname = properties_key[0]
            property_node = self._source.get_node(node_refname)
            if properties_key == node_refname:
                old_properties = property_node
            else:
                for key in properties_key[1:]:
                    property_node = property_node.get(key)
                    if property_node is None:
                        break
                old_properties = property_node
        if old_properties is None:
            properties = self._source.create_empty(device_node, path=["properties"])
            device_node["properties"] = properties
            device_node.save()
        else:
            properties = old_properties

        id_prop = 0
        for _ in range(nb_properties):
            prop_name, prop_nb_values = (
                attr_prop_list[id_prop],
                int(attr_prop_list[id_prop + 1]),
            )
            id_prop += 2
            if prop_nb_values == 1:
                properties[prop_name] = attr_prop_list[id_prop]
            else:
                properties[prop_name] = attr_prop_list[
                    id_prop : id_prop + prop_nb_values
                ]
            id_prop += prop_nb_values
        properties.save()

    @_debug
    def put_property(self, object_name, nb_properties, attr_prop_list):
        # Not use in our case
        pass

    @_debug
    def put_server_info(self, tmp_server, tmp_host, tmp_mode, tmp_level, tmp_extra):
        # Not use in our case
        pass

    @_debug
    def unexport_device(self, dev_name):
        device_info = self._source.get_exported_device_info(dev_name)
        device_info.clear()

    @_debug
    def unexport_event(self, event_name):
        # Not use in our case
        pass

    @_debug
    def unexport_server(self, server_name):
        server_node = self._source.personal_2_node.get(server_name)
        if server_node is None:
            return

        for device in server_node.get("device"):
            tango_name = device.get("tango_name")
            if tango_name:
                self.unexport_device(tango_name)

    @_debug
    def delete_all_device_attribute_property(self, dev_name, attr_list):
        prop_attr_device = self._source.get_property_attr_device(dev_name)
        for attr_name in attr_list:
            del prop_attr_device[attr_name]

    @_debug
    def my_sql_select(self, cmd):
        # TODO: see if it's really needed
        _logger.error("my_sql_select is not available. Called with: %s", cmd)
        return ([0, 0], [])

    @_debug
    def get_csdb_server_list(self):
        source = self._source
        exported_devices = source.get_exported_devices_keys("sys/database*")
        result = []
        for dev_name in exported_devices:
            info = source.get_exported_device_info(dev_name)
            result.append(info.get("IOR"))
        return result

    @_debug
    def get_attribute_alias2(self, attr_name):
        attr_alias = self._source.get_attr_alias_mapping()
        result = []
        for alias, name in attr_alias.items():
            if name == attr_name:
                result.append(alias)
        return result

    @_debug
    def get_alias_attribute(self, alias_name):
        attr_alias = self._source.get_attr_alias_mapping()
        attr_name = attr_alias.get(alias_name)
        return attr_name and [attr_name] or []

    @_debug
    def rename_server(self, old_name, new_name):
        device_node = self._source.tango_name_2_node.get(new_name)
        if device_node is not None:
            th_exc(
                db_errors.DB_SQLError,
                "Device server process name " + new_name + "is already used!",
                "DataBase::DbRenameServer()",
            )
        device_node = self._source.tango_name_2_node.pop(old_name)
        device_node["tango_name"] = new_name
        self._source.tango_name_2_node[new_name] = device_node
        device_node.save()

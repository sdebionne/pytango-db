"""Provide a context to run a device with a database."""

# from __future__ import absolute_import

# Imports
import os
import sys
import time
import struct
import socket
import traceback
import collections
from functools import partial

# Concurrency imports
import threading
import multiprocessing
import queue

# CLI imports
from ast import literal_eval
from importlib import import_module
from argparse import ArgumentTypeError

# Local imports
from tango.server import run
from tango.utils import is_non_str_seq
from tango import DeviceProxy, Util

__all__ = ("MultiDeviceTestContext")

# Helpers

IOR = collections.namedtuple(
    "IOR",
    "first dtype_length dtype nb_profile tag "
    "length major minor wtf host_length host port body",
)


def ascii_to_bytes(s):
    convert = lambda x: bytes((int(x, 16),))
    return b"".join(convert(s[i : i + 2]) for i in range(0, len(s), 2))


def parse_ior(encoded_ior):
    assert encoded_ior[:4] == "IOR:"
    ior = ascii_to_bytes(encoded_ior[4:])
    dtype_length = struct.unpack_from("II", ior)[-1]
    form = "II{:d}sIIIBBHI".format(dtype_length)
    host_length = struct.unpack_from(form, ior)[-1]
    form = "II{:d}sIIIBBHI{:d}sH0I".format(dtype_length, host_length)
    values = struct.unpack_from(form, ior)
    values += (ior[struct.calcsize(form) :],)
    strip = lambda x: x[:-1] if isinstance(x, bytes) else x
    return IOR(*map(strip, values))


def get_server_host_port():
    util = Util.instance()
    ds = util.get_dserver_device()
    encoded_ior = util.get_dserver_ior(ds)
    ior = parse_ior(encoded_ior)
    return ior.host.decode(), ior.port


def literal_dict(arg):
    return dict(literal_eval(arg))


def device(path):
    """Get the device class from a given module."""
    module_name, device_name = path.rsplit(".", 1)
    try:
        module = import_module(module_name)
    except Exception:
        raise ArgumentTypeError(
            "Error importing {0}.{1}:\n{2}".format(
                module_name, device_name, traceback.format_exc()
            )
        )
    return getattr(module, device_name)


def get_host_ip():
    """Get the primary external host IP.

    This is useful because an explicit IP is required to get
    tango events to work properly. Note that localhost does not work
    either.
    """
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    # Connecting to a UDP address doesn't send packets
    s.connect(("8.8.8.8", 0))
    # Get ip address
    ip = s.getsockname()[0]
    return ip


def _device_class_from_field(field):
    """
    Helper function that extracts and return a device class from a
    'class' field of a devices_info dictionary

    :param field: the field from which to extract the device class

    :return: the device class extracted from the field
    """
    device_cls_class = None
    if is_non_str_seq(field):
        (device_cls_class, device_class) = (field[0], field[1])
        if isinstance(device_cls_class, str):
            device_cls_class = device(device_cls_class)
    else:
        device_class = field
    if isinstance(device_class, str):
        device_class = device(device_class)
    return (device_cls_class, device_class)


class MultiDeviceTestContext(object):
    """Context to run device(s) with a database.

    The difference with respect to
    :class:`~tango.test_context.DeviceTestContext` is that it allows
    to export multiple devices (even of different Tango classes).

    Example usage::

        from tango.server import Device, attribute
        from tango.test_context import MultiDeviceTestContext


        class Device1(Device):

            @attribute
            def attr1(self):
                return 1.0


        class Device2(Device):

            @attribute
            def read_attr2(self):
                return 2.0


        devices_info = (
            {
                "class": Device1,
                "devices": [
                    {
                        "name": "test/device1/1"
                    },
                ]
            },
            {
                "class": Device2,
                "devices": [
                    {
                        "name": "test/device2/1",
                    },
                ]
            }
        )

        def test_devices():
            with MultiDeviceTestContext(devices_info, process=True) as context:
                proxy1 = context.get_device("test/device1/1")
                proxy2 = context.get_device("test/device2/1")
                assert proxy1.attr1 == 1.0
                assert proxy2.attr2 == 2.0

    :param devices_info:
      a sequence of dicts with information about
      devices to be exported. Each dict consists of the following keys:

        * "class" which value is either of:

          * :class:`~tango.server.Device` or the name of some such class
          * a sequence of two elements, the first element being a
            :class:`~tango.DeviceClass` or the name of some such class,
            the second element being a :class:`~tango.DeviceImpl` or the
            name of such such class

        * "devices" which value is a sequence of dicts with the following keys:

          * "name" (str)
          * "properties" (dict)
          * "memorized" (dict)

    :type devices_info:
      sequence<dict>
    :param server_name:
      Name to use for the device server.
      Optional.  Default is the first device's class name.
    :type server_name:
      :py:obj:`str`
    :param instance_name:
      Name to use for the device server instance.
      Optional.  Default is lower-case version of the server name.
    :type instance_name:
      :py:obj:`str`
    :param db:
      Path to a pre-populated text file to use for the
      database.
      Optional.  Default is to create a new temporary file and populate it
      based on the devices and properties supplied in `devices_info`.
    :type db:
      :py:obj:`str`
    :param host:
      Hostname to use for device server's ORB endpoint.
      Optional.  Default is a local IP address.
    :type host:
      :py:obj:`str`
    :param port:
      Port number to use for the device server's ORB endpoint.
      Optional.  Default is chosen by omniORB.
    :type port:
      :py:obj:`int`
    :param debug:
      Debug level for the device server logging.
      0=OFF, 1=FATAL, 2=ERROR, 3=WARN, 4=INFO, 5=DEBUG.
      Optional. Default is warn.
    :type debug:
      :py:obj:`int`
    :param process:
      True if the device server should be launched in a new process, otherwise
      use a new thread.  Note:  if the context will be used mutiple times, it
      may seg fault if the thread mode is chosen.
      Optional.  Default is thread.
    :type process:
      :py:obj:`bool`
    :param daemon:
      True if the new thread/process must be created in daemon mode.
      Optional.  Default is not daemon.
    :type daemon:
      :py:obj:`bool`
    :param timeout:
      How long to wait (seconds) for the device server to start up, and also
      how long to wait on joining the thread/process when stopping.
      Optional.  Default differs for thread and process modes.
    :type timeout:
      :py:obj:`float`
    :param green_mode:
      Green mode to use for the device server.
      Optional.  Default uses the Device specification (via green_mode class attribute),
      or if that isn't specified the global green mode.
    :type green_mode:
      :obj:`~tango.GreenMode`
    """

    dbase = "dbase=yes"
    command = "{0} {1} -ORBendPoint giop:tcp:{2}:{3}"

    thread_timeout = 3.0
    process_timeout = 5.0

    def __init__(
        self,
        devices_info,
        server_name=None,
        instance_name=None,
        db=None,
        host=None,
        port=0,
        debug=3,
        process=False,
        daemon=False,
        timeout=None,
        green_mode=None,
    ):
        if not server_name:
            _, first_device = _device_class_from_field(devices_info[0]["class"])
            server_name = first_device.__name__
        if not instance_name:
            instance_name = server_name.lower()
        # if db is None:
        #     handle, db = tempfile.mkstemp()
        #     self.handle = handle
        # else:
        #     self.handle = None
        if host is None:
            # IP address is used instead of the hostname on purpose (see #246)
            host = get_host_ip()
        if timeout is None:
            timeout = self.process_timeout if process else self.thread_timeout
        # Patch bug #819
        if process:
            os.environ["ORBscanGranularity"] = "0"
        # Attributes
        self.db = db
        self.host = host
        self.port = port
        self.timeout = timeout
        self.server_name = "/".join(("dserver", server_name, instance_name))
        self.queue = multiprocessing.Queue() if process else queue.Queue()
        self._devices = {}

        # Command args
        string = self.command.format(server_name, instance_name, host, port)
        string += " -v{0}".format(debug) if debug else ""
        cmd_args = string.split()

        class_list = []
        device_list = []
        tangoclass_list = []
        for device_info in devices_info:
            device_cls, device = _device_class_from_field(device_info["class"])
            tangoclass = device.__name__
            if tangoclass in tangoclass_list:
                # self.delete_db()
                raise ValueError(
                    "multiple entries in devices_info pointing "
                    "to the same Tango class"
                )
            tangoclass_list.append(tangoclass)
            # # File
            # self.append_db_file(
            #     server_name, instance_name, tangoclass, device_info["devices"]
            # )
            if device_cls:
                class_list.append((device_cls, device, tangoclass))
            else:
                device_list.append(device)

        # Target and arguments
        if class_list and device_list:
            # self.delete_db()
            raise ValueError(
                "mixing HLAPI and classical API in devices_info " "is not supported"
            )
        if class_list:
            runserver = partial(run, class_list, cmd_args, green_mode=green_mode)
        elif len(device_list) == 1 and hasattr(device_list[0], "run_server"):
            runserver = partial(device.run_server, cmd_args, green_mode=green_mode)
        elif device_list:
            runserver = partial(run, device_list, cmd_args, green_mode=green_mode)
        else:
            raise ValueError("Wrong format of devices_info")

        cls = multiprocessing.Process if process else threading.Thread
        self.thread = cls(target=self.target, args=(runserver, process))
        self.thread.daemon = daemon

    def target(self, runserver, process=False):
        try:
            runserver(post_init_callback=self.post_init, raises=True)
        except Exception:
            # Put exception in the queue
            etype, value, tb = sys.exc_info()
            if process:
                tb = None  # Traceback objects can't be pickled
            self.queue.put((etype, value, tb))
        finally:
            # Put something in the queue just in case
            exc = RuntimeError("The server failed to report anything")
            self.queue.put((None, exc, None))
            # Make sure the process has enough time to send the items
            # because the it might segfault while cleaning up the
            # the tango resources
            if process:
                time.sleep(0.1)

    def post_init(self):
        try:
            host, port = get_server_host_port()
            self.queue.put((host, port))
        except Exception as exc:
            self.queue.put((None, exc, None))
        finally:
            # Put something in the queue just in case
            exc = RuntimeError("The post_init routine failed to report anything")
            self.queue.put((None, exc, None))

    # def append_db_file(self, server, instance, tangoclass, device_prop_info):
    #     """Generate a database file corresponding to the given arguments."""
    #     device_names = [info["name"] for info in device_prop_info]
    #     # Open the file
    #     with open(self.db, "a") as f:
    #         f.write("/".join((server, instance, "DEVICE", tangoclass)))
    #         f.write(": ")
    #         f.write(", ".join(device_names))
    #         f.write("\n")
    #     # Create database
    #     db = Database(self.db)
    #     # Write properties
    #     for info in device_prop_info:
    #         device_name = info["name"]
    #         properties = info.get("properties", {})
    #         # Patch the property dict to avoid a PyTango bug
    #         patched = dict(
    #             (key, value if value != "" else " ")
    #             for key, value in properties.items()
    #         )
    #         db.put_device_property(device_name, patched)

    #         memorized = info.get("memorized", {})
    #         munged = {
    #             attribute_name: {"__value": memorized_value}
    #             for (attribute_name, memorized_value) in memorized.items()
    #         }
    #         db.put_device_attribute_property(device_name, munged)
    #     return db

    # def delete_db(self):
    #     """delete temporary database file only if it was created by this class"""
    #     if self.handle is not None:
    #         os.close(self.handle)
    #         os.unlink(self.db)

    def get_server_access(self):
        """Return the full server name."""
        # form = "tango://{0}:{1}/{2}#{3}"
        # return form.format(self.host, self.port, self.server_name, self.dbase)
        return self.server_name

    def get_device_access(self, device_name):
        """Return the full device name."""
        # form = "tango://{0}:{1}/{2}#{3}"
        # return form.format(self.host, self.port, device_name, self.dbase)
        return device_name

    def get_device(self, device_name):
        """Return the device proxy corresponding to the given device name.

        Maintains previously accessed device proxies in a cache to not recreate
        then on every access.
        """
        if device_name not in self._devices:
            device = DeviceProxy(self.get_device_access(device_name))
            self._devices[device_name] = device
        return self._devices[device_name]

    def start(self):
        """Run the server."""
        self.thread.start()
        self.connect()
        return self

    def connect(self):
        try:
            args = self.queue.get(timeout=self.timeout)
        except queue.Empty:
            if self.thread.is_alive():
                raise RuntimeError(
                    "The server appears to be stuck at initialization. "
                    "Check stdout/stderr for more information."
                )
            elif hasattr(self.thread, "exitcode"):
                raise RuntimeError(
                    "The server process stopped with exitcode {}. "
                    "Check stdout/stderr for more information."
                    "".format(self.thread.exitcode)
                )
            else:
                raise RuntimeError(
                    "The server stopped without reporting. "
                    "Check stdout/stderr for more information."
                )
        try:
            self.host, self.port = args
        except ValueError as e:
            raise RuntimeError(*args) from e
        # Get server proxy
        self.server = DeviceProxy(self.get_server_access())
        self.server.ping()

    def stop(self):
        """Kill the server."""
        # try:
        if self.server:
            self.server.command_inout("Kill")
        self.join(self.timeout)
        # finally:
        #     self.delete_db()

    def join(self, timeout=None):
        self.thread.join(timeout)

    def __enter__(self):
        """Enter method for context support.

        :return:
          Instance of this test context.  Use `get_device` to get proxy
          access to any of the devices started by this context.
        :rtype:
          :class:`~tango.test_context.MultiDeviceTestContext`
        """
        if not self.thread.is_alive():
            self.start()
        return self

    def __exit__(self, exc_type, exception, trace):
        """Exit method for context support."""
        self.stop()
        return False

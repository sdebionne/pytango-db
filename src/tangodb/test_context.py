import os
import sys
import time
from six import reraise
from argparse import Namespace
from functools import partial

import multiprocessing
import queue

from tango import DeviceProxy
from tango.test_context import get_server_host_port
from tangodb.database import main_run


class DataBaseContext:

    timeout = 5.0

    def __init__(
        self,
        name: str = "2",
        port: int = None,
        db_access: str = "yaml",
        db_path: str = "tango_database",
        logging_level: int = 0,
        embedded: bool = False,
    ):
        options = Namespace()
        options.port = port
        options.db_access = db_access
        options.db_path = db_path
        options.logging_level = logging_level
        options.embedded = embedded
        options.argv = ["DataBaseds", name]

        self.options = options
        self.db_name = "sys/database/" + name
        self.server_name = "/".join(("dserver", "DataBaseds", name))

        runserver = partial(main_run, options, self.post_init)

        # Patch bug #819
        os.environ["ORBscanGranularity"] = "0"

        self.queue = multiprocessing.Queue()
        self.thread = multiprocessing.Process(target=self.target, args=(runserver,))
        # self.thread.daemon = False

    def target(self, runserver, process=False):
        try:
            runserver()
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

    def get_server_access(self):
        """Return the full server name."""
        form = "tango://{0}:{1}/{2}"
        return form.format(self.host, self.port, self.server_name)

    def get_device(self):
        os.environ["TANGO_HOST"] = f"localhost:{self.port}"
        return DeviceProxy(self.db_name)

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
        except ValueError:
            reraise(*args)
        # Get server proxy
        self.server = DeviceProxy(self.get_server_access())
        self.server.ping()

    def stop(self):
        """Kill the server."""
        if self.server:
            self.server.command_inout("Kill")
        self.join(self.timeout)

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

import os
import pytest

from tango import DevState
from tango.server import Device
from tango.server import attribute, device_property

from tangodb.test_context import DataBaseContext
from tangodb.test_context_device import MultiDeviceTestContext


class Device1(Device):

    _prop1 = device_property(dtype=str)

    def init_device(self):
        super(Device, self).init_device()
        self.set_state(DevState.ON)
        self._attr1 = 100

    @attribute
    def attr1(self):
        return self._attr1

    @attribute(dtype=str)
    def prop1(self):
        return self._prop1


class Device2(Device):
    def init_device(self):
        super(Device, self).init_device()
        self.set_state(DevState.ON)
        self._attr1 = 200

    @attribute
    def attr1(self):
        return self._attr1


@pytest.fixture(scope="module")
def database():
    with DataBaseContext(db_access="yaml", db_path="tests/tango_database") as context:
        os.environ["TANGO_HOST"] = f"{context.host}:{context.port}"
        yield context


def test_database_context(database):
    db = database.get_device()
    db.ping()

    exported_device_list = db.DbGetDeviceExportedList("*")

    assert "dserver/databaseds/2" in exported_device_list
    assert "sys/database/2" in exported_device_list


# @pytest.fixture(scope="module")
@pytest.fixture
def server(database):
    devices_info = (
        {"class": Device1, "devices": [{"name": "test/device1/1"}]},
        {"class": Device2, "devices": [{"name": "test/device2/1"}]},
    )

    with MultiDeviceTestContext(
        devices_info, server_name="test", instance_name="test1", process=True
    ) as context:
        yield context


def test_multi_with_two_devices(server):
    proxy1 = server.get_device("test/device1/1")
    proxy2 = server.get_device("test/device2/1")
    assert proxy1.State() == DevState.ON
    assert proxy2.State() == DevState.ON
    assert proxy1.attr1 == 100
    assert proxy1.prop1 == "Hello, world!"
    assert proxy2.attr1 == 200

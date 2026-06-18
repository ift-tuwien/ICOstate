*****
Usage
*****

.. currentmodule:: icostate

The main class for communication in the stateful API provided by this package is :class:`ICOsystem`. Depending on the `current state <#state-diagram>`_  of objects of this class you will be able to use different coroutines to interact with the system.

State Diagram
#############

.. mermaid::

   stateDiagram-v2
       disconnected: Disconnected
       stu_connected: STU Connected
       sensor_node_connected: Sensor Node Connected
       measurement: Measurement

       disconnected --> stu_connected: connect_stu

       stu_connected --> disconnected: disconnect_stu
       stu_connected --> stu_connected: get_adc_configuration, rename, reset_stu, set_adc_configuration
       stu_connected --> sensor_node_connected: connect_sensor_node_mac

       sensor_node_connected --> stu_connected: disconnect_sensor_node
       sensor_node_connected --> sensor_node_connected: get_adc_configuration, get_sensor_configuration, rename, set_adc_configuration, set_sensor_configuration
       sensor_node_connected --> measurement: start_measurement

       measurement --> sensor_node_connected: stop_measurement


In addition to coroutines that label the edges of the `state diagram <#state-diagram>`_ above you can also:

- Use the coroutine :meth:`ICOsystem.is_sensor_node_connected`, which works in any state.

- Use the coroutines:

  - :meth:`ICOsystem.get_stu_mac_address` or
  - :meth:`ICOsystem.collect_sensor_nodes`.

  which works in all states **except** :attr:`State.Disconnected`.

STU
###

Connecting to STU
*****************

Before you work with the ICOtronic system you need to set up the CAN connection to the STU (Stationary Transceiver Unit), which you can do using the coroutine :meth:`ICOsystem.connect_stu`. After you are done working working with the STU you also need to disconnect the connection using the coroutine :meth:`ICOsystem.disconnect_stu`.

.. doctest::

   >>> from asyncio import run
   >>> from sys import stderr
   >>> from icostate import ICOsystem

   >>> async def connect_disconnect_stu(icosystem: ICOsystem):
   ...     try:
   ...        await icosystem.connect_stu()
   ...     except CANInitError as error:
   ...        print(f"Unable to connect to ICOtronic system: {error}", file=stderr)
   ...     finally:
   ...        await icosystem.disconnect_stu()

   >>> run(connect_disconnect_stu(ICOsystem()))

Resetting STU
*************

In case you have the ICOtronic system does not react as you expect you can reset it using the coroutine :meth:`ICOsystem.reset_stu`.

.. doctest::

   >>> from asyncio import run
   >>> from sys import stderr
   >>> from icostate import ICOsystem

   >>> async def reset_stu(icosystem: ICOsystem):
   ...     try:
   ...        await icosystem.connect_stu()
   ...        await icosystem.reset_stu()
   ...     except CANInitError as error:
   ...        print(f"Error while resetting STU: {error}", file=stderr)
   ...     finally:
   ...        await icosystem.disconnect_stu()

   >>> run(reset_stu(ICOsystem()))

Finding Available Sensor Nodes
******************************

To retrieve information about available sensor nodes use the coroutine :meth:`ICOsystem.collect_sensor_nodes`.

.. doctest::

   >>> from asyncio import run
   >>> from sys import stderr
   >>> from netaddr import EUI
   >>> from icostate import ICOsystem, SensorNodeInfo

   >>> async def get_sensor_nodes(icosystem: ICOsystem) -> list[SensorNodeInfo]:
   ...     try:
   ...        await icosystem.connect_stu()
   ...        sensor_nodes = await icosystem.collect_sensor_nodes()
   ...     except CANInitError as error:
   ...        print(f"Unable to connect to ICOtronic system: {error}", file=stderr)
   ...     finally:
   ...        await icosystem.disconnect_stu()
   ...     return sensor_nodes

   >>> sensor_nodes = run(get_sensor_nodes(ICOsystem()))
   >>> # We assume that at least one sensor node is available
   >>> len(sensor_nodes) >= 1
   True
   >>> node_info = sensor_nodes[0]
   >>> # Each list entry contains information about name, MAC address and RSSI
   >>> isinstance(node_info.name, str)
   True
   >>> len(node_info.name) <= 8
   True
   >>> isinstance(node_info.mac_address, EUI)
   True
   >>> -100 < node_info.rssi < 0
   True

Sensor Node
###########

Connecting to Sensor Node
*************************

Before you start a measurement you need to connect to a sensor node. To do that use the coroutine :meth:`ICOsystem.connect_sensor_node_mac`. Please do not forget to disconnect from the node with the coroutine :meth:`ICOsystem.disconnect_sensor_node` afterwards.

.. doctest::

   >>> from asyncio import run
   >>> from sys import stderr
   >>> from icostate import ICOsystem
   >>> from icostate.config import settings

   >>> async def connect_disconnect_sensor_node(icosystem: ICOsystem,
   ...                                          mac_address: str):
   ...     try:
   ...         await icosystem.connect_stu()
   ...         print("Sensor node connected after STU connection: "
   ...               f"{await icosystem.is_sensor_node_connected()}")
   ...         try:
   ...             await icosystem.connect_sensor_node_mac(mac_address)
   ...             print("Sensor node connected after sensor node connection: "
   ...                   f"{await icosystem.is_sensor_node_connected()}")
   ...         except CANConnectionError as error:
   ...             print(f"Unable to connect to sensor node: {error}", file=stderr)
   ...         finally:
   ...             await icosystem.disconnect_sensor_node()
   ...         print("Sensor node connected after sensor node disconnection: "
   ...               f"{await icosystem.is_sensor_node_connected()}")
   ...     except CANInitError as error:
   ...         print(f"Unable to connect to ICOtronic system: {error}", file=stderr)
   ...     finally:
   ...         await icosystem.disconnect_stu()

   >>> mac_address = settings.sensor_node.eui # Change to MAC address of your sensor node
   >>> run(connect_disconnect_sensor_node(ICOsystem(), mac_address))
   Sensor node connected after STU connection: False
   Sensor node connected after sensor node connection: True
   Sensor node connected after sensor node disconnection: False

Rename a Sensor Node
********************

To rename a sensor node use the coroutine :meth:`ICOsystem.rename`, which requires the new name of the sensor node as parameter. If the system is currently not connected to a sensor node, then the coroutine also requires the MAC address of the sensor node. After using the coroutine successfully the system will switch back to the state it was in before renaming the sensor node (either “STU Connected” or “Sensor Node Connected”).

.. doctest::

   >>> from asyncio import run
   >>> from sys import stderr
   >>> from icostate import ICOsystem
   >>> from icostate.config import settings

   >>> async def rename_disconnected(icosystem: ICOsystem,
   ...                               mac_address: str,
   ...                               new_name: str):
   ...     try:
   ...         await icosystem.connect_stu()
   ...         print(f"State Before: {icosystem.state}")
   ...         await icosystem.rename(new_name, mac_address)
   ...         print(f"State After: {icosystem.state}")
   ...     except CANInitError as error:
   ...         print(f"Unable to connect to ICOtronic system: {error}", file=stderr)
   ...     finally:
   ...         await icosystem.disconnect_stu()

   >>> mac_address = settings.sensor_node.eui # Change to MAC address of your sensor node
   >>> name = "Test-STH"
   >>> run(rename_disconnected(ICOsystem(), mac_address, name))
   State Before: STU Connected
   State After: STU Connected

Events
######

Objects of the :class:`ICOsystem` provides an event based API (based on `pyee`_) you can use to react to changes to the system. Currently the following events are supported:

- ``sensor_node_name``: Called when the name of the current sensor node changes
- ``sensor_node_mac_address``: Called when the MAC address of a sensor node changes
- ``sensor_node_adc_configuration``: Called when the ADC configuration of a sensor node is updated
- ``sensor_node_measurement_data``: Called when new streaming data is available

.. _pyee: https://pyee.readthedocs.io

The example below shows how you can react to changes of the sensor node name:

.. doctest::

   >>> from asyncio import sleep, run
   >>> from sys import stderr
   >>> from icostate import ICOsystem
   >>> from icostate.config import settings

   >>> async def react_sensor_node_name(icosystem: ICOsystem, mac_address: str):
   ...
   ...     @icosystem.on("sensor_node_name")
   ...     async def name_changed(name: str):
   ...         print(f"Name of sensor node: {name}")
   ...
   ...     try:
   ...         await icosystem.connect_stu()
   ...         try:
   ...             await icosystem.connect_sensor_node_mac(mac_address)
   ...             await sleep(0)  # Allow scheduler to trigger event coroutine
   ...         except CANConnectionError as error:
   ...             print(f"Unable to connect to sensor node: {error}", file=stderr)
   ...         finally:
   ...             await icosystem.disconnect_sensor_node()
   ...     except CANInitError as error:
   ...         print(f"Unable to connect to ICOtronic system: {error}", file=stderr)
   ...     finally:
   ...         await icosystem.disconnect_stu()

   >>> mac_address = settings.sensor_node.eui # Change to MAC address of your sensor node
   >>> run(react_sensor_node_name(ICOsystem(), mac_address))
   Name of sensor node: Test-STH


Measurement (Streaming)
#######################

To start a measurement use the function `start_measurement` and provide a :class:`StreamingConfiguration`, which specifies the measurement channels that should be active. To retrieve the measurement data (objects of the class :class:`MeasurementData`) use the `pyee`_ event ``sensor_node_measurement_data``. The example below shows you how to do that:

.. doctest::

   >>> from asyncio import run, sleep
   >>> from sys import stderr
   >>> from time import time
   >>> from icostate import ICOsystem, MeasurementData, StreamingConfiguration
   >>> from icostate.config import settings

   >>> async def measure_data(icosystem: ICOsystem, mac_address: str) -> None:
   ...     try:
   ...         await icosystem.connect_stu()
   ...         try:
   ...             await icosystem.connect_sensor_node_mac(mac_address)
   ...
   ...             data = None
   ...             measurement_time = None
   ...
   ...             @icosystem.on("sensor_node_measurement_data")
   ...             async def measurement_data_changed(measurement_data:
   ...                                                MeasurementData) -> None:
   ...                 nonlocal measurement_time
   ...                 measurement_time = time()
   ...                 nonlocal data
   ...                 data = measurement_data
   ...
   ...             await icosystem.start_measurement(StreamingConfiguration(first=True))
   ...             # Wait until one measurement data object is ready
   ...             while data is None:
   ...                 await sleep(0.01)
   ...
   ...             await icosystem.stop_measurement()
   ...
   ...             # Use methods `first`, `second`, and `third` to access different
   ...             # channels
   ...             first_channel = data.first()
   ...             # By default measurement data is saved as raw 16 bit ADC value
   ...             assert all((
   ...                 True if 0 <= datapoint.value <= 2**16 else False
   ...                 for datapoint in first_channel
   ...             ))
   ...             # Timestamps are stored in seconds from the epoch
   ...             assert all((
   ...                 (
   ...                     True
   ...                     if measurement_time - 1
   ...                     <= datapoint.timestamp
   ...                     <= measurement_time + 1
   ...                     else False
   ...                 )
   ...                 for datapoint in first_channel
   ...             )), "Offset of timestamp of measurement data too large"
   ...
   ...             # You can also access the measurement counters
   ...             # (cyclic value between 0 - 255)
   ...             assert all((
   ...                 True if 0 <= datapoint.counter <= 255 else False
   ...                 for datapoint in first_channel
   ...             ))
   ...             # To get a sense of the quality of the signal you can use the method
   ...             # dataloss, which will return a value between 0 (no data loss) and
   ...             # 1 (all data lost).
   ...             assert 0 <= data.dataloss() <= 1
   ...         except CANConnectionError as error:
   ...             print(f"Measurement error: {error}", file=stderr)
   ...         finally:
   ...             await icosystem.disconnect_sensor_node()
   ...     except CANInitError as error:
   ...         print(f"Unable to connect to ICOtronic system: {error}", file=stderr)
   ...     finally:
   ...         await icosystem.disconnect_stu()

   >>> mac_address = settings.sensor_node.eui # Change to MAC address of your sensor node
   >>> run(measure_data(ICOsystem(), mac_address))

For more information on how to work with measurement data, please take a look at the links below.

.. table:: Additional documentation about measurement data
   :widths: auto

   +--------------------------+------------------------------------------------+
   | Topic                    |  Links                                         |
   +--------------------------+------------------------------------------------+
   | Conversion               | - Function :func:`MeasurementData.apply`       |
   |                          | - `Converting Data Values`_ (ICOtronic library)|
   +--------------------------+------------------------------------------------+
   | Storing Measurement Data | - `Storing Data`_ (ICOtronic library)          |
   +--------------------------+------------------------------------------------+

.. _Converting Data Values: https://icotronic.readthedocs.io/en/6.0.0/usage.html#converting-data-values
.. _Storing Data: https://icotronic.readthedocs.io/en/6.0.0/usage.html#storing-data

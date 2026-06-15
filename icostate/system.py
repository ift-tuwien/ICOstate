"""Application Programming Interface for stateful access to ICOtronic system"""

# pylint: disable=too-many-lines

# -- Imports ------------------------------------------------------------------

from __future__ import annotations

from asyncio import create_task, sleep, Task
from logging import getLogger
from math import inf
from time import monotonic
from typing import Any

from icotronic.can import Connection, SensorNode, StreamingConfiguration, STU
from icotronic.can.adc import ADCConfiguration
from icotronic.can.node.stu import AsyncSensorNodeManager, SensorNodeInfo
from icotronic.can.sensor import SensorConfiguration
from icotronic.can.status import State as NodeState
from icotronic.measurement import MeasurementData
from netaddr import AddrFormatError, EUI
from pyee.asyncio import AsyncIOEventEmitter

from icostate.error import IncorrectStateError
from icostate.sensor import SensorNodeAttributes
from icostate.state import State

# -- Classes ------------------------------------------------------------------


class Measurement:
    """Collect measurement data

    Args:

        icosystem:

            ICOsystem class that should be used to measure data

    """

    def __init__(self, icosystem: ICOsystem) -> None:

        self.icosystem = icosystem
        self.read_task: Task[Any] | None = None
        self.logger = getLogger()
        self.update_rate = 60.0  # Sensible default of 60 Hz
        """Measurement update rate in Hz"""

    def start(
        self,
        configuration: StreamingConfiguration,
        update_rate: float,
        runtime: float = inf,
    ) -> None:
        """Start the measurement

        Args:

            configuration:

                The streaming configuration that should be used for the
                measurement

            update_rate:

                The measurement update rate in Hz, i.e. how many times in
                a second ``icosystem`` emits the
                ``sensor_node_measurement_data`` event.

            runtime:

                The measurement runtime in seconds

        """

        self.update_rate = update_rate

        if self.read_task is not None:
            self.logger.info("Stopping old measurement task")
            self.stop()

        self.logger.info("Creating new measurement task")
        self.read_task = create_task(self._read(configuration, runtime))

    def stop(self):
        """Stop the current measurement"""

        if self.read_task is not None:
            self.read_task.cancel()
        self.read_task = None

    async def _read(
        self, configuration: StreamingConfiguration, runtime: float = inf
    ):
        """Task for collecting measurement data

        configuration:

            The streaming configuration that should be used for the
            measurement

        runtime:

            The measurement runtime in seconds

        """

        async with self.icosystem.sensor_node.open_data_stream(
            configuration
        ) as stream:
            self.logger.info(
                "Opened stream with configuration: %s", configuration
            )

            period = 1 / self.update_rate
            collected_data = MeasurementData(configuration)
            start = monotonic()
            end = start + runtime
            async for data, _ in stream:
                collected_data.append(data)
                current = monotonic()
                if current - start >= period or current >= end:
                    self.icosystem.emit(
                        "sensor_node_measurement_data", collected_data
                    )
                    if current >= end:
                        self.icosystem.state = State.SENSOR_NODE_CONNECTED
                        break
                    start = current
                    collected_data = MeasurementData(configuration)


class ICOsystem(AsyncIOEventEmitter):
    """Stateful access to ICOtronic system

    Args:

        *arguments:

            Positional arguments (handled by pyee)

        **keyword_arguments:

            Keyword arguments (handled by pyee)

    Raises:

        CANInitError: if the CAN initialization fails

    """

    def __init__(self, *arguments, **keyword_arguments):
        super().__init__(*arguments, **keyword_arguments)

        self.state = State.DISCONNECTED
        self.connection = Connection()
        self.stu: STU | None = None
        self.sensor_node_connection: AsyncSensorNodeManager | None = None
        self.sensor_node: SensorNode = None

        self.measurement = Measurement(self)

        self.sensor_node_attributes: SensorNodeAttributes | None = None
        """Information about currently connected sensor node"""

    def check_in_state(
        self, states: set[State], description: str, invert=False
    ) -> None:
        """Check if the system is in an allowed state

        Args:

            states:
                The set of allowed states

            description:
                A description of the action that is only allowed in the states
                specified by ``states``

            invert:
                Specifies if the state check should be inverted, i.e. that the
                code should check that the current state is not part of any of
                the states in ``states``

        Raises:

            IncorrectStateError:

                If the current state is not included in ``states``

        Examples:

            Import necessary code

            >>> from asyncio import run
            >>> from icostate.state import State

            Checking for correct state will not raise an exception

            >>> system = ICOsystem()
            >>> system.check_in_state({State.DISCONNECTED},
            ...                       "Doing something")

            Checking for an incorrect state will raise an exception

            >>> system.check_in_state({State.STU_CONNECTED},
            ...     "Doing something") # doctest:+NORMALIZE_WHITESPACE
            Traceback (most recent call last):
               ...
            icostate.error.IncorrectStateError: Doing something only allowed
            in the state: STU Connected (current state: Disconnected)

        """

        plural = "" if len(states) <= 1 else "s"
        states_representation = ", ".join(map(str, states))
        state_info = f"(current state: {self.state})"
        if invert:
            if self.state in states:
                raise IncorrectStateError(
                    f"{description} not allowed in the state{plural}:"
                    f" {states_representation}"
                    + (f" {state_info}" if plural else "")
                )
        else:
            if self.state not in states:
                raise IncorrectStateError(
                    f"{description} only allowed in the state{plural}: "
                    f"{states_representation} {state_info}"
                )

    async def connect_stu(self) -> None:
        """Connect to STU

        Raises:

            NoResponseError:

                If there was no response to an request made by this coroutine

        Examples:

            Import necessary code

            >>> from asyncio import run

            Connect and disconnect from STU

            >>> async def connect_disconnect_stu(icosystem: ICOsystem):
            ...     print(f"Before connection to STU: {icosystem.state}")
            ...     await icosystem.connect_stu()
            ...     print(f"After connection to STU: {icosystem.state}")
            ...     await icosystem.disconnect_stu()
            ...     print(f"After disconnection fro STU: {icosystem.state}")
            >>> run(connect_disconnect_stu(ICOsystem()))
            Before connection to STU: Disconnected
            After connection to STU: STU Connected
            After disconnection fro STU: Disconnected

        """

        self.check_in_state({State.DISCONNECTED}, "Connecting to STU")

        # pylint: disable=unnecessary-dunder-call
        self.stu = await self.connection.__aenter__()
        # pylint: enable=unnecessary-dunder-call
        self.state = State.STU_CONNECTED
        assert isinstance(self.stu, STU)

    async def disconnect_stu(self) -> None:
        """Disconnect from STU

        Raises:

            NoResponseError:

                If there was no response to an request made by this coroutine

        """

        self.check_in_state({State.STU_CONNECTED}, "Disconnecting from STU")

        await self.connection.__aexit__(None, None, None)
        self.state = State.DISCONNECTED
        self.stu = None

    async def get_stu_mac_address(self) -> EUI:
        """Get STU MAC address

        Raises:

            NoResponseError:

                If there was no response to an request made by this coroutine

        Examples:

            Import necessary code

            >>> from asyncio import run
            >>> from netaddr import EUI

            Get the MAC address of a connected STU

            >>> async def get_mac_address(icosystem: ICOsystem):
            ...     await icosystem.connect_stu()
            ...     mac_address = await icosystem.get_stu_mac_address()
            ...     await icosystem.disconnect_stu()
            ...     return mac_address
            >>> mac_address = run(get_mac_address(ICOsystem()))
            >>> isinstance(mac_address, EUI)
            True

            Trying to get the MAC address without an STU connection fails

            >>> async def get_mac_address(icosystem: ICOsystem):
            ...     await icosystem.get_stu_mac_address()
            >>> run(get_mac_address(ICOsystem())
            ...    ) # doctest:+NORMALIZE_WHITESPACE
            Traceback (most recent call last):
               ...
            icostate.error.IncorrectStateError: Getting MAC address of STU not
            allowed in the state: Disconnected

        """

        self.check_in_state(
            {State.DISCONNECTED}, "Getting MAC address of STU", invert=True
        )

        assert isinstance(self.stu, STU)

        return await self.stu.get_mac_address()

    async def reset_stu(self) -> None:
        """Reset STU

        Raises:

            NoResponseError:

                If there was no response to an request made by this coroutine

        Examples:

            Import necessary code

            >>> from asyncio import run

            Reset a connected STU

            >>> async def reset_stu(icosystem: ICOsystem):
            ...     await icosystem.connect_stu()
            ...     await icosystem.reset_stu()
            ...     await icosystem.disconnect_stu()
            >>> run(reset_stu(ICOsystem()))

            Resetting the STU will not work if the STU is not connected

            >>> async def reset_stu_without_connection(icosystem: ICOsystem):
            ...     await icosystem.reset_stu()
            >>> run(reset_stu_without_connection(
            ...     ICOsystem())) # doctest:+NORMALIZE_WHITESPACE
            Traceback (most recent call last):
               ...
            icostate.error.IncorrectStateError: Resetting STU only allowed in
                the state: STU Connected (current state: Disconnected)

        """

        self.check_in_state({State.STU_CONNECTED}, "Resetting STU")

        assert isinstance(self.stu, STU)

        await self.stu.reset()

        # Make sure that the STU is in the correct state after the reset,
        # although this seems to be the case anyway. At least in my limited
        # tests the STU was always in the “operating state” even directly
        # after the reset.
        operating = NodeState(location="Application", state="Operating")
        while (state := await self.stu.get_state()) != operating:
            await sleep(1)

        assert state == operating

    async def collect_sensor_nodes(self) -> list[SensorNodeInfo]:
        """Get available sensor nodes

        This coroutine collects sensor node information until either

        - no new sensor node was found or
        - until the given timeout, if no sensor node was found.

        Returns:

            A list containing information about the available sensor nodes

        Raises:

            NoResponseError:

                If there was no response to an request made by this coroutine

        Examples:

            Import necessary code

            >>> from asyncio import run

            Collect sensor nodes

            >>> async def collect_sensor_nodes(icosystem: ICOsystem
            ...     ) -> list[SensorNodeInfo]:
            ...     await icosystem.connect_stu()
            ...     nodes = await icosystem.collect_sensor_nodes()
            ...     await icosystem.disconnect_stu()
            ...     return nodes
            >>> sensor_nodes = run(collect_sensor_nodes(ICOsystem()))
            >>> # We assume that at least one sensor node is available
            >>> len(sensor_nodes) >= 1
            True

        """

        self.check_in_state(
            {State.DISCONNECTED},
            "Collecting data about sensor devices",
            invert=True,
        )

        assert isinstance(self.stu, STU)

        return await self.stu.collect_sensor_nodes()

    async def connect_sensor_node_mac(self, mac_address: str) -> None:
        """Connect to the node with the specified MAC address

        Args:

            mac_address:

                The MAC address of the sensor node

        Raises:

            ValueError:

                If the specified MAC address is not valid

            NoResponseError:

                If there was no response to an request made by this coroutine

        Examples:

            Import necessary code

            >>> from asyncio import run
            >>> from icostate.config import settings

            Connect to and disconnect from sensor node

            >>> async def connect_sensor_node(icosystem: ICOsystem,
            ...                               mac_address: str):
            ...     await icosystem.connect_stu()
            ...     await icosystem.connect_sensor_node_mac(mac_address)
            ...     print(icosystem.state)
            ...     await icosystem.disconnect_sensor_node()
            ...     print(icosystem.state)
            ...     await icosystem.disconnect_stu()
            >>> run(connect_sensor_node(ICOsystem(), settings.sensor_node.eui))
            Sensor Node Connected
            STU Connected

            Try to connect using an invalid MAC address

            >>> async def try_connect_incorrect_mac(icosystem: ICOsystem):
            ...     try:
            ...         await icosystem.connect_stu()
            ...         await icosystem.connect_sensor_node_mac(
            ...             "Not a MAC address")
            ...     finally:
            ...         await icosystem.disconnect_stu()

            >>> run(try_connect_incorrect_mac(ICOsystem()))
            Traceback (most recent call last):
               ...
            ValueError: “Not a MAC address” is not a valid MAC address:...

        """

        self.check_in_state(
            {State.STU_CONNECTED}, "Connecting to sensor device"
        )

        assert isinstance(self.stu, STU)

        eui = None
        try:
            eui = EUI(mac_address)
        except AddrFormatError as error:
            raise ValueError(
                f"“{mac_address}” is not a valid MAC address: {error}"
            ) from error

        assert isinstance(eui, EUI)

        self.sensor_node_connection = self.stu.connect_sensor_node(eui)
        assert isinstance(self.sensor_node_connection, AsyncSensorNodeManager)
        # pylint: disable=unnecessary-dunder-call
        self.sensor_node = await self.sensor_node_connection.__aenter__()
        # pylint: enable=unnecessary-dunder-call
        assert isinstance(self.sensor_node, SensorNode)

        mac_address = await self.sensor_node.get_mac_address()
        self.emit("sensor_node_mac_address", mac_address)
        name = await self.sensor_node.get_name()
        self.emit("sensor_node_name", name)
        adc_configuration = await self.sensor_node.get_adc_configuration()
        self.emit("sensor_node_adc_configuration", adc_configuration)

        self.sensor_node_attributes = SensorNodeAttributes(
            mac_address=mac_address,
            name=name,
            adc_configuration=adc_configuration,
        )
        self.state = State.SENSOR_NODE_CONNECTED

    async def disconnect_sensor_node(self) -> None:
        """Disconnect from current sensor node

        Raises:

            NoResponseError:

                If there was no response to an request made by this coroutine

        """

        self.check_in_state(
            {State.SENSOR_NODE_CONNECTED}, "Disconnecting from sensor device"
        )

        assert isinstance(self.stu, STU)
        assert isinstance(self.sensor_node, SensorNode)
        assert isinstance(self.sensor_node_connection, AsyncSensorNodeManager)

        await self.sensor_node_connection.__aexit__(None, None, None)

        self.sensor_node = None
        self.sensor_node_attributes = None
        self.state = State.STU_CONNECTED

    async def is_sensor_node_connected(self) -> bool:
        """Check if the STU is connected to a sensor node

        Returns:

            - ``True``, if the STU is connected to a sensor node
            - ``False``, otherwise

        Examples:

            Import necessary code

            >>> from asyncio import run
            >>> from icostate.config import settings

            Check if a sensor node is connected

            >>> async def connect_sensor_node(icosystem: ICOsystem,
            ...                               mac_address: str):
            ...     print("Before connection to STU:",
            ...           await icosystem.is_sensor_node_connected())
            ...     await icosystem.connect_stu()
            ...     print("After connection to STU:",
            ...           await icosystem.is_sensor_node_connected())
            ...     await icosystem.connect_sensor_node_mac(mac_address)
            ...     print("After connection to sensor node:",
            ...           await icosystem.is_sensor_node_connected())
            ...     await icosystem.disconnect_sensor_node()
            ...     print("After disconnection from sensor node:",
            ...           await icosystem.is_sensor_node_connected())
            ...     await icosystem.disconnect_stu()
            >>> run(connect_sensor_node(ICOsystem(), settings.sensor_node.eui))
            Before connection to STU: False
            After connection to STU: False
            After connection to sensor node: True
            After disconnection from sensor node: False

        """

        if self.state == State.DISCONNECTED:
            return False

        assert isinstance(self.stu, STU)

        return await self.stu.is_connected()

    async def _connect_sensor_node(
        self, mac_address: str | None = None
    ) -> bool:
        """Connect to sensor node if not already connected

        Args:

            mac_address:

                The MAC address of the sensor device that this coroutine
                connects to, if the system is not already connected to a
                sensor node

        Returns:

            - ``True``, if the system was **not** connected to a sensor node
                        before
            - ``False``, otherwise

        """

        disconnected_before = False
        if self.state == State.STU_CONNECTED:
            if not isinstance(mac_address, str):
                raise ValueError(
                    "MAC address is required for connecting to sensor node"
                )

            assert isinstance(mac_address, str)
            disconnected_before = True
            await self.connect_sensor_node_mac(mac_address)

        assert isinstance(self.sensor_node, SensorNode)

        return disconnected_before

    async def rename(
        self, new_name: str, mac_address: str | None = None
    ) -> str:
        """Set the name of the sensor node with the specified MAC address

        Depending on the state the system is in this coroutine will **either**:

        1. connect to the sensor device with the given MAC address, if there
           is no connection yet and disconnect afterwards or
        2. just use the current connection and rename the current sensor
           device. In this case the given MAC address will be ignored!

        Args:

            new_name:

                The new name of the sensor device

            mac_address:

                The MAC address of the sensor device that should be renamed

        Returns:

            The (old) name before it was changed to ``new_name``

        Raises:

            NoResponseError: If there was no response to an request made by
                             this coroutine

            ValueError: If you call this method without specifying the MAC
                        address while the system is not connected to a sensor
                        node

        Examples:

            Import necessary code

            >>> from asyncio import run
            >>> from icostate.config import settings

            Rename a disconnected sensor node

            >>> async def rename_disconnected(icosystem: ICOsystem,
            ...                               mac_address: str,
            ...                               name: str):
            ...     await icosystem.connect_stu()
            ...     print(f"Before renaming: {icosystem.state}")
            ...     await icosystem.rename(name, mac_address)
            ...     print(f"After renaming: {icosystem.state}")
            ...     await icosystem.disconnect_stu()
            >>> name = "Test-STH"
            >>> run(rename_disconnected(ICOsystem(),
            ...                         settings.sensor_node.eui, name))
            Before renaming: STU Connected
            After renaming: STU Connected

            The coroutine returns the old name of the sensor node

            >>> async def rename_disconnected(icosystem: ICOsystem,
            ...                               mac_address: str,
            ...                               name: str):
            ...     await icosystem.connect_stu()
            ...     old_name = await icosystem.rename(name, mac_address)
            ...     await icosystem.disconnect_stu()
            ...     return old_name
            >>> name = "Test-STH"
            >>> new_name = "Test-RN"
            >>> old = run(rename_disconnected(ICOsystem(),
            ...                               settings.sensor_node.eui,
            ...                               new_name))
            >>> old == name
            True
            >>> old = run(rename_disconnected(ICOsystem(),
            ...                               settings.sensor_node.eui, name))
            >>> old == new_name
            True

            Rename a connected sensor node

            >>> async def rename_connected(icosystem: ICOsystem,
            ...                            mac_address: str,
            ...                            name: str):
            ...     await icosystem.connect_stu()
            ...     await icosystem.connect_sensor_node_mac(mac_address)
            ...     print(f"Before renaming: {icosystem.state}")
            ...     await icosystem.rename(name, None)
            ...     print(f"After renaming: {icosystem.state}")
            ...     await icosystem.disconnect_sensor_node()
            ...     await icosystem.disconnect_stu()
            >>> name = "Test-STH"
            >>> run(rename_connected(ICOsystem(), settings.sensor_node.eui,
            ...                      name))
            Before renaming: Sensor Node Connected
            After renaming: Sensor Node Connected

        """

        self.check_in_state(
            {State.STU_CONNECTED, State.SENSOR_NODE_CONNECTED},
            "Renaming sensor device",
        )
        assert isinstance(self.stu, STU)

        disconnect_after = await self._connect_sensor_node(mac_address)
        # Sensor node attributes should have been set at least once by
        # calling `connect_sensor_node_mac` either directly or indirectly.
        assert isinstance(self.sensor_node_attributes, SensorNodeAttributes)
        old_name = self.sensor_node_attributes.name

        await self.sensor_node.set_name(new_name)
        self.sensor_node_attributes.name = new_name
        self.emit("sensor_node_name", self.sensor_node_attributes.name)

        if disconnect_after:
            await self.disconnect_sensor_node()

        return old_name

    async def get_adc_configuration(
        self, mac_address: str | None = None
    ) -> ADCConfiguration:
        """Read the ADC configuration of a sensor node

        Depending on the state the system is in this coroutine will **either**:

        1. connect to the sensor device with the given MAC address, if there
           is no connection yet and disconnect afterwards or
        2. just use the current connection and get the ADC configuration of
           the current sensor device. In this case the given MAC address will
           be ignored!

        Args:

            mac_address:

                The MAC address of the sensor device for which we want to
                retrieve the ADC configuration

        Raises:

            NoResponseError: If there was no response to an request made by
                             this coroutine

            ValueError: If you call this method without specifying the MAC
                        address while the system is not connected to a sensor
                        node

        Returns:

            The ADC configuration of the sensor node

        Examples:

            Import necessary code

            >>> from asyncio import run
            >>> from icostate.config import settings

            Read the ADC configuration of a disconnected sensor node

            >>> async def get_adc_configuration(icosystem: ICOsystem,
            ...                                 mac_address: str
            ...                                ) -> ADCConfiguration:
            ...     await icosystem.connect_stu()
            ...     print(f"Before reading ADC config: {icosystem.state}")
            ...     adc_config = (await
            ...         icosystem.get_adc_configuration(mac_address))
            ...     print(f"After reading ADC config: {icosystem.state}")
            ...     await icosystem.disconnect_stu()
            ...     return adc_config
            >>> config = run(get_adc_configuration(ICOsystem(),
            ...                                    settings.sensor_node.eui))
            Before reading ADC config: STU Connected
            After reading ADC config: STU Connected
            >>> isinstance(config.prescaler, int)
            True
            >>> isinstance(config.acquisition_time, int)
            True
            >>> isinstance(config.oversampling_rate, int)
            True
            >>> isinstance(config.reference_voltage, float)
            True
            >>> 1 <= config.prescaler <= 127
            True

            Read the ADC configuration of a connected sensor node

            >>> async def get_adc_configuration(icosystem: ICOsystem,
            ...                                 mac_address: str
            ...                                ) -> ADCConfiguration:
            ...     await icosystem.connect_stu()
            ...     await icosystem.connect_sensor_node_mac(mac_address)
            ...     adc_config = await icosystem.get_adc_configuration()
            ...     await icosystem.disconnect_sensor_node()
            ...     await icosystem.disconnect_stu()
            ...     return adc_config
            >>> config = run(get_adc_configuration(ICOsystem(),
            ...                                    settings.sensor_node.eui))
            >>> isinstance(config, ADCConfiguration)
            True

        """

        self.check_in_state(
            {State.STU_CONNECTED, State.SENSOR_NODE_CONNECTED},
            "Getting ADC configuration of sensor node",
        )

        disconnect_after = await self._connect_sensor_node(mac_address)

        # Sensor node attributes should have been set at least once by
        # calling `connect_sensor_node_mac` either directly or indirectly.
        adc_configuration = await self.sensor_node.get_adc_configuration()
        self.sensor_node.adc_configuration = adc_configuration
        self.emit("sensor_node_adc_configuration", adc_configuration)

        if disconnect_after:
            await self.disconnect_sensor_node()

        return adc_configuration

    async def set_adc_configuration(
        self,
        adc_configuration: ADCConfiguration,
        mac_address: str | None = None,
    ) -> None:
        """Change the ADC configuration of a sensor node

        Depending on the state the system is in this coroutine will **either**:

        1. connect to the sensor device with the given MAC address, if there
           is no connection yet and disconnect afterwards or
        2. just use the current connection and change the ADC configuration of
           the current sensor device. In this case the given MAC address will
           be ignored!

        Args:

            mac_address:

                The MAC address of the sensor device for which we want to
                change the ADC configuration

        Raises:

            NoResponseError: If there was no response to an request made by
                             this coroutine

            ValueError: If you call this method without specifying the MAC
                        address while the system is not connected to a sensor
                        node

        Examples:

            Import necessary code

            >>> from asyncio import run
            >>> from icostate.config import settings

            Set the ADC configuration of a disconnected sensor node

            >>> async def set_adc_configuration(icosystem: ICOsystem,
            ...                                 adc_config: ADCConfiguration,
            ...                                 mac_address: str):
            ...     await icosystem.connect_stu()
            ...     print(f"Before setting ADC config: {icosystem.state}")
            ...     adc_config = (await
            ...         icosystem.set_adc_configuration(adc_config,
            ...                                         mac_address))
            ...     print(f"After setting ADC config: {icosystem.state}")
            ...     await icosystem.disconnect_stu()
            >>> config = ADCConfiguration(prescaler=2,
            ...                           acquisition_time=8,
            ...                           oversampling_rate=64)
            >>> config = run(set_adc_configuration(ICOsystem(), config,
            ...                                    settings.sensor_node.eui))
            Before setting ADC config: STU Connected
            After setting ADC config: STU Connected

            Set the ADC configuration of a connected sensor node

            >>> async def set_adc_configuration(icosystem: ICOsystem,
            ...                                 adc_config: ADCConfiguration,
            ...                                 mac_address: str):
            ...     await icosystem.connect_stu()
            ...     await icosystem.connect_sensor_node_mac(mac_address)
            ...     print(f"Before setting ADC config: {icosystem.state}")
            ...     adc_config = (await
            ...         icosystem.set_adc_configuration(adc_config))
            ...     print(f"After setting ADC config: {icosystem.state}")
            ...     await icosystem.disconnect_sensor_node()
            ...     await icosystem.disconnect_stu()
            >>> config = ADCConfiguration(prescaler=2,
            ...                           acquisition_time=8,
            ...                           oversampling_rate=64)
            >>> config = run(set_adc_configuration(ICOsystem(), config,
            ...                                    settings.sensor_node.eui))
            Before setting ADC config: Sensor Node Connected
            After setting ADC config: Sensor Node Connected

        """

        self.check_in_state(
            {State.STU_CONNECTED, State.SENSOR_NODE_CONNECTED},
            "Setting ADC configuration of sensor node",
        )

        disconnect_after = await self._connect_sensor_node(mac_address)

        await self.sensor_node.set_adc_configuration(**adc_configuration)
        self.emit("sensor_node_adc_configuration", adc_configuration)

        if disconnect_after:
            await self.disconnect_sensor_node()

    async def get_sensor_configuration(self) -> SensorConfiguration:
        """Get the sensor numbers for the different measurement channels

        Raises:

            UnsupportedFeatureException: if the sensor node does not
            support reading the sensor configuration

        Examples:

            Import necessary code

            >>> from asyncio import run
            >>> from icostate.config import settings
            >>> from icostate import UnsupportedFeatureException

            Getting sensor config for node works

            >>> async def get_sensor_configuration(icosystem: ICOsystem,
            ...                                    mac_address: str):
            ...     await icosystem.connect_stu()
            ...     await icosystem.connect_sensor_node_mac(mac_address)
            ...     try:
            ...         config = await icosystem.get_sensor_configuration()
            ...     except UnsupportedFeatureException:
            ...         config = {}
            ...     finally:
            ...         await icosystem.disconnect_sensor_node()
            ...         await icosystem.disconnect_stu()
            ...     return config
            >>> config = run(get_sensor_configuration(ICOsystem(),
            ...                              settings.sensor_node.eui))
            >>> all([ 0 <= value <= 255 for value in config.values()])
            True

        """

        self.check_in_state(
            {State.SENSOR_NODE_CONNECTED}, "Setting sensor configuration"
        )

        assert isinstance(self.sensor_node, SensorNode)

        return await self.sensor_node.get_sensor_configuration()

    async def set_sensor_configuration(
        self, sensors: SensorConfiguration
    ) -> None:
        """Change the sensor numbers for the different measurement channels

        Args:

            sensors:

                The sensor numbers for the different measurement channels

        Raises:

            UnsupportedFeatureException: if the sensor node does not
            support changing the sensor configuration

        Examples:

            Import necessary code

            >>> from asyncio import run
            >>> from icostate.config import settings

            Setting sensor config for node (with support) works

            >>> async def set_sensor_configuration(icosystem: ICOsystem,
            ...                                    config: SensorConfiguration,
            ...                                    mac_address: str):
            ...     await icosystem.connect_stu()
            ...     await icosystem.connect_sensor_node_mac(mac_address)
            ...     await icosystem.set_sensor_configuration(config)
            ...     config_read = await icosystem.get_sensor_configuration()
            ...     await icosystem.disconnect_sensor_node()
            ...     await icosystem.disconnect_stu()
            ...     return config_read
            >>> config = SensorConfiguration(first=2, second=5, third=3)
            >>> config_read = run(set_sensor_configuration(
            ...     ICOsystem(), config, settings.sensor_node.eui))
            >>> config == config_read
            True

        """

        self.check_in_state(
            {State.SENSOR_NODE_CONNECTED}, "Setting sensor configuration"
        )

        assert isinstance(self.sensor_node, SensorNode)

        await self.sensor_node.set_sensor_configuration(sensors)

    async def start_measurement(
        self,
        configuration: StreamingConfiguration,
        update_rate: float = 60,
        runtime: float = inf,
    ) -> None:
        """Start Measurement

        Args:

            configuration:

                The streaming configuration that should be used for the
                measurement

            update_rate:

                The measurement update rate in Hz, i.e. how many times in
                a second ``icosystem`` emits the
                ``sensor_node_measurement_data`` event.

            runtime:

                The measurement runtime in seconds

        """

        self.check_in_state(
            {State.SENSOR_NODE_CONNECTED}, "Starting measurement"
        )

        self.measurement.start(configuration, update_rate, runtime)

        self.state = State.MEASUREMENT

    async def stop_measurement(self) -> None:
        """Stop measurement"""

        self.check_in_state({State.MEASUREMENT}, "Stopping measurement")

        self.measurement.stop()

        self.state = State.SENSOR_NODE_CONNECTED


if __name__ == "__main__":
    from doctest import testmod

    testmod()

"""Device update listener."""

import logging
import inspect
from ipaddress import IPv4Address
from typing import (
    cast,
    Any,
    Awaitable,
    Callable,
    Dict,
    Iterable,
    Mapping,
    Optional,
    Union,
)
from types import MethodType
import weakref

from async_upnp_client import UpnpAdvertisementListener, UpnpDevice, UpnpFactory

_LOGGER = logging.getLogger(__name__)


NotifyAsyncCallbackType = Callable[["UpnpDevice", bool], Awaitable[None]]


class DeviceUpdater:
    """
    Device updater.

    Listens for SSDP advertisements and updates device inline when needed.
    Inline meaning that it keeps the original UpnpDevice instance.
    So be sure to keep only references to the UpnpDevice,
    as a device might decide to remove a service after an update!
    """

    def __init__(
        self,
        device: Union[UpnpDevice, Iterable[UpnpDevice], None],
        factory: UpnpFactory,
        source_ip: Optional[IPv4Address] = None,
    ) -> None:
        """Initialize."""
        # Don't keep the device object alive if nothing else is using it anymore
        self._devices = weakref.WeakValueDictionary[str, UpnpDevice]()
        if isinstance(device, UpnpDevice):
            self._devices[device.udn] = device
        elif isinstance(device, Iterable):
            self._devices.update({dev.udn: dev for dev in device})

        self._callbacks: Dict[str, weakref.ReferenceType] = {}

        self._factory = factory
        self._listener = UpnpAdvertisementListener(
            on_alive=self._on_alive,
            on_byebye=self._on_byebye,
            on_update=self._on_update,
            source_ip=source_ip,
        )

    async def async_start(self) -> None:
        """Start listening for notifications."""
        _LOGGER.debug("Start listening for notifications.")
        await self._listener.async_start()

    async def async_stop(self) -> None:
        """Stop listening for notifications."""
        _LOGGER.debug("Stop listening for notifications.")
        await self._listener.async_stop()

    def add_device(
        self,
        device: UpnpDevice,
        async_on_notify: Optional[NotifyAsyncCallbackType] = None,
    ) -> None:
        """Add a device from which to listen for updates.

        :param async_on_notify: Function called when device sends an update:
            callback(device: UpnpDevice, changed: bool)
                * device is the same device as here
                * changed is True if the device location or configuration changed
            `device.available` will be updated before the callback is called.
        """
        if device.udn in self._devices:
            raise UpnpError("Device UDN is already registered")
        self._devices[device.udn] = device

        if inspect.ismethod(async_on_notify):
            self._callbacks[device.udn] = weakref.WeakMethod(
                cast(MethodType, async_on_notify)
            )
        elif async_on_notify:
            self._callbacks[device.udn] = weakref.ref(async_on_notify)

    def remove_device(self, device: Union[UpnpDevice, str]) -> None:
        """Remove a device, specified by device object or UDN.

        This isn't strictly necessary, as the device will be automatically
        removed when all other users have dropped their references.
        """
        if isinstance(device, UpnpDevice):
            device = device.udn
        try:
            del self._devices[device]
        except KeyError:
            pass
        try:
            del self._callbacks[device]
        except KeyError:
            pass

    async def _on_alive(self, data: Mapping[str, str]) -> None:
        """Handle on alive."""
        # Ensure for root devices only.
        if data.get("nt") != "upnp:rootdevice":
            return

        # Ensure for one of our devices.
        try:
            device = self._devices[data["_udn"]]
        except KeyError:
            return

        _LOGGER.debug("Handling alive: %s", data)
        await self._async_handle_alive_update(data, device)

    async def _on_byebye(self, data: Mapping[str, Any]) -> None:
        """Handle on byebye."""
        # Ensure for root devices only.
        if data.get("nt") != "upnp:rootdevice":
            return

        # Ensure for one of our devices.
        try:
            device = self._devices[data["_udn"]]
        except KeyError:
            return

        _LOGGER.debug("Handling byebye: %s", data)
        device.available = False
        await self._call_notify_callback(device, False)

    async def _on_update(self, data: Mapping[str, Any]) -> None:
        """Handle on update."""
        # Ensure for root devices only.
        if data.get("nt") != "upnp:rootdevice":
            return

        # Ensure for one of our devices.
        try:
            device = self._devices[data["_udn"]]
        except KeyError:
            return

        _LOGGER.debug("Handling update: %s", data)
        await self._async_handle_alive_update(data, device)

    async def _async_handle_alive_update(
        self, data: Mapping[str, str], device: UpnpDevice
    ) -> None:
        """Handle on_alive or on_update."""
        do_reinit = False

        # Handle BOOTID.UPNP.ORG.
        boot_id = data.get("BOOTID.UPNP.ORG")
        if boot_id and boot_id != device.boot_id:
            _LOGGER.debug("New boot_id: %s, old boot_id: %s", boot_id, device.boot_id)
            do_reinit = True

        # Handle CONFIGID.UPNP.ORG.
        config_id = data.get("CONFIGID.UPNP.ORG")
        if config_id and config_id != device.config_id:
            _LOGGER.debug(
                "New config_id: %s, old config_id: %s",
                config_id,
                device.config_id,
            )
            do_reinit = True

        # Handle LOCATION.
        location = data.get("LOCATION")
        if location and device.device_url != location:
            _LOGGER.debug(
                "New location: %s, old location: %s", location, device.device_url
            )
            do_reinit = True

        if location and do_reinit:
            await self._reinit_device(device, location, boot_id, config_id)

        # We heard from it, so mark it available.
        device.available = True

        await self._call_notify_callback(device, do_reinit)

    async def _reinit_device(
        self,
        device: UpnpDevice,
        location: str,
        boot_id: Optional[str],
        config_id: Optional[str],
    ) -> None:
        """Reinitialize device."""
        # pylint: disable=protected-access
        _LOGGER.debug("Reinitializing device")

        new_device = await self._factory.async_create_device(location)

        device._device_info = new_device._device_info

        # Copy new services and update the binding between the original device and new services.
        device.services = new_device.services
        for service in device.services.values():
            service.device = device

        device.boot_id = boot_id
        device.config_id = config_id

    async def _call_notify_callback(self, device: UpnpDevice, reinit: bool) -> None:
        """Call the callback that's listening for notify events from the device."""
        callback_ref = self._callbacks.get(device.udn)
        if not callback_ref:
            return
        callback = callback_ref()
        if not callback:
            # Refered function/method has gone, so nothing to call
            del self._callbacks[device.udn]
            return

        await callback(device, reinit)

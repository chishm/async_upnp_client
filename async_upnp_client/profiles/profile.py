# -*- coding: utf-8 -*-
"""UPnP base-profile module."""

import asyncio
import logging
from datetime import timedelta
from typing import Dict, List, Mapping, Optional, Sequence, Set

from async_upnp_client.client import (
    EventCallbackType,
    UpnpAction,
    UpnpDevice,
    UpnpService,
    UpnpStateVariable,
)
from async_upnp_client.device_updater import DeviceUpdater, NotifyAsyncCallbackType
from async_upnp_client.event_handler import UpnpEventHandler
from async_upnp_client.search import async_search
from async_upnp_client.ssdp import IPvXAddress, SSDP_MX

_LOGGER = logging.getLogger(__name__)

SUBSCRIBE_TIMEOUT = timedelta(minutes=9)


class UpnpProfileDevice:
    """
    Base class for UpnpProfileDevices.

    Override _SERVICE_TYPES for aliases.
    """

    DEVICE_TYPES: List[str] = []

    _SERVICE_TYPES: Dict[str, Set[str]] = {}

    @classmethod
    async def async_search(
        cls,
        source_ip: Optional[IPvXAddress] = None,
        timeout: int = SSDP_MX,
    ) -> Set[Mapping[str, str]]:
        """
        Search for this device type.

        This only returns search info, not a profile itself.

        :param source_ip Source IP to scan from
        :param timeout Timeout to use
        :return: Set of devices (dicts) found
        """
        responses = set()

        async def on_response(data: Mapping[str, str]) -> None:
            if "st" in data and data["st"] in cls.DEVICE_TYPES:
                responses.add(data)

        await async_search(
            async_callback=on_response, source_ip=source_ip, timeout=timeout
        )

        return responses

    @classmethod
    async def async_discover(cls) -> Set[Mapping[str, str]]:
        """Alias for async_search."""
        return await cls.async_search()

    def __init__(
        self,
        device: UpnpDevice,
        event_handler: UpnpEventHandler,
        device_updater: Optional[DeviceUpdater] = None,
    ) -> None:
        """Initialize."""
        self.device = device
        self._event_handler = event_handler
        self._device_updater = device_updater
        self.on_event: Optional[EventCallbackType] = None
        self._icon: Optional[str] = None
        self._subscribed_to_services = False

        self.async_on_notify: Optional[NotifyAsyncCallbackType] = None
        if self._device_updater:
            self._device_updater.add_device(self.device, self._async_on_device_notify)

    @property
    def name(self) -> str:
        """Get the name of the device."""
        return self.device.name

    @property
    def manufacturer(self) -> str:
        """Get the manufacturer of this device."""
        return self.device.manufacturer

    @property
    def model_description(self) -> Optional[str]:
        """Get the model description of this device."""
        return self.device.model_description

    @property
    def model_name(self) -> str:
        """Get the model name of this device."""
        return self.device.model_name

    @property
    def model_number(self) -> Optional[str]:
        """Get the model number of this device."""
        return self.device.model_number

    @property
    def serial_number(self) -> Optional[str]:
        """Get the serial number of this device."""
        return self.device.serial_number

    @property
    def udn(self) -> str:
        """Get the UDN of the device."""
        return self.device.udn

    @property
    def device_type(self) -> str:
        """Get the device type of this device."""
        return self.device.device_type

    @property
    def icon(self) -> Optional[str]:
        """Get a URL for the biggest icon for this device."""
        if not self.device.icons:
            return None
        if not self._icon:
            _ICON_MIME_PREFERENCE = {"image/png": 3, "image/jpeg": 2, "image/gif": 1}
            icons = [icon for icon in self.device.icons if icon.url]
            icons = sorted(
                icons,
                # Sort by area, then colour depth, then prefered mimetype
                key=lambda icon: (
                    icon.width * icon.height,
                    icon.depth,
                    _ICON_MIME_PREFERENCE.get(icon.mimetype, 0),
                ),
                reverse=True,
            )
            self._icon = icons[0].url
        return self._icon

    def _service(self, service_type_abbreviation: str) -> Optional[UpnpService]:
        """Get UpnpService by service_type or alias."""
        if not self.device:
            return None

        if service_type_abbreviation not in self._SERVICE_TYPES:
            return None

        for service_type in self._SERVICE_TYPES[service_type_abbreviation]:
            if self.device.has_service(service_type):
                return self.device.service(service_type)

        return None

    def _state_variable(
        self, service_name: str, state_variable_name: str
    ) -> Optional[UpnpStateVariable]:
        """Get state_variable from service."""
        service = self._service(service_name)
        if not service:
            return None

        if not service.has_state_variable(state_variable_name):
            return None

        return service.state_variable(state_variable_name)

    def _action(self, service_name: str, action_name: str) -> Optional[UpnpAction]:
        """Check if service has action."""
        service = self._service(service_name)
        if not service:
            return None

        if not service.has_action(action_name):
            return None

        return service.action(action_name)

    def _interesting_service(self, service: UpnpService) -> bool:
        """Check if service is a service we're interested in."""
        service_type = service.service_type
        for service_types in self._SERVICE_TYPES.values():
            if service_type in service_types:
                return True

        return False

    async def async_subscribe_services(self) -> timedelta:
        """(Re-)Subscribe to services.

        Calling this function for *resubscription* is only necessary if no
        device_updater was supplied, and the device has changed location or
        configuration. Otherwise, resubscription will be handled automatically.
        """
        for service in self.device.services.values():
            # ensure we are interested in this service_type
            if not self._interesting_service(service):
                continue

            service.on_event = self._on_event

            _LOGGER.debug("(Re)-subscribing to service: %s", service)
            sid = await self._event_handler.async_subscribe(
                service, timeout=SUBSCRIBE_TIMEOUT
            )
            if not sid:
                _LOGGER.debug("Failed subscribing to: %s", service)

        self._subscribed_to_services = True
        return SUBSCRIBE_TIMEOUT

    async def async_unsubscribe_services(self) -> None:
        """Unsubscribe from all of our subscribed services."""
        self._subscribed_to_services = False
        await self._do_unsubscribe_services()

    async def _do_unsubscribe_services(self) -> None:
        """Unsubscribe from all subscribed services, ignoring exceptions.

        Internal method that does not change _subscribed_to_services
        """
        await asyncio.gather(
            (
                self._event_handler.async_unsubscribe(service)
                for service in self.device.services.values()
            ),
            return_exceptions=True,
        )

    def _on_event(
        self, service: UpnpService, state_variables: Sequence[UpnpStateVariable]
    ) -> None:
        """
        State variable(s) changed. Override to handle events.

        :param service Service which sent the event.
        :param state_variables State variables which have been changed.
        """
        if self.on_event:
            self.on_event(service, state_variables)  # pylint: disable=not-callable

    async def _async_on_device_notify(self, device: UpnpDevice, reinit: bool) -> None:
        """Device might have changed availability, location, or services.

        :param device: Device which sent the notification.
        :param reinit: Device had to be re-initialized.
        """
        if self._subscribed_to_services:
            if device.available:
                # Redo subscriptions in case the location has changed or device restarted
                await self.async_subscribe_services()
            else:
                # Clear out all subscriptions
                await self._do_unsubscribe_services()
        if self.async_on_notify:
            await self.async_on_notify(device, reinit)

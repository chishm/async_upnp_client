# -*- coding: utf-8 -*-
"""UPnP event handler module."""

import aiohttp
import asyncio
import dataclasses
import logging
import urllib.parse
from datetime import datetime, timedelta
from http import HTTPStatus
import socket
from typing import Dict, Mapping, Optional, Tuple, Union
import weakref

import defusedxml.ElementTree as DET

from async_upnp_client.client import UpnpRequester, UpnpService
from async_upnp_client.const import NS
from async_upnp_client.utils import async_get_local_ip, get_local_ip

_LOGGER = logging.getLogger(__name__)
_LOGGER_TRAFFIC_UPNP = logging.getLogger("async_upnp_client.traffic.upnp")

DEFAULT_SUBSCRIPTION_TIMEOUT = timedelta(seconds=1800)
RESUBSCRIBE_THRESHOLD = timedelta(seconds=60)


@dataclasses.dataclass
class SubscriptionInfo:
    service: weakref.ReferenceType[UpnpService]
    timeout: timedelta
    renewal_time: datetime


class UpnpEventHandler:
    """
    Handles upnp eventing.

    An incoming NOTIFY request should be pass to handle_notify().
    subscribe/resubscribe/unsubscribe handle subscriptions.
    """

    def __init__(
        self,
        callback_url: str,
        requester: UpnpRequester,
        listen_ports: Mapping[socket.AddressFamily, int] = {},
    ) -> None:
        """Initialize.

        callback_url can be a normal URL or a format string with {host} and
            {port} placeholders that will be filled based on how the device is
            connected.
        listen_ports is a mapping of IP version to the local listening port,
            used to determine callback_url for devices.
        """
        self._callback_url = callback_url
        if "{port}" in self._callback_url and not listen_ports:
            raise ValueError("callback_url format requires a listening port")
        self._requester = requester
        self.listen_ports = listen_ports
        # Legacy callback_url property assumes only one IP and port is listening
        self._listen_ip = get_local_ip()
        self._listen_port = next(iter(listen_ports.values()))

        self._subscriptions: Dict[str, SubscriptionInfo] = {}
        self._backlog: Dict[str, Tuple[Mapping, str]] = {}
        self._resubscriber_task: Optional[asyncio.Task] = None

    @property
    def callback_url(self) -> str:
        """Return callback URL on which we are callable.

        This URL should work for most cases, but makes assumptions about how
        a device will connect. Use callback_url_for_service to get a more
        specific URL.
        """
        return self._callback_url.format(host=self._listen_ip, port=self._listen_port)

    async def async_callback_url_for_service(self, service: UpnpService) -> str:
        """Determine a URL for the service to call back on.

        This can vary based on the service device's IP address.
        """
        _LOGGER.debug("Determine callback URL for: %s", service)

        # Shortcut when callback_url can be determined without connecting to the
        # service device
        if "{host}" not in self._callback_url and (
            len(self.listen_ports) == 1 or "{port}" not in self._callback_url
        ):
            return self.callback_url

        # Figure out how this host connects to the device, then infer how the
        # device can connect back
        device_host = urllib.parse.urlparse(service.device.device_url).netloc
        addr_family, local_host = await async_get_local_ip(device_host)
        port = self.listen_ports[addr_family]

        return self._callback_url.format(host=local_host, port=port)

    def sid_for_service(self, service: UpnpService) -> Optional[str]:
        """Get the service connected to SID."""
        for sid, entry in self._subscriptions.items():
            if entry.service() == service:
                return sid

        return None

    def service_for_sid(self, sid: str) -> Optional[UpnpService]:
        """Get a UpnpService for SID."""
        if sid not in self._subscriptions:
            return None

        service = self._subscriptions[sid].service()
        if not service:
            # Clear the SubscriptionInfo now that the associated service is gone
            del self._subscriptions[sid]
            return None

        return service

    def _sid_and_service(
        self, service_or_sid: Union[UpnpService, str]
    ) -> Tuple[Optional[str], Optional[UpnpService]]:
        """Resolve a SID or service to both SID and service."""
        sid: Optional[str]
        service: Optional[UpnpService]

        if isinstance(service_or_sid, UpnpService):
            service = service_or_sid
            sid = self.sid_for_service(service)
        else:
            sid = service_or_sid
            service = self.service_for_sid(sid)

        return sid, service

    async def handle_notify(self, headers: Mapping[str, str], body: str) -> HTTPStatus:
        """Handle a NOTIFY request."""
        # ensure valid request
        _LOGGER_TRAFFIC_UPNP.debug(
            "Incoming request:\nNOTIFY\n%s\n\n%s",
            "\n".join([key + ": " + value for key, value in headers.items()]),
            body,
        )
        if "NT" not in headers or "NTS" not in headers:
            _LOGGER_TRAFFIC_UPNP.debug("Sending response: %s", HTTPStatus.BAD_REQUEST)
            return HTTPStatus.BAD_REQUEST

        if (
            headers["NT"] != "upnp:event"
            or headers["NTS"] != "upnp:propchange"
            or "SID" not in headers
        ):
            _LOGGER_TRAFFIC_UPNP.debug(
                "Sending response: %s", HTTPStatus.PRECONDITION_FAILED
            )
            return HTTPStatus.PRECONDITION_FAILED
        sid = headers["SID"]

        # SID not known yet? store it in the backlog
        # Some devices don't behave nicely and send events before the SUBSCRIBE call is done.
        if sid not in self._subscriptions:
            _LOGGER.debug("Storing NOTIFY in backlog for SID: %s", sid)
            self._backlog[sid] = (
                headers,
                body,
            )

            _LOGGER_TRAFFIC_UPNP.debug("Sending response: %s", HTTPStatus.OK)
            return HTTPStatus.OK

        service = self.service_for_sid(sid)
        if not service:
            # SID was known, but service object has been deleted. Send an error
            # to the device. The service has been removed from _subscriptions,
            # this error won't happen a second time.
            _LOGGER.warning("Received NOTIFY for dead UpnpService, SID: %s", sid)

            _LOGGER_TRAFFIC_UPNP.debug(
                "Sending response: %s", HTTPStatus.PRECONDITION_FAILED
            )
            return HTTPStatus.PRECONDITION_FAILED

        # decode event and send updates to service
        changes = {}
        stripped_body = body.rstrip(" \t\r\n\0")
        el_root = DET.fromstring(stripped_body)
        for el_property in el_root.findall("./event:property", NS):
            for el_state_var in el_property:
                name = el_state_var.tag
                value = el_state_var.text or ""
                changes[name] = value

        # send changes to service
        service.notify_changed_state_variables(changes)

        _LOGGER_TRAFFIC_UPNP.debug("Sending response: %s", HTTPStatus.OK)
        return HTTPStatus.OK

    def _add_subscription(
        self, sid: str, service: UpnpService, timeout: timedelta
    ) -> SubscriptionInfo:
        """Add a subscription to the _subscriptions array.

        :return: The subscription
        """
        if sid in self._subscriptions:
            raise ValueError("Existing SID")

        def _finalize_callback(ref: weakref.ref) -> None:
            """Finalize a subscription that is being deleted.

            Remove it from our list of subscriptions.

            DO NOT unsubscribe here, we may not be in the right async context.
            Issue a warning instead.
            """
            del ref  # Unused
            try:
                self._subscriptions.pop(sid)
            except KeyError:
                # Old key that we didn't do anything with?
                _LOGGER.warning("Unknown SID to finalize %s", sid)
            else:
                _LOGGER.warning(
                    "Subscription with SID %s was not unsubscribed before deletion", sid
                )

        self._subscriptions[sid] = SubscriptionInfo(
            service=weakref.ref(service, callback=_finalize_callback),
            timeout=timeout,
            renewal_time=datetime.now() + timeout,
        )
        return self._subscriptions[sid]

    async def _async_do_subscribe(
        self,
        service: UpnpService,
        timeout: timedelta = DEFAULT_SUBSCRIPTION_TIMEOUT,
    ) -> Optional[str]:
        """
        Subscribe to a UpnpService.

        Be sure to re-subscribe before the subscription timeout passes.

        :param service UpnpService to subscribe to self
        :param timeout Timeout of subscription
        :return SID on success, None on failure
        """
        callback_url = await self.async_callback_url_for_service(service)

        _LOGGER.debug("Subscribing to: %s, callback URL: %s", service, callback_url)

        # do SUBSCRIBE request
        headers = {
            "NT": "upnp:event",
            "TIMEOUT": "Second-" + str(timeout.seconds),
            "HOST": urllib.parse.urlparse(service.event_sub_url).netloc,
            "CALLBACK": "<{}>".format(callback_url),
        }
        response_status, response_headers, _ = await self._requester.async_http_request(
            "SUBSCRIBE", service.event_sub_url, headers
        )

        # check results
        if response_status != 200:
            _LOGGER.debug("Did not receive 200, but %s", response_status)
            return None

        if "sid" not in response_headers:
            _LOGGER.debug("No SID received, aborting subscribe")
            return None

        # Device can give a different TIMEOUT header than what we have provided.
        new_timeout = timeout
        if (
            "timeout" in response_headers
            and response_headers["timeout"] != "Second-infinite"
            and "Second-" in response_headers["timeout"]
        ):
            response_timeout = response_headers["timeout"]
            timeout_seconds = int(response_timeout[7:])  # len("Second-") == 7
            new_timeout = timedelta(seconds=timeout_seconds)

        sid = response_headers["sid"]
        subscription = self._add_subscription(sid, service, new_timeout)
        _LOGGER.debug("Got SID: %s, renewal_time: %s", sid, subscription.renewal_time)

        # replay any backlog we have for this service
        if sid in self._backlog:
            _LOGGER.debug("Re-playing backlogged NOTIFY for SID: %s", sid)
            item = self._backlog[sid]
            await self.handle_notify(item[0], item[1])
            del self._backlog[sid]

        return sid

    async def _async_do_resubscribe(
        self,
        sid: str,
        service: UpnpService,
        timeout: Optional[timedelta] = None,
    ) -> Optional[str]:
        """Renew existing subscription to a UpnpService.

        :return New SID on success, None on failure
        """
        _LOGGER.debug("Resubscribing to: %s", service)

        # do SUBSCRIBE request
        if timeout is None:
            timeout = self._subscriptions[sid].timeout

        headers = {
            "HOST": urllib.parse.urlparse(service.event_sub_url).netloc,
            "SID": sid,
            "TIMEOUT": "Second-" + str(timeout.total_seconds()),
        }

        try:
            (
                response_status,
                response_headers,
                _,
            ) = await self._requester.async_http_request(
                "SUBSCRIBE", service.event_sub_url, headers
            )
        except (asyncio.TimeoutError, aiohttp.ClientError) as exc:
            _LOGGER.debug(
                "Error %s trying to resubscribe SID %s for device %s",
                exc,
                sid,
                service.device,
            )
            return None

        # check results
        if response_status != 200:
            _LOGGER.debug("Did not receive 200, but %s", response_status)
            return None

        # Devices should return the SID when re-subscribe,
        # but in case it doesn't, use the new SID.
        if "sid" in response_headers and response_headers["sid"]:
            new_sid: str = response_headers["sid"]
            if new_sid != sid:
                # Move the subscription to the right key in _subscriptions
                self._subscriptions[new_sid] = self._subscriptions[sid]
                del self._subscriptions[sid]
                sid = new_sid

        # Device can give a different TIMEOUT header than what we have provided.
        new_timeout = timeout
        if (
            "timeout" in response_headers
            and response_headers["timeout"] != "Second-infinite"
            and "Second-" in response_headers["timeout"]
        ):
            response_timeout = response_headers["timeout"]
            timeout_seconds = int(response_timeout[7:])  # len("Second-") == 7
            new_timeout = timedelta(seconds=timeout_seconds)

        subscription = self._subscriptions[sid]
        subscription.timeout = new_timeout
        subscription.renewal_time = datetime.now() + new_timeout
        _LOGGER.debug("Got SID: %s, renewal_time: %s", sid, subscription.renewal_time)

        return sid

    async def async_subscribe(
        self,
        service_or_sid: Union[UpnpService, str],
        timeout: Optional[timedelta] = None,
    ) -> Optional[str]:
        """Subscribe or resubscribe to a UpnpService.

        :param service UpnpService to subscribe to self
        :param timeout Timeout of subscription
        :return SID on success, None on failure
        """
        sid, service = self._sid_and_service(service_or_sid)
        if not service:
            _LOGGER.debug("Unknown service for %s", service_or_sid)
            return None

        if sid:
            sid = await self._async_do_resubscribe(sid, service, timeout)

        if not sid:
            # Resubscribe failed. Maybe there was no subscription or the device
            # location changed.

            # Check if there was an old subscription before (re)creating.
            old_sid = self.sid_for_service(service)

            # Try to create a new subscription
            if timeout is None:
                timeout = DEFAULT_SUBSCRIPTION_TIMEOUT
            sid = await self._async_do_subscribe(service, timeout)

            if sid and old_sid and sid != old_sid:
                # New subscription succeeded. Delete the old one, it's not needed
                del self._subscriptions[old_sid]
            elif not sid and old_sid:
                # New subscription failed. Don't delete the old one, but return
                # None to let the caller know.
                pass

        await self._update_resubscriber_task()

        return sid

    async def async_resubscribe_all(self) -> datetime:
        """Renew all current subscriptions.

        Return the next time that this must be done.
        """
        await asyncio.gather(self.async_subscribe(sid) for sid in self._subscriptions)
        return min(service.renewal_time for service in self._subscriptions.values())

    async def _resubscribe_loop(self) -> None:
        """Periodically resubscribes to current subscriptions."""
        _LOGGER.debug("_resubscribe_loop started")
        while self._subscriptions:
            next_renewal = min(
                service.renewal_time for service in self._subscriptions.values()
            )
            wait_time = next_renewal - datetime.now() - RESUBSCRIBE_THRESHOLD
            if wait_time > timedelta(0):
                await asyncio.sleep(wait_time.total_seconds())

            renewal_threshold = datetime.now() + RESUBSCRIBE_THRESHOLD

            # TODO: If resubscribe returns None, that means resubscription failed.
            # TODO: Let service know.

            await asyncio.gather(
                self.async_subscribe(sid, entry.timeout)
                for sid, entry in self._subscriptions.items()
                if entry.renewal_time < renewal_threshold
            )

        _LOGGER.debug("_resubscribe_loop ended because of no subscriptions")

    async def _update_resubscriber_task(self) -> None:
        """Start or stop the resubscriber task, depending on having subscriptions."""
        # Clear out done task to make later logic easier
        if self._resubscriber_task and self._resubscriber_task.cancelled():
            self._resubscriber_task = None

        if self._subscriptions and not self._resubscriber_task:
            _LOGGER.debug("Creating resubscribe_task")
            self._resubscriber_task = asyncio.create_task(
                self._resubscribe_loop(), name="UpnpEventHandler._resubscriber_task"
            )

        if not self._subscriptions and self._resubscriber_task:
            _LOGGER.debug("Cancelling resubscribe_task")
            self._resubscriber_task.cancel()
            await self._resubscriber_task
            self._resubscriber_task = None

    async def async_unsubscribe(
        self, service_or_sid: Union[UpnpService, str]
    ) -> Optional[str]:
        """Unsubscribe from a UpnpService.

        :return Unsubscribed SID on success, None on failure
        """
        sid, service = self._sid_and_service(service_or_sid)

        if not sid or not service:
            _LOGGER.debug("Could not determine what to unsubscribe: %s")
            return None

        _LOGGER.debug(
            "Unsubscribing service %s from device %s", service, service.device
        )

        # do UNSUBSCRIBE request
        headers = {
            "HOST": urllib.parse.urlparse(service.event_sub_url).netloc,
            "SID": sid,
        }

        try:
            response_status, _, _ = await self._requester.async_http_request(
                "UNSUBSCRIBE", service.event_sub_url, headers
            )
        except (asyncio.TimeoutError, aiohttp.ClientError) as exc:
            _LOGGER.debug(
                "Error %s trying to unsubscribe SID %s for device %s",
                exc,
                sid,
                service.device,
            )
            # No communication from the device? It's probably unsubscribed now.
            # Continue on to removing the registration.
        else:
            # check results
            if response_status != 200:
                _LOGGER.debug("Did not receive 200, but %s", response_status)
                return None

        # remove registration
        del self._subscriptions[sid]

        await self._update_resubscriber_task()

        return sid

    async def async_unsubscribe_all(self) -> None:
        """Unsubscribe all subscriptions."""
        services = self._subscriptions.copy()
        await asyncio.gather(self.async_unsubscribe(sid) for sid in services)

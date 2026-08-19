"""Decide and dispatch cross-namespace bridge requests for the discovery agent.

Each tick the agent compares the local Agnocast state with the remote
snapshots gathered over gossip. When a topic has an Agnocast endpoint locally
and the opposite-role endpoint in another namespace, a bridge is needed here
so the two reach each other through ROS 2 (DDS):

  * local publisher  + remote subscriber -> A2R bridge (publish to DDS)
  * local subscriber + remote publisher  -> R2A bridge (reinject from DDS)

Agnocast services ride on an internal request topic, so the same comparison decides them; see
``decide_service_bridges``, which emits type=DaemonService instead.

The request is sent as a ``BridgeMsg`` (type=DaemonPubSub) to the per-namespace
bridge_manager over an abstract-namespace UNIX domain socket
(``\\0agnocast_bridge_manager_<ipc_ns_inode>[_d<domain>]``).
The struct layout is mirrored here so the daemon stays decoupled from
libagnocast's C++ headers; ``agnocast_bridge_msg.hpp`` owns the source of
truth and a test asserts the size stays in sync.
"""

from dataclasses import dataclass
import errno
import os
import socket
import struct
from typing import Iterable, Optional

TOPIC_NAME_BUFFER_SIZE = 256
MESSAGE_TYPE_BUFFER_SIZE = 256
SERVICE_NAME_BUFFER_SIZE = 256
SERVICE_TYPE_BUFFER_SIZE = 256

# BridgeMsgType discriminator values (match the C++ enum).
_BRIDGE_MSG_TYPE_DAEMON_PUBSUB = 2
_BRIDGE_MSG_TYPE_DAEMON_SERVICE = 3

# Agnocast services ride on a pair of ordinary topics, so they reach us through the same gossip
# as everything else. The request topic carries the client->service direction and is the one that
# identifies the service; the per-client response topics are ignored.
SRV_REQUEST_PREFIX = '/AGNOCAST_SRV_REQUEST'
SRV_RESPONSE_PREFIX = '/AGNOCAST_SRV_RESPONSE'

# BridgeMsg wire format for a DaemonPubSub-variant message (528 bytes total).
# The C++ BridgeMsg is `uint32_t type` + union { pubsub | service | daemon_pubsub }.
# All payload variants are 4-byte aligned so no padding precedes the union.
# Senders transmit only the bytes for the active variant, so a DaemonPubSub
# message is 4 (tag) + 524 (BridgeMsgDaemonPubSubPayload) = 528 bytes.
#
#   uint32 type                             [0..3]   = _BRIDGE_MSG_TYPE_DAEMON_PUBSUB
#   BridgeMsgDaemonPubSubPayload at union offset 4..527:
#     char[256] topic_name                  [4..259]
#     char[256] type_name                   [260..515]
#     uint32    direction                   [516..519]
#     uint32    qos_depth                   [520..523]
#     bool      qos_is_transient_local      [524]
#     bool      qos_is_reliable             [525]
#     2 bytes   padding                     [526..527]
#
# Must stay in sync with bridge_msg_wire_size<BridgeMsgDaemonPubSubPayload>() == 528.
_MSG_PACK_FORMAT = '=I256s256sIIBB2x'

# BridgeMsg wire format for a DaemonService-variant message (520 bytes total).
#
#   uint32 type                             [0..3]   = _BRIDGE_MSG_TYPE_DAEMON_SERVICE
#   BridgeMsgDaemonServicePayload at union offset 4..519:
#     char[256] service_name                [4..259]
#     char[256] service_type                [260..515]
#     uint32    direction                   [516..519]
#
# Must stay in sync with bridge_msg_wire_size<BridgeMsgDaemonServicePayload>() == 520.
_SERVICE_MSG_PACK_FORMAT = '=I256s256sI'

DIRECTION_ROS2_TO_AGNOCAST = 0
DIRECTION_AGNOCAST_TO_ROS2 = 1

_BRIDGE_UDS_BASE = 'agnocast_bridge_manager'


@dataclass(frozen=True)
class ServiceBridgeRequest:
    service_name: str
    type_name: str
    direction: int
    # Selects the target bridge_manager's UDS, exactly as for BridgeRequest.
    domain_id: int = 0


@dataclass(frozen=True)
class BridgeRequest:
    topic_name: str
    type_name: str
    direction: int
    qos_depth: int
    qos_is_transient_local: bool
    qos_is_reliable: bool
    # Selects the target bridge_manager's UDS (one manager per domain); not part
    # of the wire payload, since that manager already runs in this domain.
    domain_id: int = 0


def serialize_request(req: BridgeRequest) -> bytes:
    topic = req.topic_name.encode('utf-8')[: TOPIC_NAME_BUFFER_SIZE - 1]
    type_name = req.type_name.encode('utf-8')[: MESSAGE_TYPE_BUFFER_SIZE - 1]
    return struct.pack(
        _MSG_PACK_FORMAT,
        _BRIDGE_MSG_TYPE_DAEMON_PUBSUB,
        topic,
        type_name,
        req.direction,
        req.qos_depth,
        1 if req.qos_is_transient_local else 0,
        1 if req.qos_is_reliable else 0,
    )


def serialize_service_request(req: ServiceBridgeRequest) -> bytes:
    name = req.service_name.encode('utf-8')[: SERVICE_NAME_BUFFER_SIZE - 1]
    type_name = req.type_name.encode('utf-8')[: SERVICE_TYPE_BUFFER_SIZE - 1]
    return struct.pack(
        _SERVICE_MSG_PACK_FORMAT,
        _BRIDGE_MSG_TYPE_DAEMON_SERVICE,
        name,
        type_name,
        req.direction,
    )


def _service_name_of(topic_name: str) -> Optional[str]:
    """Return the service a request topic belongs to, or None if it is not one."""
    if not topic_name.startswith(SRV_REQUEST_PREFIX):
        return None
    service_name = topic_name[len(SRV_REQUEST_PREFIX):]
    # A bare prefix names no service; anything else must be an absolute service name.
    return service_name if service_name.startswith('/') else None


def _service_type_of(type_name: str) -> Optional[str]:
    """Map a request topic's message type to the service type the bridge loader wants.

    The request topic carries ``pkg/srv/Foo_Request``; the service plugin is registered under
    ``pkg/srv/Foo``. Only that suffix is stripped, so a malformed or already-stripped type is
    reported as unusable rather than silently bridged under a name no plugin answers to.
    """
    if not type_name.endswith('_Request'):
        return None
    return type_name[: -len('_Request')]


def _resolve_types(local_state, remote_states) -> dict:
    """Resolve each ``(topic, domain)``'s message type, preferring local then any remote.

    The type must be resolved across local + *all* remotes: the remote that
    supplies the opposite-role endpoint may lack the type while another snapshot
    has it.
    """
    types = {
        (t.topic_name, t.domain_id): t.type_name for t in local_state.topics if t.type_name}
    for remote in remote_states.values():
        for t in remote.topics:
            if t.type_name:
                types.setdefault((t.topic_name, t.domain_id), t.type_name)
    return types


def decide_bridges(local_state, remote_states) -> list:
    """Return the bridge requests this namespace should issue this tick.

    ``remote_states`` maps ``(host_uuid, ipc_ns_inode)`` to AgnocastDaemonState.
    Topics match only within the same domain (a bridge never crosses domains;
    cross-domain relaying is the external domain_bridge's job), and requests are
    collapsed to one per ``(topic, domain, direction)``.
    """
    requests = {}

    local_by_topic = {(t.topic_name, t.domain_id): t for t in local_state.topics}
    types = _resolve_types(local_state, remote_states)

    for (host_uuid, ipc_ns_inode), remote in remote_states.items():
        if host_uuid == local_state.host_uuid and ipc_ns_inode == local_state.ipc_ns_inode:
            continue
        for remote_topic in remote.topics:
            # Service request/response topics are Agnocast internals. They are bridged as
            # services by decide_service_bridges(); relaying them as plain pub/sub would carry
            # the payload without the request/response correlation a service needs.
            if remote_topic.topic_name.startswith(
                    (SRV_REQUEST_PREFIX, SRV_RESPONSE_PREFIX)):
                continue

            local_topic = local_by_topic.get((remote_topic.topic_name, remote_topic.domain_id))
            if local_topic is None:
                continue

            local_pubs = [p for p in local_topic.publishers if not p.is_bridge]
            local_subs = [s for s in local_topic.subscribers if not s.is_bridge]
            remote_pubs = [p for p in remote_topic.publishers if not p.is_bridge]
            remote_subs = [s for s in remote_topic.subscribers if not s.is_bridge]

            domain_id = local_topic.domain_id
            type_name = types.get((local_topic.topic_name, domain_id))
            if not type_name:
                continue

            if local_pubs and remote_subs:
                pub = local_pubs[0]
                key = (local_topic.topic_name, domain_id, DIRECTION_AGNOCAST_TO_ROS2)
                requests.setdefault(key, BridgeRequest(
                    topic_name=local_topic.topic_name,
                    type_name=type_name,
                    direction=DIRECTION_AGNOCAST_TO_ROS2,
                    qos_depth=pub.qos_depth,
                    qos_is_transient_local=pub.qos_is_transient_local,
                    qos_is_reliable=pub.qos_is_reliable,
                    domain_id=domain_id,
                ))

            if local_subs and remote_pubs:
                sub = local_subs[0]
                key = (local_topic.topic_name, domain_id, DIRECTION_ROS2_TO_AGNOCAST)
                requests.setdefault(key, BridgeRequest(
                    topic_name=local_topic.topic_name,
                    type_name=type_name,
                    direction=DIRECTION_ROS2_TO_AGNOCAST,
                    qos_depth=sub.qos_depth,
                    qos_is_transient_local=sub.qos_is_transient_local,
                    qos_is_reliable=sub.qos_is_reliable,
                    domain_id=domain_id,
                ))

    return list(requests.values())


def decide_service_bridges(local_state, remote_states) -> list:
    """Return the service bridge requests this namespace should issue this tick.

    A service and its clients in two different IPC namespaces can only reach each other over DDS,
    which takes an R2A bridge beside the service and an A2R bridge beside the clients. Neither
    manager can build its own unaided: each one's precondition is a ROS 2 endpoint that only the
    other's bridge would create, so they wait on each other forever. We see both namespaces, so we
    ask for both halves and let the leases break the cycle.

    Roles come from the request topic, which every Agnocast service subscribes to and every
    Agnocast client publishes on:

      * local subscriber (service) + remote publisher (client)  -> R2A here
      * local publisher  (client)  + remote subscriber (service) -> A2R here

    Requests are collapsed to one per ``(service, domain, direction)``, and matched within a
    domain only, as for pub/sub.
    """
    requests = {}

    local_by_topic = {(t.topic_name, t.domain_id): t for t in local_state.topics}
    types = _resolve_types(local_state, remote_states)

    for (host_uuid, ipc_ns_inode), remote in remote_states.items():
        if host_uuid == local_state.host_uuid and ipc_ns_inode == local_state.ipc_ns_inode:
            continue
        for remote_topic in remote.topics:
            service_name = _service_name_of(remote_topic.topic_name)
            if service_name is None:
                continue

            local_topic = local_by_topic.get((remote_topic.topic_name, remote_topic.domain_id))
            if local_topic is None:
                continue

            domain_id = local_topic.domain_id
            request_type = types.get((local_topic.topic_name, domain_id))
            if not request_type:
                continue
            service_type = _service_type_of(request_type)
            if not service_type:
                continue

            # Endpoints created by a bridge are excluded on both sides: they are the effect we are
            # trying to produce, and counting them would keep every lease alive off its own bridge.
            local_pubs = [p for p in local_topic.publishers if not p.is_bridge]
            local_subs = [s for s in local_topic.subscribers if not s.is_bridge]
            remote_pubs = [p for p in remote_topic.publishers if not p.is_bridge]
            remote_subs = [s for s in remote_topic.subscribers if not s.is_bridge]

            # A local Agnocast service with a remote client: expose it on DDS via R2A.
            if local_subs and remote_pubs:
                key = (service_name, domain_id, DIRECTION_ROS2_TO_AGNOCAST)
                requests.setdefault(key, ServiceBridgeRequest(
                    service_name=service_name,
                    type_name=service_type,
                    direction=DIRECTION_ROS2_TO_AGNOCAST,
                    domain_id=domain_id,
                ))

            # A local Agnocast client with a remote service: reach it over DDS via A2R.
            if local_pubs and remote_subs:
                key = (service_name, domain_id, DIRECTION_AGNOCAST_TO_ROS2)
                requests.setdefault(key, ServiceBridgeRequest(
                    service_name=service_name,
                    type_name=service_type,
                    direction=DIRECTION_AGNOCAST_TO_ROS2,
                    domain_id=domain_id,
                ))

    return list(requests.values())


def _bridge_uds_addr(ipc_ns_inode: int, domain_id: int) -> str:
    name = '\x00' + _BRIDGE_UDS_BASE + '_' + str(ipc_ns_inode)
    if domain_id:
        name += '_d' + str(domain_id)
    return name


def send_request(uds_addr: str, payload: bytes) -> Optional[str]:
    """Send ``payload`` to ``uds_addr``; return an error string or None.

    Transient failures (bridge_manager not yet bound, receiver buffer full)
    are swallowed since the request is re-issued idempotently next tick.
    """
    transient_errnos = (
        errno.ECONNREFUSED,
        errno.ENOENT,
        errno.EAGAIN,
        errno.EWOULDBLOCK,
        errno.ENOBUFS,
    )
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
    sock.setblocking(False)
    try:
        try:
            sock.sendto(payload, uds_addr)
        except OSError as e:
            if e.errno in transient_errnos:
                return None
            return f'sendto({uds_addr!r}): {os.strerror(e.errno) if e.errno else str(e)}'
    finally:
        sock.close()
    return None


def dispatch_service_requests(
        requests: Iterable[ServiceBridgeRequest], ipc_ns_inode: int, logger=None) -> None:
    """Deliver each service request to the per-namespace bridge_manager UDS.

    Same delivery contract as ``dispatch_requests``: best-effort, re-issued idempotently every
    tick, and a missing peer never stalls the daemon.
    """
    for req in requests:
        err = send_request(
            _bridge_uds_addr(ipc_ns_inode, req.domain_id), serialize_service_request(req))
        if err is not None and logger is not None:
            logger.warn('daemon service bridge dispatch failed: %s', err)


def dispatch_requests(
        requests: Iterable[BridgeRequest], ipc_ns_inode: int, logger=None) -> None:
    """Deliver each request to the per-namespace bridge_manager UDS.

    Each request goes to the manager that owns its (IPC namespace, domain).
    The listener UDS is absent until that bridge_manager is up;
    ``send_request`` swallows ECONNREFUSED/ENOENT so a missing peer never
    stalls the daemon, and the request is re-issued idempotently next tick.
    """
    for req in requests:
        err = send_request(
            _bridge_uds_addr(ipc_ns_inode, req.domain_id), serialize_request(req))
        if err is not None and logger is not None:
            logger.warn('daemon bridge dispatch failed: %s', err)

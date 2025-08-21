# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import enum
import hashlib
import logging
import threading
from collections import defaultdict
from contextlib import contextmanager
from datetime import timedelta
from io import StringIO
from typing import Any, Callable, TYPE_CHECKING

from flask import current_app
from paramiko import RSAKey
from sqlalchemy import create_engine, event, pool
from sqlalchemy.engine import Engine
from sqlalchemy.engine.url import URL
from sshtunnel import SSHTunnelForwarder

from superset.databases.utils import make_url_safe
from superset.utils.core import get_query_source_from_request, get_user_id, QuerySource
from superset.utils.json import dumps

if TYPE_CHECKING:
    from superset.databases.ssh_tunnel.models import SSHTunnel
    from superset.models.core import Database


logger = logging.getLogger(__name__)


EngineKey = str
TunnelKey = str


class EngineModes(enum.Enum):
    # reuse existing engine if available, otherwise create a new one; this mode should
    # have a connection pool configured in the database
    POOLED = enum.auto()

    # always create a new engine for every connection; this mode will use a NullPool
    # and is the default behavior for Superset
    PER_CONNECTION = enum.auto()


class TunnelManager:
    """
    Manages SSH tunnel lifecycle and caching.

    Handles creation, caching, health checking, and cleanup of SSH tunnels
    used for database connections.
    """

    def __init__(self, mode: EngineModes) -> None:
        self.mode = mode

        self._tunnels: dict[TunnelKey, SSHTunnelForwarder] = {}
        self._tunnel_locks: defaultdict[
            TunnelKey,
            threading.Lock,
        ] = defaultdict(threading.Lock)

    def get_tunnel(self, ssh_tunnel: "SSHTunnel", uri: URL) -> SSHTunnelForwarder:
        """
        Get or create an SSH tunnel for the given configuration.
        """
        tunnel_key = self._get_tunnel_key(ssh_tunnel, uri)

        # Check if tunnel exists and is healthy
        if tunnel_key in self._tunnels:
            tunnel = self._tunnels[tunnel_key]
            if tunnel.is_active:
                return tunnel

        # Create or recreate tunnel with proper locking
        with self._tunnel_locks[tunnel_key]:
            existing_tunnel = self._tunnels.get(tunnel_key)
            if existing_tunnel and existing_tunnel.is_active:
                return existing_tunnel

            # Replace inactive or missing tunnel
            return self._replace_tunnel(tunnel_key, ssh_tunnel, uri, existing_tunnel)

    def _replace_tunnel(
        self,
        tunnel_key: str,
        ssh_tunnel: "SSHTunnel",
        uri: URL,
        old_tunnel: SSHTunnelForwarder | None,
    ) -> SSHTunnelForwarder:
        """
        Replace tunnel with proper cleanup. Assumes caller holds lock.
        """
        if old_tunnel:
            try:
                old_tunnel.stop()
            except Exception:
                logger.exception("Error stopping old tunnel")

        try:
            new_tunnel = self._create_tunnel(ssh_tunnel, uri)
            self._tunnels[tunnel_key] = new_tunnel
        except Exception:
            # Remove failed tunnel from cache
            self._tunnels.pop(tunnel_key, None)
            logger.exception("Failed to create tunnel")
            raise

        return new_tunnel

    def _get_tunnel_key(self, ssh_tunnel: "SSHTunnel", uri: URL) -> TunnelKey:
        """
        Build a unique key for the SSH tunnel by hashing parameters.
        ."""
        keys = self._get_tunnel_kwargs(ssh_tunnel, uri)
        keys_json = dumps(keys, sort_keys=True)

        # Hash the key to avoid storing sensitive information
        return hashlib.sha256(keys_json.encode()).hexdigest()

    def _create_tunnel(self, ssh_tunnel: "SSHTunnel", uri: URL) -> SSHTunnelForwarder:
        """
        Create and start a new SSH tunnel.
        """
        kwargs = self._get_tunnel_kwargs(ssh_tunnel, uri)
        tunnel = SSHTunnelForwarder(**kwargs)
        tunnel.start()

        return tunnel

    def _get_tunnel_kwargs(self, ssh_tunnel: "SSHTunnel", uri: URL) -> dict[str, Any]:
        """
        Build kwargs for SSHTunnelForwarder.
        """
        from superset.utils.ssh_tunnel import get_default_port

        backend = uri.get_backend_name()
        kwargs = {
            "ssh_address_or_host": (ssh_tunnel.server_address, ssh_tunnel.server_port),
            "ssh_username": ssh_tunnel.username,
            "remote_bind_address": (uri.host, uri.port or get_default_port(backend)),
            "local_bind_address": (ssh_tunnel.local_bind_address,),
            "debug_level": logging.getLogger("flask_appbuilder").level,
        }

        if ssh_tunnel.password:
            kwargs["ssh_password"] = ssh_tunnel.password
        elif ssh_tunnel.private_key:
            private_key_file = StringIO(ssh_tunnel.private_key)
            private_key = RSAKey.from_private_key(
                private_key_file,
                ssh_tunnel.private_key_password,
            )
            kwargs["ssh_pkey"] = private_key

        # disable keepalive if using per-connection mode
        if self.mode == EngineModes.PER_CONNECTION:
            kwargs["keepalive"] = 0

        return kwargs

    def get_active_tunnels(self) -> set[TunnelKey]:
        """
        Return set of currently active tunnel keys.
        """
        return set(self._tunnels.keys())

    def cleanup_locks(self, active_keys: set[TunnelKey]) -> int:
        """
        Remove locks for tunnels that no longer exist.

        Returns number cleaned.
        """
        abandoned_locks = set(self._tunnel_locks.keys()) - active_keys
        for key in abandoned_locks:
            self._tunnel_locks.pop(key, None)

        return len(abandoned_locks)


class CleanupManager:
    """
    Manages background cleanup threads for resource cleanup.

    Handles starting, stopping, and coordinating background cleanup
    operations to prevent memory leaks.
    """

    def __init__(self, cleanup_interval: float, cleanup_fn: Callable[[], None]) -> None:
        self.cleanup_interval = cleanup_interval
        self.cleanup_fn = cleanup_fn
        self._cleanup_thread: threading.Thread | None = None
        self._cleanup_stop_event = threading.Event()
        self._cleanup_thread_lock = threading.Lock()

    def start(self) -> None:
        """
        Start the background cleanup thread.
        """
        with self._cleanup_thread_lock:
            if self._cleanup_thread is None or not self._cleanup_thread.is_alive():
                self._cleanup_stop_event.clear()
                self._cleanup_thread = threading.Thread(
                    target=self._cleanup_worker,
                    name=f"CleanupManager-{id(self)}",
                    daemon=True,
                )
                self._cleanup_thread.start()
                logger.info(
                    f"Started cleanup thread with {self.cleanup_interval}s interval"
                )

    def stop(self) -> None:
        """
        Stop the background cleanup thread gracefully.
        """
        with self._cleanup_thread_lock:
            if self._cleanup_thread is not None and self._cleanup_thread.is_alive():
                self._cleanup_stop_event.set()
                self._cleanup_thread.join(timeout=5.0)  # 5 second timeout
                if self._cleanup_thread.is_alive():
                    logger.warning("Cleanup thread did not stop within timeout")
                else:
                    logger.info("Cleanup thread stopped")
                self._cleanup_thread = None

    def trigger_cleanup(self) -> None:
        """
        Manually trigger cleanup operation.
        """
        try:
            self.cleanup_fn()
        except Exception:
            logger.exception("Error during manual cleanup")

    def _cleanup_worker(self) -> None:
        """
        Background thread worker that periodically runs cleanup.
        """
        while not self._cleanup_stop_event.is_set():
            try:
                self.cleanup_fn()
            except Exception:
                logger.exception("Error during background cleanup")

            # Use wait() instead of sleep() to allow for immediate shutdown
            if self._cleanup_stop_event.wait(timeout=self.cleanup_interval):
                break  # Stop event was set


class EngineManager:
    """
    A manager for SQLAlchemy engines.

    This class handles the creation and management of SQLAlchemy engines, allowing them
    to be configured with connection pools and reused across requests. The default mode
    is the default behavior for Superset, where we create a new engine for every
    connection, using a NullPool. The `POOLED` mode allows for reusing of the
    engines, as well as configuring the pool through the database settings.
    """

    def __init__(
        self,
        mode: EngineModes = EngineModes.PER_CONNECTION,
        cleanup_interval: float = timedelta(minutes=5).total_seconds(),
    ) -> None:
        self.mode = mode

        # Core engine management
        self._engines: dict[EngineKey, Engine] = {}
        self._engine_locks: defaultdict[
            EngineKey,
            threading.Lock,
        ] = defaultdict(threading.Lock)

        # Composed managers for specific responsibilities
        self._tunnel_manager = TunnelManager(mode)
        self._cleanup_manager = CleanupManager(
            cleanup_interval,
            self._cleanup_abandoned_locks,
        )

    def __del__(self) -> None:
        """
        Ensure cleanup thread is stopped when the manager is destroyed.
        """
        try:
            self._cleanup_manager.stop()
        except Exception as ex:
            # Avoid exceptions during garbage collection, but log if possible
            try:
                logger.warning(f"Error stopping cleanup thread: {ex}")
            except Exception:  # noqa: S110
                # If logging fails during destruction, we can't do anything
                pass

    @contextmanager
    def get_engine(
        self,
        database: "Database",
        catalog: str | None,
        schema: str | None,
        source: QuerySource | None,
    ) -> Engine:
        """
        Context manager to get a SQLAlchemy engine.
        """
        # users can wrap the engine in their own context manager for different
        # reasons
        customization = current_app.config["ENGINE_CONTEXT_MANAGER"]

        with customization(database, catalog, schema):
            # we need to check for errors indicating that OAuth2 is needed, and
            # return the proper exception so it starts the authentication flow
            from superset.utils.oauth2 import check_for_oauth2

            with check_for_oauth2(database):
                yield self._get_engine(database, catalog, schema, source)

    def _get_engine(
        self,
        database: "Database",
        catalog: str | None,
        schema: str | None,
        source: QuerySource | None,
    ) -> Engine:
        """
        Get a specific engine, or create it if none exists.
        """
        source = source or get_query_source_from_request()
        user_id = get_user_id()

        # default behavior is to create a new engine for every connection
        if self.mode == EngineModes.PER_CONNECTION:
            return self._create_engine(
                database,
                catalog,
                schema,
                source,
                user_id,
            )

        engine_key = self._get_engine_key(
            database,
            catalog,
            schema,
            source,
            user_id,
        )

        if engine_key not in self._engines:
            with self._engine_locks[engine_key]:
                # double-checked locking to ensure thread safety and prevent unnecessary
                # engine creation
                if engine_key not in self._engines:
                    engine = self._create_engine(
                        database,
                        catalog,
                        schema,
                        source,
                        user_id,
                    )
                    self._engines[engine_key] = engine
                    self._add_disposal_listener(engine, engine_key)

        return self._engines[engine_key]

    def _get_engine_key(
        self,
        database: "Database",
        catalog: str | None,
        schema: str | None,
        source: QuerySource | None,
        user_id: int | None,
    ) -> EngineKey:
        """
        Generate a unique key for the engine based on the database and context.
        """
        uri, keys = self._get_engine_args(
            database,
            catalog,
            schema,
            source,
            user_id,
        )
        keys["uri"] = uri
        keys["source"] = source

        keys_json = dumps(keys, sort_keys=True)

        # Hash the key to avoid storing sensitive information
        return hashlib.sha256(keys_json.encode()).hexdigest()

    def _get_engine_args(
        self,
        database: "Database",
        catalog: str | None,
        schema: str | None,
        source: QuerySource | None,
        user_id: int | None,
    ) -> tuple[URL, dict[str, Any]]:
        """
        Build the almost final SQLAlchemy URI and engine kwargs.

        "Almost" final because we may still need to mutate the URI if an SSH tunnel is
        needed, since the drivers needs to connect to the tunnel instead of the original
        DB. But the local SSH port number is only available after the tunnel is created.
        """
        from superset.extensions import security_manager
        from superset.utils.feature_flag_manager import FeatureFlagManager
        from superset.utils.oauth2 import get_oauth2_access_token

        uri = make_url_safe(database.sqlalchemy_uri_decrypted)

        extra = database.get_extra(source)
        kwargs = extra.get("engine_params", {})

        # get pool class
        if self.mode == EngineModes.PER_CONNECTION or "poolclass" not in extra:
            kwargs["poolclass"] = pool.NullPool
        else:
            pools = {
                "queue": pool.QueuePool,
                "singleton": pool.SingletonThreadPool,
                "assertion": pool.AssertionPool,
                "null": pool.NullPool,
                "static": pool.StaticPool,
            }
            kwargs["poolclass"] = pools.get(extra["poolclass"], pool.QueuePool)

            # Set default pool_recycle for connection pools to prevent stale connections
            # Only set if not explicitly configured and we're using a real pool
            if "pool_recycle" not in kwargs and kwargs["poolclass"] != pool.NullPool:
                # 1 hour default - conservative but prevents most timeout issues
                kwargs["pool_recycle"] = 3600

        # update URI for specific catalog/schema
        connect_args = extra.setdefault("connect_args", {})
        uri, connect_args = database.db_engine_spec.adjust_engine_params(
            uri,
            connect_args,
            catalog,
            schema,
        )

        # get effective username
        username = database.get_effective_user(uri)

        feature_flag_manager = FeatureFlagManager()
        if username and feature_flag_manager.is_feature_enabled(
            "IMPERSONATE_WITH_EMAIL_PREFIX"
        ):
            user = security_manager.find_user(username=username)
            if user and user.email and "@" in user.email:
                username = user.email.split("@")[0]

        # update URI/kwargs for user impersonation
        if database.impersonate_user:
            oauth2_config = database.get_oauth2_config()
            access_token = (
                get_oauth2_access_token(
                    oauth2_config,
                    database.id,
                    user_id,
                    database.db_engine_spec,
                )
                if oauth2_config and user_id
                else None
            )

            uri, kwargs = database.db_engine_spec.impersonate_user(
                database,
                username,
                access_token,
                uri,
                kwargs,
            )

        # update kwargs from params stored encrupted at rest
        database.update_params_from_encrypted_extra(kwargs)

        # mutate URI
        if mutator := current_app.config["DB_CONNECTION_MUTATOR"]:
            source = source or get_query_source_from_request()
            uri, kwargs = mutator(
                uri,
                kwargs,
                username,
                security_manager,
                source,
            )

        # validate final URI
        database.db_engine_spec.validate_database_uri(uri)

        return uri, kwargs

    def _create_engine(
        self,
        database: "Database",
        catalog: str | None,
        schema: str | None,
        source: QuerySource | None,
        user_id: int | None,
    ) -> Engine:
        """
        Create the actual engine.

        This should be the only place in Superset where a SQLAlchemy engine is created,
        """
        uri, kwargs = self._get_engine_args(
            database,
            catalog,
            schema,
            source,
            user_id,
        )

        if database.ssh_tunnel:
            tunnel = self._tunnel_manager.get_tunnel(database.ssh_tunnel, uri)
            uri = uri.set(
                host=tunnel.local_bind_address[0],
                port=tunnel.local_bind_port,
            )

        try:
            engine = create_engine(uri, **kwargs)
        except Exception as ex:
            raise database.db_engine_spec.get_dbapi_mapped_exception(ex) from ex

        return engine

    def start_cleanup_thread(self) -> None:
        """Start the background cleanup thread."""
        self._cleanup_manager.start()

    def stop_cleanup_thread(self) -> None:
        """Stop the background cleanup thread gracefully."""
        self._cleanup_manager.stop()

    def cleanup(self) -> None:
        """Manually trigger cleanup of abandoned locks."""
        self._cleanup_manager.trigger_cleanup()

    def _cleanup_abandoned_locks(self) -> None:
        """
        Remove locks for engines and tunnels that no longer exist.

        This prevents memory leaks from accumulating locks in defaultdict
        when engines/tunnels are disposed outside of normal cleanup paths.
        """
        # Clean up engine locks
        active_engine_keys = set(self._engines.keys())
        abandoned_engine_locks = set(self._engine_locks.keys()) - active_engine_keys
        for key in abandoned_engine_locks:
            self._engine_locks.pop(key, None)

        if abandoned_engine_locks:
            logger.debug(
                f"Cleaned up {len(abandoned_engine_locks)} abandoned engine locks"
            )

        # Clean up tunnel locks via tunnel manager
        active_tunnel_keys = self._tunnel_manager.get_active_tunnels()
        tunnel_locks_cleaned = self._tunnel_manager.cleanup_locks(active_tunnel_keys)

        if tunnel_locks_cleaned:
            logger.debug(f"Cleaned up {tunnel_locks_cleaned} abandoned tunnel locks")

    def _add_disposal_listener(self, engine: Engine, engine_key: EngineKey) -> None:
        @event.listens_for(engine, "engine_disposed")
        def on_engine_disposed(engine_instance: Engine) -> None:
            try:
                # `pop` is atomic -- no lock needed
                if self._engines.pop(engine_key, None):
                    logger.info(f"Engine disposed and removed from cache: {engine_key}")
                    self._engine_locks.pop(engine_key, None)
            except Exception as ex:
                logger.error(
                    "Error during engine disposal cleanup for %s: %s",
                    engine_key,
                    str(ex),
                )

# -*- coding: utf-8 -*-
"""
This module contains the TorrentClient class, which is responsible for
all direct interactions with the libtorrent library.
"""
import asyncio
import logging
import libtorrent as lt
import threading
from collections import defaultdict
from concurrent.futures import Future

class LibtorrentError(Exception):
    """Custom exception to clearly pass specific errors from the libtorrent core."""
    def __init__(self, error_code):
        self.error_code = error_code
        super().__init__(f"Libtorrent error: {error_code.message()}")

class TorrentClient:
    """A wrapper around a libtorrent session to handle torrenting operations."""
    def __init__(self, loop=None):
        self.loop = loop or asyncio.get_event_loop()
        self.log = logging.getLogger("TorrentClient")
        settings = {
            'listen_interfaces': '0.0.0.0:6881', 'enable_dht': True,
            'alert_mask': lt.alert_category.error | lt.alert_category.status | lt.alert_category.storage,
            'dht_bootstrap_nodes': 'router.bittorrent.com:6881,dht.transmissionbt.com:6881,router.utorrent.com:6881',
        }
        self.ses = lt.session(settings)
        self.alert_task = None
        self._running = False
        self.dht_ready = asyncio.Event()
        self.pending_metadata = {}
        self.pending_reads = defaultdict(list)
        self.pending_reads_lock = threading.Lock()
        self.piece_download_futures = defaultdict(list)

    async def start(self):
        """Starts the torrent client's alert loop."""
        self.log.info("Starting TorrentClient...")
        self._running = True
        self.alert_task = self.loop.create_task(self._alert_loop())
        self.log.info("TorrentClient started.")

    def stop(self):
        """Stops the torrent client's alert loop."""
        self.log.info("Stopping TorrentClient...")
        self._running = False
        if self.alert_task:
            self.alert_task.cancel()
        self.log.info("TorrentClient stopped.")

    async def add_torrent(self, infohash: str):
        """Adds a torrent via infohash and returns the handle once metadata is received."""
        self.log.info(f"Adding torrent for infohash: {infohash}")
        save_dir = f"/dev/shm/{infohash}"

        meta_future = self.loop.create_future()
        self.pending_metadata[infohash] = meta_future
        trackers = [
            "udp://tracker.openbittorrent.com:80", "udp://tracker.opentrackr.org:1337/announce",
            "udp://tracker.coppersurfer.tk:6969/announce", "udp://tracker.leechers-paradise.org:6969/announce",
        ]
        magnet_uri = f"magnet:?xt=urn:btih:{infohash}&{'&'.join(['tr=' + t for t in trackers])}"
        params = lt.parse_magnet_uri(magnet_uri)
        params['save_path'] = save_dir

        handle = self.ses.add_torrent(params)

        await self.dht_ready.wait()
        self.log.debug(f"Waiting for metadata for {infohash}...")
        handle = await asyncio.wait_for(meta_future, timeout=180)

        return handle

    def remove_torrent(self, handle):
        """Removes a torrent from the session."""
        if handle and handle.is_valid():
            infohash = str(handle.info_hash())
            self.pending_metadata.pop(infohash, None)
            self.ses.remove_torrent(handle, lt.session.delete_files)
            self.log.info(f"Removed torrent: {infohash}")

    async def download_and_read_piece(self, handle, piece_index):
        """Asynchronously downloads and reads a single piece."""
        if not handle.have_piece(piece_index):
            self.log.debug(f"Piece {piece_index}: Not available, setting high priority and waiting for download.")
            handle.piece_priority(piece_index, 7)
            future = self.loop.create_future()
            self.piece_download_futures[piece_index].append(future)

            try:
                await asyncio.wait_for(future, timeout=60.0)
            except asyncio.TimeoutError:
                self.log.error(f"Piece {piece_index}: Timed out waiting for download.")
                if not handle.have_piece(piece_index):
                    raise LibtorrentError("Piece download timed out and piece not available.")

        read_future = self.loop.create_future()
        with self.pending_reads_lock:
            self.pending_reads[piece_index].append(read_future)
        handle.read_piece(piece_index)

        try:
            return await asyncio.wait_for(read_future, timeout=60.0)
        except asyncio.TimeoutError:
            self.log.error(f"Piece {piece_index}: Read operation timed out.")
            raise LibtorrentError("Piece read timed out.")

    def _handle_metadata_received(self, alert):
        infohash_str = str(alert.handle.info_hash())
        if infohash_str in self.pending_metadata and not self.pending_metadata[infohash_str].done():
            self.pending_metadata[infohash_str].set_result(alert.handle)

    def _handle_piece_finished(self, alert):
        futures = self.piece_download_futures.pop(alert.piece_index, [])
        for future in futures:
            if not future.done(): future.set_result(True)

    def _handle_dht_bootstrap(self, alert):
        if not self.dht_ready.is_set(): self.dht_ready.set()

    def _handle_read_piece(self, alert):
        with self.pending_reads_lock:
            futures = self.pending_reads.pop(alert.piece, [])
        if alert.error and alert.error.value() != 0:
            error = LibtorrentError(alert.error)
            for future in futures:
                if not future.done(): future.set_exception(error)
            return
        data = bytes(alert.buffer)
        for future in futures:
            if not future.done(): future.set_result(data)

    async def _alert_loop(self):
        """The main alert processing loop for the libtorrent session."""
        while self._running:
            try:
                alerts = self.ses.pop_alerts()
                for alert in alerts:
                    if alert.category() & lt.alert_category.error: self.log.error(f"Libtorrent Alert: {alert}")
                    else: self.log.debug(f"Libtorrent Alert: {alert}")
                    if isinstance(alert, lt.metadata_received_alert): self._handle_metadata_received(alert)
                    elif isinstance(alert, lt.piece_finished_alert): self._handle_piece_finished(alert)
                    elif isinstance(alert, lt.read_piece_alert): self._handle_read_piece(alert)
                    elif isinstance(alert, lt.dht_bootstrap_alert): self._handle_dht_bootstrap(alert)
                await asyncio.sleep(0.1)
            except asyncio.CancelledError:
                break
            except Exception:
                self.log.exception("Error in libtorrent alert loop.")

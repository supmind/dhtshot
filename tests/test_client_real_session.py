import asyncio
import pytest
import pytest_asyncio
import libtorrent as lt
import hashlib
import bencode2

from screenshot.client import TorrentClient
from screenshot.errors import TorrentClientError, MetadataTimeoutError

# Mark all tests in this file as async
pytestmark = pytest.mark.asyncio

@pytest_asyncio.fixture
async def client():
    """Fixture to provide a TorrentClient with a real libtorrent session."""
    # Set a longer timeout for real network tests
    client = TorrentClient(loop=asyncio.get_running_loop(), metadata_timeout=60)
    await client.start()
    yield client
    await client.stop()

@pytest.fixture
def valid_metadata():
    """Fixture to provide valid torrent metadata and its infohash."""
    info = {
        b'name': b'test.mp4',
        b'piece length': 16384,
        b'pieces': b'a' * 20,
        b'length': 1024  # File size in bytes
    }
    encoded_info = bencode2.bencode(info)
    infohash = hashlib.sha1(encoded_info).hexdigest()
    metadata = {b'info': info, b'announce': b'http://dummy.tracker/announce'}
    encoded_metadata = bencode2.bencode(metadata)
    return {"infohash": infohash, "metadata": encoded_metadata}

async def test_add_torrent_with_valid_metadata(client, valid_metadata):
    """
    Tests that add_torrent correctly adds a torrent when valid metadata is provided.
    """
    infohash = valid_metadata['infohash']
    metadata = valid_metadata['metadata']

    handle = await client.add_torrent(infohash, metadata=metadata)
    assert handle.is_valid()
    assert str(handle.info_hash()) == infohash

    # Check that the torrent is in the session
    torrents = client._ses.get_torrents()
    assert len(torrents) == 1
    assert str(torrents[0].info_hash()) == infohash

    # Clean up
    await client.remove_torrent(handle)


async def test_add_torrent_with_mismatched_infohash_raises_error(client, valid_metadata):
    """
    Tests that add_torrent raises a TorrentClientError if the provided infohash
    does not match the one in the metadata.
    """
    metadata = valid_metadata['metadata']
    wrong_infohash = '0' * 40

    with pytest.raises(TorrentClientError, match="提供的元数据 infohash"):
        await client.add_torrent(wrong_infohash, metadata=metadata)

async def test_add_torrent_with_invalid_metadata_raises_error(client):
    """
    Tests that add_torrent raises a TorrentClientError if the metadata is invalid.
    """
    infohash = 'a' * 40
    invalid_metadata = b'not-bencoded-data'

    with pytest.raises(TorrentClientError, match="无法解析元数据"):
        await client.add_torrent(infohash, metadata=invalid_metadata)

async def test_add_real_torrent_and_fetch_metadata(client):
    """
    Tests that the client can fetch metadata for a real, well-known torrent
    from the BitTorrent DHT using the infohash for 'Sintel'.
    """
    # This infohash was provided by the user. It corresponds to the open-source movie 'Sintel'.
    infohash = '08ada5a7a6183aae1e09d831df6748d566095a10'

    handle = None

    async def run_test():
        nonlocal handle
        handle = await client.add_torrent(infohash)
        assert handle.is_valid()

        ti = await client._execute_sync(handle.get_torrent_info)
        assert ti is not None
        assert ti.is_valid()
        assert str(ti.info_hash()) == infohash
        assert ti.name() == "Sintel"

    try:
        # Manually implement the timeout since the pytest marker is not supported
        await asyncio.wait_for(run_test(), timeout=90.0)
    except asyncio.TimeoutError:
        pytest.fail("The real torrent metadata fetch timed out after 90 seconds.")
    finally:
        if handle:
            await client.remove_torrent(handle)

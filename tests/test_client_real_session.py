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
    client = TorrentClient(loop=asyncio.get_running_loop())
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

async def test_add_torrent_without_metadata_times_out(client):
    """
    Tests that add_torrent without metadata (and without peers) times out.
    This confirms we are hitting the magnet link path.
    """
    # This infohash doesn't exist, so metadata download should time out.
    infohash = 'f' * 40

    # Set a very short timeout for the test
    client.metadata_timeout = 1

    with pytest.raises(MetadataTimeoutError):
        await client.add_torrent(infohash)

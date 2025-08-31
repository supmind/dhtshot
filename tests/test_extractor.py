import pytest
import struct
from io import BytesIO

from screenshot.extractor import KeyframeExtractor

# Helper function to create a box
def create_box(box_type: bytes, payload: bytes) -> bytes:
    """Creates a simple MP4 box with a 32-bit size."""
    size = 8 + len(payload)
    return struct.pack('>I4s', size, box_type) + payload

def test_h264_extractor_parses_correctly():
    """
    Tests the KeyframeExtractor with a manually constructed, valid 'moov' atom.
    This test verifies that all major tables (stsd, stts, stss, stsz, stsc, stco)
    are parsed correctly to build a complete and accurate sample map.
    """
    # --- Build a fake moov atom for testing ---

    # avcC: Decoder configuration payload
    avcc_payload = b'\x01\x64\x00\x1f\xff\xe1\x00\x19\x67\x64\x00\x1f\xac\xd9\x41\xe0\x22\x07\xb0\x8b\x00\x00\x03\x00\x01\x00\x00\x03\x00\x3d\x08\x01\x01\x00\x04\x68\xee\x3c\x80'
    avcc_box = create_box(b'avcC', avcc_payload)

    # avc1: Sample description for H.264 video. Contains a 78-byte header, then the avcC box.
    avc1_header = b'\x00' * 78
    avc1_payload = avc1_header + avcc_box
    avc1_box = create_box(b'avc1', avc1_payload)

    # stsd: Sample Description Box. Contains version, flags, entry_count, and then the avc1 box.
    stsd_payload = b'\x00\x00\x00\x00' + struct.pack('>I', 1) + avc1_box
    stsd_box = create_box(b'stsd', stsd_payload)

    # stts: Time-to-Sample Box (2 samples, each with a duration of 1000)
    stts_payload = b'\x00\x00\x00\x00' + struct.pack('>I', 1) + struct.pack('>II', 2, 1000)
    stts_box = create_box(b'stts', stts_payload)

    # stss: Sync Sample Box (keyframes). Sample 1 is a keyframe.
    stss_payload = b'\x00\x00\x00\x00' + struct.pack('>I', 1) + struct.pack('>I', 1)
    stss_box = create_box(b'stss', stss_payload)

    # stsz: Sample Size Box (2 samples, with sizes 100 and 200 respectively)
    stsz_payload = b'\x00\x00\x00\x00' + struct.pack('>I', 0) + struct.pack('>I', 2) + struct.pack('>II', 100, 200)
    stsz_box = create_box(b'stsz', stsz_payload)

    # stsc: Sample to Chunk Box (1 chunk containing 2 samples)
    stsc_payload = b'\x00\x00\x00\x00' + struct.pack('>I', 1) + struct.pack('>III', 1, 2, 1)
    stsc_box = create_box(b'stsc', stsc_payload)

    # stco: Chunk Offset Box (1 chunk located at file offset 1024)
    stco_payload = b'\x00\x00\x00\x00' + struct.pack('>I', 1) + struct.pack('>I', 1024)
    stco_box = create_box(b'stco', stco_payload)

    # Assemble the stbl (Sample Table) Box
    stbl_payload = stsd_box + stts_box + stss_box + stsz_box + stsc_box + stco_box
    stbl_box = create_box(b'stbl', stbl_payload)

    # Assemble the higher-level boxes (minf, mdia, trak)
    minf_payload = stbl_box
    minf_box = create_box(b'minf', minf_payload)
    hdlr_payload = b'\x00\x00\x00\x00' + b'\x00\x00\x00\x00' + b'vide' + b'\x00' * 12
    hdlr_box = create_box(b'hdlr', hdlr_payload)
    # mdhd: Media Header Box (timescale 90000). Version 0 of mdhd box has timescale at offset 12.
    mdhd_payload = b'\x00' * 12 + struct.pack('>I', 90000) + b'\x00' * 4
    mdhd_box = create_box(b'mdhd', mdhd_payload)
    mdia_payload = mdhd_box + hdlr_box + minf_box
    mdia_box = create_box(b'mdia', mdia_payload)
    # A valid 'trak' box must contain a 'tkhd' (track header) box.
    tkhd_box = create_box(b'tkhd', b'\x00' * 84)
    trak_payload = tkhd_box + mdia_box
    trak_box = create_box(b'trak', trak_payload)
    # A valid 'moov' box should contain a 'mvhd' (movie header) box.
    mvhd_box = create_box(b'mvhd', b'\x00' * 100)
    moov_payload = mvhd_box + trak_box
    moov_data = create_box(b'moov', moov_payload)

    # --- Run the extractor ---
    extractor = KeyframeExtractor(moov_data)

    # --- Assertions ---
    assert extractor.codec_name == 'h264'
    assert extractor.mode == 'avc1'
    assert extractor.nal_length_size == 4  # from avcc payload: (\xff & 3) + 1 = 4
    assert extractor.extradata == avcc_payload
    assert extractor.timescale == 90000
    assert len(extractor.samples) == 2
    assert len(extractor.keyframes) == 1

    keyframe = extractor.keyframes[0]
    assert keyframe.index == 0
    assert keyframe.sample_index == 1
    assert keyframe.pts == 0
    assert keyframe.timescale == 90000

    sample1 = extractor.samples[0]
    assert sample1.is_keyframe is True
    assert sample1.index == 1
    assert sample1.offset == 1024
    assert sample1.size == 100
    assert sample1.pts == 0

    sample2 = extractor.samples[1]
    assert sample2.is_keyframe is False
    assert sample2.index == 2
    assert sample2.offset == 1024 + 100
    assert sample2.size == 200
    assert sample2.pts == 1000

def test_extractor_handles_avcc_after_other_box():
    """
    This test validates the fix for 'avc1' parsing where 'avcC' is not the first sub-box.
    It places a dummy 'free' box before the 'avcC' box. The old logic, which assumed
    'avcC' was the first box, would fail this test.
    """
    free_box = create_box(b'free', b'some padding')
    avcc_payload = b'\x01\x64\x00\x1f\xff\xe1\x00\x09\x67\x42\xc0\x1e\xda\x02\x80\xf6\x84\x00\x00\x03\x00\x04\x00\x00\x03\x00\xf0\x88\x46\xa0'
    avcc_box = create_box(b'avcC', avcc_payload)

    avc1_header = b'\x00' * 78
    avc1_payload = avc1_header + free_box + avcc_box
    avc1_box = create_box(b'avc1', avc1_payload)

    stsd_payload = b'\x00\x00\x00\x00' + struct.pack('>I', 1) + avc1_box
    stsd_box = create_box(b'stsd', stsd_payload)

    # Create a minimal moov structure around this specific stsd
    stbl_payload = stsd_box + create_box(b'stts', b'\x00'*8) + create_box(b'stss', b'\x00'*8) + create_box(b'stsz', b'\x00'*12) + create_box(b'stsc', b'\x00'*8) + create_box(b'stco', b'\x00'*8)
    stbl_box = create_box(b'stbl', stbl_payload)
    minf_box = create_box(b'minf', stbl_box)
    hdlr_box = create_box(b'hdlr', b'\x00\x00\x00\x00' + b'\x00\x00\x00\x00' + b'vide' + b'\x00' * 12)
    mdhd_box = create_box(b'mdhd', b'\x00'*24)
    mdia_payload = mdhd_box + hdlr_box + minf_box
    mdia_box = create_box(b'mdia', mdia_payload)
    tkhd_box = create_box(b'tkhd', b'\x00' * 84) # Dummy tkhd
    trak_payload = tkhd_box + mdia_box
    trak_box = create_box(b'trak', trak_payload)
    mvhd_box = create_box(b'mvhd', b'\x00' * 100) # Dummy mvhd
    moov_payload = mvhd_box + trak_box
    moov_data = create_box(b'moov', moov_payload)

    # --- Run the extractor ---
    extractor = KeyframeExtractor(moov_data)

    # --- Assertions ---
    # The key assertion is that we found the avcC box correctly despite the 'free' box.
    assert extractor.mode == 'avc1'
    assert extractor.extradata == avcc_payload


def test_extractor_missing_stbl_box():
    """
    Tests that the extractor raises a ValueError if the 'stbl' box is missing,
    as it's essential for parsing sample information.
    """
    # Create a moov structure without the 'stbl' box
    minf_payload = create_box(b'dinf', b'') # Some other box, but not stbl
    minf_box = create_box(b'minf', minf_payload)
    hdlr_box = create_box(b'hdlr', b'\x00\x00\x00\x00' + b'\x00\x00\x00\x00' + b'vide' + b'\x00' * 12)
    mdhd_box = create_box(b'mdhd', b'\x00'*24)
    mdia_payload = mdhd_box + hdlr_box + minf_box
    mdia_box = create_box(b'mdia', mdia_payload)
    tkhd_box = create_box(b'tkhd', b'\x00' * 84)
    trak_payload = tkhd_box + mdia_box
    trak_box = create_box(b'trak', trak_payload)
    mvhd_box = create_box(b'mvhd', b'\x00' * 100)
    moov_payload = mvhd_box + trak_box
    moov_data = create_box(b'moov', moov_payload)

    with pytest.raises(ValueError, match="在视频轨道中未找到 'stbl' Box"):
        KeyframeExtractor(moov_data)


def test_extractor_no_keyframes():
    """
    Tests the extractor with a valid stream that contains samples but no keyframes.
    This is simulated by having an 'stss' box with an entry count of 0.
    """
    # Use the structure from the main test, but replace the 'stss' box
    # stss: Sync Sample Box with 0 entries
    stss_payload = b'\x00\x00\x00\x00' + struct.pack('>I', 0)
    stss_box = create_box(b'stss', stss_payload)

    # Re-build a minimal moov with this 'stss' box
    avcc_payload = b'\x01\x64\x00\x1f\xff\xe1\x00\x19\x67\x64\x00\x1f\xac\xd9\x41\xe0\x22\x07\xb0\x8b\x00\x00\x03\x00\x01\x00\x00\x03\x00\x3d\x08\x01\x01\x00\x04\x68\xee\x3c\x80'
    avcc_box = create_box(b'avcC', avcc_payload)
    avc1_header = b'\x00' * 78
    avc1_payload = avc1_header + avcc_box
    avc1_box = create_box(b'avc1', avc1_payload)
    stsd_payload = b'\x00\x00\x00\x00' + struct.pack('>I', 1) + avc1_box
    stsd_box = create_box(b'stsd', stsd_payload)
    stts_payload = b'\x00\x00\x00\x00' + struct.pack('>I', 1) + struct.pack('>II', 2, 1000)
    stts_box = create_box(b'stts', stts_payload)
    stsz_payload = b'\x00\x00\x00\x00' + struct.pack('>I', 0) + struct.pack('>I', 2) + struct.pack('>II', 100, 200)
    stsz_box = create_box(b'stsz', stsz_payload)
    stsc_payload = b'\x00\x00\x00\x00' + struct.pack('>I', 1) + struct.pack('>III', 1, 2, 1)
    stsc_box = create_box(b'stsc', stsc_payload)
    stco_payload = b'\x00\x00\x00\x00' + struct.pack('>I', 1) + struct.pack('>I', 1024)
    stco_box = create_box(b'stco', stco_payload)

    stbl_payload = stsd_box + stts_box + stss_box + stsz_box + stsc_box + stco_box
    stbl_box = create_box(b'stbl', stbl_payload)

    minf_payload = stbl_box
    minf_box = create_box(b'minf', minf_payload)
    hdlr_box = create_box(b'hdlr', b'\x00\x00\x00\x00' + b'\x00\x00\x00\x00' + b'vide' + b'\x00' * 12)
    mdhd_box = create_box(b'mdhd', b'\x00' * 12 + struct.pack('>I', 90000) + b'\x00' * 4)
    mdia_payload = mdhd_box + hdlr_box + minf_box
    mdia_box = create_box(b'mdia', mdia_payload)
    tkhd_box = create_box(b'tkhd', b'\x00' * 84)
    trak_payload = tkhd_box + mdia_box
    trak_box = create_box(b'trak', trak_payload)
    mvhd_box = create_box(b'mvhd', b'\x00' * 100)
    moov_payload = mvhd_box + trak_box
    moov_data = create_box(b'moov', moov_payload)

    extractor = KeyframeExtractor(moov_data)

    assert len(extractor.samples) == 2
    assert len(extractor.keyframes) == 0


def test_extractor_handles_avc3_in_band():
    """
    Tests that the extractor correctly identifies an 'avc3' stream (in-band SPS/PPS)
    where 'avc1' is not present. It should set the mode to 'avc3' and extradata to None.
    """
    # avc3 box is similar to avc1 but without the avcC sub-box.
    avc3_header = b'\x00' * 78
    avc3_box = create_box(b'avc3', avc3_header)

    stsd_payload = b'\x00\x00\x00\x00' + struct.pack('>I', 1) + avc3_box
    stsd_box = create_box(b'stsd', stsd_payload)

    # Build minimal moov around this
    stbl_payload = stsd_box + create_box(b'stts', b'\x00'*8) + create_box(b'stss', b'\x00'*8) + create_box(b'stsz', b'\x00'*12) + create_box(b'stsc', b'\x00'*8) + create_box(b'stco', b'\x00'*8)
    stbl_box = create_box(b'stbl', stbl_payload)
    minf_box = create_box(b'minf', stbl_box)
    hdlr_box = create_box(b'hdlr', b'\x00\x00\x00\x00' + b'\x00\x00\x00\x00' + b'vide' + b'\x00' * 12)
    mdhd_box = create_box(b'mdhd', b'\x00'*24)
    mdia_payload = mdhd_box + hdlr_box + minf_box
    mdia_box = create_box(b'mdia', mdia_payload)
    tkhd_box = create_box(b'tkhd', b'\x00' * 84)
    trak_payload = tkhd_box + mdia_box
    trak_box = create_box(b'trak', trak_payload)
    mvhd_box = create_box(b'mvhd', b'\x00' * 100)
    moov_payload = mvhd_box + trak_box
    moov_data = create_box(b'moov', moov_payload)

    extractor = KeyframeExtractor(moov_data)

    assert extractor.mode == 'avc3'
    assert extractor.extradata is None


def test_extractor_no_video_track():
    """
    Tests that the extractor raises ValueError if the moov atom contains no video track.
    This is simulated by having a track with a 'soun' (sound) handler instead of 'vide'.
    """
    # Create a track with a 'soun' handler
    sound_hdlr_payload = b'\x00\x00\x00\x00' + b'\x00\x00\x00\x00' + b'soun' + b'\x00' * 12
    hdlr_box = create_box(b'hdlr', sound_hdlr_payload)
    mdia_payload = hdlr_box # minimal mdia
    mdia_box = create_box(b'mdia', mdia_payload)
    tkhd_box = create_box(b'tkhd', b'\x00' * 84)
    trak_payload = tkhd_box + mdia_box
    trak_box = create_box(b'trak', trak_payload)

    mvhd_box = create_box(b'mvhd', b'\x00' * 100)
    moov_payload = mvhd_box + trak_box
    moov_data = create_box(b'moov', moov_payload)

    with pytest.raises(ValueError, match="在 'moov' Box 中未找到有效的视频轨道"):
        KeyframeExtractor(moov_data)

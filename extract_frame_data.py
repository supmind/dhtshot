import asyncio
from screenshot.pymp4parse import F4VParser
from screenshot.service import KeyframeInfo
import pprint

async def main():
    with open("keyframe_video.mp4", "rb") as f:
        data = f.read()

    moov_box = None
    for box in F4VParser.parse(bytes_input=data):
        if box.header.box_type == 'moov':
            moov_box = box
            break

    if moov_box:
        avc1_box = F4VParser.find_child_box(moov_box, ['trak', 'mdia', 'minf', 'stbl', 'stsd', 'avc1'])
        if avc1_box:
            avcC_box = F4VParser.find_child_box(avc1_box, ['avcC'])
            if avcC_box:
                sps = avcC_box.sps_list[0]
                pps = avcC_box.pps_list[0]

                print(f"SPS: {sps.hex()}")
                print(f"PPS: {pps.hex()}")
            else:
                print("avcC box not found in avc1")
        else:
            print("avc1 box not found")

        stbl_box = F4VParser.find_child_box(moov_box, ['trak', 'mdia', 'minf', 'stbl'])
        stss = getattr(stbl_box, 'stss', None)
        stsz = getattr(stbl_box, 'stsz', None)
        stco = getattr(stbl_box, 'stco', None)

        keyframe_sample = stss.entries[0]
        sample_size = stsz.entries[keyframe_sample - 1] if stsz.sample_size == 0 else stsz.sample_size
        chunk_offset = stco.entries[0]

        with open("keyframe_video.mp4", "rb") as f:
            f.seek(chunk_offset)
            # The sample size in the stsz box is the size of the NAL unit, not the whole frame.
            # The frame also includes a 4-byte length prefix.
            keyframe_data = f.read(sample_size + 4)

        print(f"Keyframe Data: {keyframe_data.hex()}")

    else:
        print("moov box not found")

if __name__ == "__main__":
    asyncio.run(main())

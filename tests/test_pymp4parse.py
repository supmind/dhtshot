# -*- coding: utf-8 -*-
import os
import pytest
import struct
from screenshot.pymp4parse import F4VParser, BoxHeader, UnImplementedBox

@pytest.mark.skip(reason="需要一个有效的测试视频文件来提取 'moov' box")
def test_parse_moov_box(moov_data):
    """
    核心单元测试：测试 F4VParser 能否正确解析一个真实的 'moov' box。
    """
    # 使用 F4VParser 解析 'moov' box 的字节数据
    # next() 用于获取生成器产生的第一个（也是唯一一个）box
    moov_box = next(F4VParser.parse(bytes_input=moov_data))

    # --- 断言验证 ---
    assert moov_box is not None
    assert moov_box.header.box_type == 'moov'
    assert len(moov_box.children) > 0, "moov box 应该包含子 box"

    # 验证 'mvhd' (Movie Header) box 是否存在且被正确添加为属性
    assert hasattr(moov_box, 'mvhd'), "解析后的 moov_box 对象应有名为 'mvhd' 的属性"

    # 验证是否至少有一个 'trak' (Track) box
    assert hasattr(moov_box, 'trak'), "moov_box 应至少包含一个 'trak' box"

    # 获取第一个 track box 进行更深入的检查
    trak_box = moov_box.trak[0] if isinstance(moov_box.trak, list) else moov_box.trak
    assert trak_box.header.box_type == 'trak'

    # 验证 track box 的内部结构
    assert hasattr(trak_box, 'tkhd'), "trak_box 应包含 'tkhd' (Track Header)"
    assert hasattr(trak_box, 'mdia'), "trak_box 应包含 'mdia' (Media)"

    mdia_box = trak_box.mdia
    assert hasattr(mdia_box, 'mdhd'), "'mdia' box 应包含 'mdhd' (Media Header)"
    assert hasattr(mdia_box, 'hdlr'), "'mdia' box 应包含 'hdlr' (Handler Reference)"
    assert hasattr(mdia_box, 'minf'), "'mdia' box 应包含 'minf' (Media Information)"

    minf_box = mdia_box.minf
    assert hasattr(minf_box, 'stbl'), "'minf' box 应包含 'stbl' (Sample Table)"

    # 'stbl' (Sample Table) 是最关键的部分，包含了所有样本信息
    stbl_box = minf_box.stbl
    assert stbl_box.header.box_type == 'stbl'

    # 验证 stbl 中所有必要的子 box 都被成功解析
    expected_stbl_children = ['stsd', 'stts', 'stss', 'stsc', 'stsz', 'stco']
    for child_type in expected_stbl_children:
        assert hasattr(stbl_box, child_type), f"'stbl' box 缺少必要的子 box: '{child_type}'"

    # 对几个关键的表进行内容断言
    stss_box = stbl_box.stss # 关键帧表
    assert stss_box.header.box_type == 'stss'
    assert len(stss_box.entries) > 0, "'stss' box (关键帧表) 不应为空"

    stco_box = stbl_box.stco # 块偏移表
    assert stco_box.header.box_type == 'stco'
    assert len(stco_box.entries) > 0, "'stco' box (块偏移表) 不应为空"

@pytest.mark.skip(reason="需要一个有效的测试视频文件来提取 'moov' box")
def test_find_child_box(moov_data):
    """
    测试辅助函数 find_child_box 是否能正确地在解析树中导航。
    """
    moov_box = next(F4VParser.parse(bytes_input=moov_data))

    # 定义一个到 'stbl' box 的有效路径
    path_to_stbl = ['trak', 'mdia', 'minf', 'stbl']

    stbl_box = F4VParser.find_child_box(moov_box, path_to_stbl)
    assert stbl_box is not None, "使用有效路径应能找到 'stbl' box"
    assert stbl_box.header.box_type == 'stbl'

    # 定义一个无效路径
    path_to_nothing = ['trak', 'foo', 'bar']

    nothing_box = F4VParser.find_child_box(moov_box, path_to_nothing)
    assert nothing_box is None, "使用无效路径应返回 None"

def test_parse_junk_data():
    """
    测试当输入为无效或垃圾数据时，解析器能否优雅地处理。
    """
    junk_data = b'\x01\x02\x03\x04\x05\x06\x07\x08'
    # list() 会消耗整个生成器
    boxes = list(F4VParser.parse(bytes_input=junk_data))
    # 不应该解析出任何 box，也不应该抛出异常
    assert len(boxes) == 0

def test_parse_incomplete_header():
    """
    测试当 box 头部不完整时解析器的行为。
    """
    # 一个只有4字节的头部，不足以构成一个完整的 header
    incomplete_data = b'\x00\x00\x00\x10'
    boxes = list(F4VParser.parse(bytes_input=incomplete_data))
    assert len(boxes) == 0

def test_parse_box_with_invalid_size():
    """
    测试当 box 声明的大小超出实际数据范围时解析器的行为。
    """
    # 声明大小为 100，但实际数据只有 20 字节
    invalid_size_data = b'\x00\x00\x00\x64' + b'test' + b'\x00' * 16
    # 解析器应该能够捕获这个错误并安全地停止，而不是崩溃
    boxes = list(F4VParser.parse(bytes_input=invalid_size_data))
    # 经过错误处理逻辑的改进，解析器现在会中止并返回一个空列表
    assert len(boxes) == 0

def test_parse_co64_box():
    """
    测试能否正确解析 'co64' (64-bit Chunk Offset) box。
    """
    # 构建一个包含 co64 的 stbl box 的字节流
    # Fullbox header (version 0, flags 0) + entry_count (1)
    co64_content = b'\x00\x00\x00\x00' + b'\x00\x00\x00\x01'
    # 64-bit offset
    co64_content += b'\x00\x00\x00\x01\x00\x00\x00\x02'

    # co64_header (size, type)
    co64_header = struct.pack('>I', len(co64_content) + 8) + b'co64'

    box = next(F4VParser.parse(bytes_input=co64_header + co64_content))

    assert box.header.box_type == 'co64'
    assert len(box.entries) == 1
    assert box.entries[0] == 0x100000002

def test_parse_unimplemented_box():
    """
    测试解析器遇到未知 box 类型时的行为。
    """
    # 创建一个自定义的 box 'test'
    test_box_data = b'\x00\x00\x00\x10' + b'test' + b'data' * 2
    box = next(F4VParser.parse(bytes_input=test_box_data))

    assert isinstance(box, UnImplementedBox)
    assert box.header.box_type == 'test'
    assert box.header.box_size == 8

def test_parsing_with_corrupt_entry_count():
    """
    测试当 box 内容（如 entry_count）损坏时解析器的鲁棒性。
    """
    # stts box 声明有 1000 个条目，但数据不足
    # Fullbox header + entry_count (1000)
    stts_content = b'\x00\x00\x00\x00' + b'\x00\x00\x03\xe8'
    # 但只提供一个条目的数据
    stts_content += b'\x00\x00\x00\x01\x00\x00\x00\x01'

    stts_header = struct.pack('>I', len(stts_content) + 8) + b'stts'

    # 解析器应该能捕获 ReadError 并安全地跳过这个 box
    # 我们用一个正常的 box 跟在后面，以确保解析可以继续
    ftyp_box_data = b'\x00\x00\x00\x18ftypisom\x00\x00\x02\x00iso2avc1mp41'

    boxes = list(F4VParser.parse(bytes_input=stts_header + stts_content + ftyp_box_data))

    # 第一个 box 解析失败，被跳过
    # 第二个 box (ftyp) 应该被成功解析
    assert len(boxes) == 1
    assert boxes[0].header.box_type == 'ftyp'

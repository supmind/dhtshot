# -*- coding: utf-8 -*-
"""
ScreenshotGenerator 的单元测试。
这些测试广泛使用 unittest.mock.patch 来模拟 `av` (PyAV) 库，
从而使我们能在不实际进行视频解码的情况下测试生成器的逻辑。
"""
import pytest
import av as real_av # 导入真实的 av 库，以便引用其异常类型
from unittest.mock import MagicMock, patch, AsyncMock

from screenshot.generator import ScreenshotGenerator

@pytest.fixture
def generator():
    """提供一个带有模拟事件循环的 ScreenshotGenerator 实例，用于所有测试。"""
    return ScreenshotGenerator(loop=MagicMock())

@patch('os.makedirs')
@patch('screenshot.generator.av')
def test_save_frame_to_jpeg(mock_av, mock_makedirs, generator):
    """
    测试 `_save_frame_to_jpeg` 辅助函数是否正确调用了文件系统和图像库。
    """
    # GIVEN: 一个模拟的 PyAV 帧和图像对象
    mock_frame = MagicMock()
    mock_image = MagicMock()
    mock_frame.to_image.return_value = mock_image

    # WHEN: 调用保存函数
    with patch.object(mock_image, 'save') as mock_save:
        generator._save_frame_to_jpeg(mock_frame, "infohash", "timestamp")

        # THEN: 应创建输出目录，并以正确的名称保存图像
        mock_makedirs.assert_called_once_with(generator.output_dir, exist_ok=True)
        mock_save.assert_called_once_with(f"{generator.output_dir}/infohash_timestamp.jpg")


@pytest.mark.asyncio
@patch('screenshot.generator.av')
async def test_generate_calls_executor(mock_av, generator):
    """
    测试主 `generate` 异步方法是否正确地将其工作委托给线程池执行器。
    """
    # GIVEN: 一个带有模拟执行器的生成器
    generator.loop.run_in_executor = AsyncMock()

    # WHEN: 调用 generate 方法
    await generator.generate(
        extradata=b'extra', packet_data=b'packet',
        infohash_hex='infohash', timestamp_str='ts'
    )

    # THEN: `run_in_executor` 应被调用，并传入正确的参数
    generator.loop.run_in_executor.assert_awaited_once_with(
        None, generator._decode_and_save, b'extra', b'packet', 'infohash', 'ts'
    )

@patch('screenshot.generator.av')
def test_decode_and_save_success_with_extradata(mock_av, generator):
    """
    测试 `_decode_and_save` 在提供了 extradata ('avc1' 模式) 时的成功路径。
    """
    # GIVEN: 一个模拟的解码器上下文，它将成功返回一个帧
    mock_codec_ctx = mock_av.CodecContext.create.return_value
    mock_frame = MagicMock()
    mock_codec_ctx.decode.return_value = [mock_frame]
    mock_packet_instance = mock_av.Packet.return_value

    # WHEN: 调用解码和保存函数
    with patch.object(generator, '_save_frame_to_jpeg') as mock_save:
        generator._decode_and_save(b'extradata', b'packetdata', 'infohash', 'ts')

        # THEN: 应创建 h264 解码器，设置 extradata，并最终保存帧
        mock_av.CodecContext.create.assert_called_once_with('h264', 'r')
        assert mock_codec_ctx.extradata == b'extradata'
        mock_av.Packet.assert_called_once_with(b'packetdata')
        mock_codec_ctx.decode.assert_called_once_with(mock_packet_instance)
        mock_save.assert_called_once_with(mock_frame, 'infohash', 'ts')

@patch('screenshot.generator.av')
def test_decode_and_save_success_no_extradata(mock_av, generator):
    """
    测试 `_decode_and_save` 在未提供 extradata ('avc3' 模式) 时的成功路径。
    """
    # GIVEN: 一个模拟的解码器上下文
    mock_codec_ctx = mock_av.CodecContext.create.return_value
    mock_frame = MagicMock()
    mock_codec_ctx.decode.return_value = [mock_frame]

    # 清理可能从上一个测试泄露的模拟状态
    if hasattr(mock_codec_ctx, 'extradata'):
        delattr(mock_codec_ctx, 'extradata')


    # WHEN: 调用解码和保存函数
    with patch.object(generator, '_save_frame_to_jpeg') as mock_save:
        generator._decode_and_save(None, b'packetdata', 'infohash', 'ts')

        # THEN: 不应设置 extradata 属性，但仍应成功解码和保存
        assert not hasattr(mock_codec_ctx, 'extradata'), "在没有提供 extradata 时，不应设置该属性"
        mock_codec_ctx.decode.assert_called_once()
        mock_save.assert_called_once_with(mock_frame, 'infohash', 'ts')

@patch('screenshot.generator.av')
def test_decode_and_save_decoding_fails_with_flush(mock_av, generator):
    """
    测试当第一次解码调用未返回帧时，代码是否会尝试一次“刷新”解码。
    """
    # GIVEN: 一个模拟的解码器，第一次调用返回空，第二次（刷新）调用返回一个帧
    mock_codec_ctx = mock_av.CodecContext.create.return_value
    mock_packet = mock_av.Packet.return_value
    mock_codec_ctx.decode.side_effect = [[], [MagicMock()]]

    # WHEN: 调用解码和保存函数
    with patch.object(generator.log, 'warning') as mock_log_warning, \
         patch.object(generator, '_save_frame_to_jpeg') as mock_save:
        generator._decode_and_save(None, b'packet', 'infohash', 'ts')

        # THEN: 应记录警告，尝试刷新，并最终保存帧
        mock_log_warning.assert_called_once_with("第一次解码未返回帧，尝试发送一个空的刷新包...")
        assert mock_codec_ctx.decode.call_count == 2
        mock_codec_ctx.decode.assert_any_call(mock_packet)
        mock_codec_ctx.decode.assert_any_call(None) # 验证刷新调用
        mock_save.assert_called_once()

@patch('screenshot.generator.av')
def test_decode_and_save_decoding_fails_completely(mock_av, generator):
    """
    测试当解码和刷新尝试都未能返回任何帧时，是否会记录错误且不保存任何内容。
    """
    # GIVEN: 一个模拟的解码器，两次调用都返回空
    mock_codec_ctx = mock_av.CodecContext.create.return_value
    mock_codec_ctx.decode.side_effect = [[], []]

    # WHEN: 调用解码和保存函数
    with patch.object(generator.log, 'error') as mock_log_error, \
         patch.object(generator, '_save_frame_to_jpeg') as mock_save:
        generator._decode_and_save(None, b'packet', 'infohash', 'ts')

        # THEN: 应记录错误，并且不应调用保存函数
        mock_log_error.assert_called_once_with("解码失败：解码器未能从时间戳 ts 的数据包中解码出任何帧。")
        mock_save.assert_not_called()

@patch('screenshot.generator.av')
def test_decode_and_save_invalid_data_error(mock_av, generator):
    """
    测试当 PyAV 抛出 `InvalidDataError` 时，是否能被正确捕获和记录。
    """
    # GIVEN: 一个模拟的解码器，它会抛出 InvalidDataError
    # 我们让模拟的 av.error 指向真实的 av.error，以便 `except` 语句能正确捕获它
    mock_av.error = real_av.error
    mock_codec_ctx = mock_av.CodecContext.create.return_value
    mock_codec_ctx.decode.side_effect = real_av.error.InvalidDataError(1, "Invalid data")

    # WHEN: 调用解码和保存函数
    with patch.object(generator.log, 'error') as mock_log_error:
        generator._decode_and_save(None, b'packet', 'infohash', 'ts')

        # THEN: 应记录一个特定的错误信息
        mock_log_error.assert_called_once()
        assert "解码器报告无效数据" in mock_log_error.call_args[0][0]

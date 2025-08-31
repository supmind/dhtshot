# -*- coding: utf-8 -*-
"""
这是一个演示如何使用 `ScreenshotService` 的示例脚本。

它执行以下操作：
1. 配置日志记录以显示来自服务的详细输出。
2. 定义一个回调函数，用于异步处理来自服务的任务状态更新。
3. 创建并启动 `ScreenshotService`。
4. 向服务提交一系列预定义的截图任务（使用 torrent infohash）。
5. 等待服务运行一段时间或直到用户按下 Ctrl+C。
6. 优雅地停止服务。

要运行此示例，请确保已安装所有依赖项：
`pip install -r requirements.txt`
然后执行：
`python example.py`
"""
import asyncio
import signal
import logging

from screenshot.service import ScreenshotService
from screenshot.config import Settings

# --- 1. 配置日志 ---
# 设置详细的日志记录，以便观察服务的内部工作状态。
# 这对于调试和理解程序的执行流程至关重要。
logging.basicConfig(
    level=logging.DEBUG,  # 记录 DEBUG 及以上级别的所有日志
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
# 为了避免过多的底层细节，可以将某些组件的日志级别调高。
# 例如，取消下面这行注释可以减少来自 TorrentClient 的冗长日志。
# logging.getLogger("TorrentClient").setLevel(logging.INFO)


async def main():
    """主异步函数，用于设置和运行服务。"""
    loop = asyncio.get_running_loop()

    # --- 2. 定义状态回调函数 ---
    # ScreenshotService 在任务完成、失败或遇到可恢复错误时会调用此函数。
    # 在实际应用中，您可以在此函数中实现复杂的逻辑，例如：
    # - 将成功结果（如截图路径）保存到数据库。
    # - 向用户发送任务完成或失败的通知。
    # - 将可恢复的失败任务（及其 resume_data）放入一个队列中以便稍后重试。
    async def task_status_callback(status: str, infohash: str, message: str, **kwargs):
        """一个示范性的回调函数，用于处理来自服务的所有任务状态更新。"""
        print("\n" + "="*20 + " 任务状态更新 " + "="*20)
        print(f"  - Infohash: {infohash}")
        print(f"  - 状态:     {status.upper()}")
        print(f"  - 消息:     {message}")
        if status == 'recoverable_failure':
            # 这是一个重要的状态，表示任务因为临时问题（如超时）失败，但可以恢复。
            # 服务会提供 `resume_data`，应将其保存下来用于重试。
            resume_data = kwargs.get('resume_data')
            print(f"  - 可恢复:   是 (恢复数据存在: {resume_data is not None})")
            print("  - 操作建议: 保存 resume_data 并稍后使用 submit_task(infohash, resume_data=...) 重试。")
        print("="*54 + "\n")

    # --- 3. 创建并运行截图服务 ---
    # 创建配置实例。Pydantic-Settings 会自动从环境变量或 .env 文件加载配置。
    settings = Settings()

    # 将配置实例和我们定义的回调函数传递给服务。
    service = ScreenshotService(settings=settings, loop=loop, status_callback=task_status_callback)
    await service.run()

    # --- 4. 提交示例任务 ---
    # 这里我们向服务提交一些截图任务。
    # 在实际应用中，这些 infohash 可能来自数据库、API 请求或其他来源。
    # 这些都是合法的、有源的开源电影 torrents，用于安全测试。
    infohashes_to_submit = [
        # Sintel - Blender Foundation Open Movie
        "ea8988740fa7a125e1593ce26ee5f24c29478f13",
        # Big Buck Bunny - Blender Foundation Open Movie
        "dd8255ecdc7ca55fb0bbf81323d87062db1f6d1c",
        # Cosmos Laundromat - Blender Foundation Open Movie
        "c9e15763f722f23e98a29decdfae341b98d53056",
        # Tears of Steel - Blender Foundation Open Movie
        "209c8226b299b308beaf2b9cd3fb49212dbd13ec",
        # 一个无效的 infohash，用于测试错误处理流程
        "0000000000000000000000000000000000000000",
    ]

    for infohash in infohashes_to_submit:
        # `submit_task` 是一个异步函数，它将任务放入队列后立即返回。
        # 服务将在后台并发处理这些任务。
        await service.submit_task(infohash)

    print("\n✅ 截图服务正在运行中。")
    print(f"已提交 {len(infohashes_to_submit)} 个任务。服务将在后台处理它们。")
    print(f"🖼️  生成的截图将保存在 '{service.settings.output_dir}' 目录中。")
    print("🚦 按下 Ctrl+C 来优雅地停止服务。")

    # --- 5. 等待关闭信号 ---
    # 我们创建一个 Future，并设置一个信号处理器。当用户按下 Ctrl+C (SIGINT) 时，
    # Future 的结果会被设置，从而解除下面的 `await` 阻塞。
    stop_event = asyncio.Future()
    loop.add_signal_handler(signal.SIGINT, stop_event.set_result, None)

    # 为了防止示例脚本无限期运行，我们还设置了一个总超时。
    timeout_seconds = 300  # 5 分钟
    print(f"⏳ 此脚本将在 {timeout_seconds} 秒后自动超时，或在您按下 Ctrl+C 时停止。")

    try:
        # 等待 stop_event 被设置，或者超时。
        await asyncio.wait_for(stop_event, timeout=timeout_seconds)
    except asyncio.TimeoutError:
        print(f"\n⌛ 脚本运行达到 {timeout_seconds} 秒超时。")

    # --- 6. 清理和关闭 ---
    print("\n🛑 正在异步停止服务，请稍候...")
    # 调用 `stop()` 会向所有内部组件发送关闭信号，并等待它们完成清理。
    await service.stop()
    print("✅ 服务已成功停止。")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n程序被用户强制中断。")

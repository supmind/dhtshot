import asyncio
import signal
import logging

from screenshot.service import ScreenshotService

# 配置日志以查看服务的输出
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
# 将服务日志级别设置为 INFO 以减少冗余信息
# logging.getLogger("ScreenshotService").setLevel(logging.INFO)


async def main():
    loop = asyncio.get_running_loop()

    # --- 状态回调定义 ---
    # 在这里你可以处理截图任务的结果。
    # 你可以将结果保存到数据库、发送通知，
    # 或者重新提交一个可恢复失败的任务。
    async def task_status_callback(status: str, infohash: str, message: str, **kwargs):
        """示范回调函数，用于处理任务状态更新。"""
        print("\n--- 任务状态更新 ---")
        print(f"  Infohash: {infohash}")
        print(f"  状态: {status}")
        print(f"  消息: {message}")
        if status == 'recoverable_failure':
            print("  此任务稍后可以使用提供的 resume_data 进行恢复。")
            # 在实际应用中，你可能会将此 resume_data 保存到数据库
            # 并有一个机制来重试任务。
            # 在此示例中，我们只打印一个确认信息。
            resume_data = kwargs.get('resume_data')
            print(f"  恢复数据可用: {resume_data is not None}")
        print("--------------------------\n")


    # 创建并运行截图服务
    # 我们将回调函数传递给服务。
    service = ScreenshotService(loop=loop, status_callback=task_status_callback)
    await service.run()

    # --- 示例任务 ---
    # 向服务提交一些截图任务。
    # 用户应将这些替换为真实的 infohash。
    infohashes_to_submit = [
        # Sintel - 一部开源电影
        "ea8988740fa7a125e1593ce26ee5f24c29478f13",
        # Big Buck Bunny - 另一部开源电影
        "dd8255ecdc7ca55fb0bbf81323d87062db1f6d1c",
        # Cosmos Laundromat - 开源电影
        "c9e15763f722f23e98a29decdfae341b98d53056",
        # Tears of Steel - 开源电影
        "209c8226b299b308beaf2b9cd3fb49212dbd13ec",
    ]

    for infohash in infohashes_to_submit:
        await service.submit_task(infohash)


    print("\n截图服务正在运行。")
    print(f"已提交 {len(infohashes_to_submit)} 个示例任务。服务现在将处理它们。")
    print(f"截图将保存在 '{service.output_dir}' 目录中。")
    print("按 Ctrl+C 停止服务。")

    # 等待正常关闭或超时
    stop = asyncio.Future()
    loop.add_signal_handler(signal.SIGINT, stop.set_result, None)

    timeout = 300  # 秒
    print(f"最多运行 {timeout} 秒。将自动停止。")

    try:
        await asyncio.wait_for(stop, timeout=timeout)
    except asyncio.TimeoutError:
        print(f"\n{timeout} 秒后超时。")

    # 清理
    print("\n正在停止服务...")
    service.stop()
    print("服务已停止。")


if __name__ == "__main__":
    asyncio.run(main())

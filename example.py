# -*- coding: utf-8 -*-
import asyncio
import logging
from functools import partial

# 导入服务和结果类
from screenshot.service import (
    ScreenshotService,
    AllSuccessResult,
    FatalErrorResult,
    PartialSuccessResult,
)

# 配置日志记录
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
# 为 ScreenshotService 设置更详细的日志级别
logging.getLogger("ScreenshotService").setLevel(logging.DEBUG)


# --- 回调处理器 ---
def my_result_handler(result, state: dict):
    """
    当任务完成时，此函数被调用。
    它使用一个共享的状态字典来管理结果和事件。
    """
    print("\n--- 收到任务结果 ---")
    if isinstance(result, AllSuccessResult):
        print(f"✅  成功: {result.infohash} 的任务已完成。")
        print(f"   本次运行生成的截图数量: {result.screenshots_count}")
        state['status'] = 'done'
    elif isinstance(result, PartialSuccessResult):
        print(f"⚠️  部分成功: {result.infohash} 的任务失败，但可恢复。")
        print(f"   原因: {result.reason}")
        print(f"   本次运行生成的截图数量: {result.screenshots_count}")
        # 存储 resume_data 以便后续重试
        state['resume_data'] = result.resume_data
        state['status'] = 'partial'
    elif isinstance(result, FatalErrorResult):
        print(f"❌  致命错误: {result.infohash} 的任务失败且无法恢复。")
        print(f"   原因: {result.reason}")
        state['status'] = 'fatal'
    else:
        print(f"🧐 未知的结果类型: {type(result)}")
        state['status'] = 'fatal'

    # 发出信号，表示此任务步骤已完成
    state['event'].set()


async def main():
    """主异步函数，用于运行服务和提交任务。"""
    loop = asyncio.get_running_loop()
    service = ScreenshotService(loop=loop)
    await service.run()

    # Sintel - 一个已知可用的种子，用于测试截图功能
    infohash_to_submit = "08ada5a7a6183aae1e09d831df6748d566095a10"

    # --- 运行 1: 正常执行 ---
    print(f"\n--- 第一次提交任务 ---")
    state1 = {'event': asyncio.Event()}
    handler1 = partial(my_result_handler, state=state1)
    await service.submit_task(infohash=infohash_to_submit, on_complete=handler1)
    await state1['event'].wait()

    # --- 运行 2: 如果第一次运行部分失败，则演示恢复功能 ---
    if state1.get('status') == 'partial':
        print(f"\n--- 任务部分失败。使用 resume_data 重新提交 ---")
        print(f"   正在使用的恢复数据: {state1.get('resume_data')}")
        state2 = {'event': asyncio.Event()}
        handler2 = partial(my_result_handler, state=state2)
        await service.submit_task(
            infohash=infohash_to_submit,
            on_complete=handler2,
            resume_data=state1.get('resume_data')
        )
        await state2['event'].wait()

    print("\n--- 示例执行完毕 ---")

    # 清理资源
    print("正在停止服务...")
    service.stop()
    print("服务已停止。")


if __name__ == "__main__":
    try:
        # 要测试基于超时的恢复功能，你可以像这样向服务注入一个测试用的超时时间：
        # service.TIMEOUT_FOR_TESTING = 5
        # 这将触发 PartialSuccessResult 的执行路径。
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n程序退出。")

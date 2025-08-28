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
    level=logging.INFO, # 默认级别设置为 INFO
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
# 为我们的应用程序专门创建一个 logger
log = logging.getLogger("ScreenshotExample")
log.setLevel(logging.DEBUG) # 将示例应用的日志级别设置为 DEBUG

# 为底层的服务和客户端也设置详细的日志级别，方便调试
logging.getLogger("ScreenshotService").setLevel(logging.DEBUG)
logging.getLogger("TorrentClient").setLevel(logging.DEBUG)


# --- 回调处理器 ---
def my_result_handler(result, state: dict, run_name: str):
    """
    当任务完成时，此函数被调用。
    它使用一个共享的状态字典来管理结果和事件。
    :param result: 服务返回的结果对象。
    :param state: 用于在运行之间共享状态的字典。
    :param run_name: 描述当前运行的名称，用于日志记录。
    """
    log.info(f"--- [{run_name}] 收到任务结果 ---")
    if isinstance(result, AllSuccessResult):
        log.info(f"✅  [{run_name}] 成功: {result.infohash} 的任务已完成。")
        log.info(f"   本次运行生成的截图数量: {result.screenshots_count}")
        state['status'] = 'done'
    elif isinstance(result, PartialSuccessResult):
        log.warning(f"⚠️  [{run_name}] 部分成功: {result.infohash} 的任务失败，但可恢复。")
        log.warning(f"   原因: {result.reason}")
        log.warning(f"   本次运行生成的截图数量: {result.screenshots_count}")
        # 存储 resume_data 以便后续重试
        state['resume_data'] = result.resume_data
        state['status'] = 'partial'
    elif isinstance(result, FatalErrorResult):
        log.error(f"❌  [{run_name}] 致命错误: {result.infohash} 的任务失败且无法恢复。")
        log.error(f"   原因: {result.reason}")
        state['status'] = 'fatal'
    else:
        log.error(f"🧐 [{run_name}] 未知的结果类型: {type(result)}")
        state['status'] = 'fatal'

    # 发出信号，表示此任务步骤已完成
    log.debug(f"[{run_name}] 的事件已设置，通知主流程继续。")
    state['event'].set()


async def main():
    """主异步函数，用于运行服务和提交任务。"""
    loop = asyncio.get_running_loop()
    # 为了进行集成测试，我们可以减小客户端的缓存大小，以便更快地触发 LRU 逻辑。
    # 这有助于我们验证死锁问题是否已解决。
    service = ScreenshotService(loop=loop, client_lru_cache_size=3)
    await service.run()

    # Sintel - 一个已知可用的种子，用于测试截图功能
    infohash_to_submit = "08ada5a7a6183aae1e09d831df6748d566095a10"

    # --- 运行 1: 正常执行 ---
    log.info(f"\n--- [运行 1] 第一次提交任务 ---")
    state1 = {'event': asyncio.Event()}
    # 使用 functools.partial 来预先填充 run_name 参数
    handler1 = partial(my_result_handler, state=state1, run_name="运行 1")
    await service.submit_task(infohash=infohash_to_submit, on_complete=handler1)
    log.info(f"[运行 1] 任务已提交，正在等待结果...")
    await state1['event'].wait()
    log.info(f"[运行 1] 已收到结果并处理完毕。")


    # --- 运行 2: 如果第一次运行部分失败，则演示恢复功能 ---
    if state1.get('status') == 'partial':
        log.info(f"\n--- [运行 2] 任务部分失败。使用 resume_data 重新提交 ---")
        log.info(f"   正在使用的恢复数据: {state1.get('resume_data')}")
        state2 = {'event': asyncio.Event()}
        handler2 = partial(my_result_handler, state=state2, run_name="运行 2")
        await service.submit_task(
            infohash=infohash_to_submit,
            on_complete=handler2,
            resume_data=state1.get('resume_data')
        )
        log.info(f"[运行 2] 恢复任务已提交，正在等待结果...")
        await state2['event'].wait()
        log.info(f"[运行 2] 已收到恢复任务的结果并处理完毕。")

    log.info("\n--- 示例执行完毕 ---")

    # 清理资源
    log.info("正在停止服务...")
    service.stop()
    log.info("服务已停止。")


if __name__ == "__main__":
    try:
        # 要测试基于超时的恢复功能，你可以像这样向服务注入一个测试用的超时时间：
        # service.TIMEOUT_FOR_TESTING = 5
        # 这将触发 PartialSuccessResult 的执行路径。
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n程序退出。")

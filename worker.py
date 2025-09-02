# -*- coding: utf-8 -*-
"""
本文件是截图服务的工作节点 (Worker) 的主程序。

工作节点的职责:
1.  启动时向中央调度器 (Scheduler) 注册自己。
2.  定期向调度器发送心跳，以表明自己处于活动状态。
3.  循环地向调度器请求新任务。
4.  将获取到的任务交给本地的 `ScreenshotService` 进行处理。
5.  通过回调函数，将 `ScreenshotService` 的处理结果（截图文件、任务状态）报告给调度器。
6.  响应终止信号 (SIGINT, SIGTERM)，实现优雅停机。
"""
import asyncio
import aiohttp
import uuid
import logging
import base64
import os
import signal
from functools import partial

from screenshot.service import ScreenshotService
from screenshot.config import Settings

# --- 全局配置 ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
log = logging.getLogger("Worker")

SCHEDULER_URL = os.getenv("SCHEDULER_URL", "http://127.0.0.1:8000")
WORKER_ID = f"worker-{uuid.uuid4()}"
HEARTBEAT_INTERVAL = 30  # 秒
POLL_INTERVAL = 10 # 秒

async def register_worker(session: aiohttp.ClientSession) -> bool:
    """向调度器注册当前工作节点。"""
    url = f"{SCHEDULER_URL}/workers/register"
    payload = {"worker_id": WORKER_ID, "status": "idle"}
    try:
        async with session.post(url, json=payload) as response:
            if response.status == 200:
                log.info(f"工作节点 {WORKER_ID} 注册成功。")
                return True
            log.error(f"注册工作节点失败。状态码: {response.status}, 响应: {await response.text()}")
            return False
    except aiohttp.ClientError as e:
        log.error(f"注册工作节点时发生连接错误: {e}")
        return False

# --- 回调函数定义 ---

async def on_screenshot_saved(session: aiohttp.ClientSession, infohash: str, filepath: str):
    """
    当 ScreenshotService 成功保存一个截图文件时被调用的回调函数。
    它负责将截图文件上传到调度器。
    """
    log.info(f"[{infohash}] 截图已保存至 {filepath}。准备上传...")
    filename = os.path.basename(filepath)
    upload_url = f"{SCHEDULER_URL}/screenshots/{infohash}"
    try:
        with open(filepath, "rb") as f:
            data = aiohttp.FormData()
            data.add_field('file', f, filename=filename, content_type='image/jpeg')
            async with session.post(upload_url, data=data) as response:
                if response.status == 200:
                    log.info(f"[{infohash}] 成功上传并记录截图 {filename}。")
                else:
                    log.error(f"[{infohash}] 上传/记录截图 {filename} 失败。状态码: {response.status}, 响应: {await response.text()}")
    except aiohttp.ClientError as e:
        log.error(f"[{infohash}] 上传截图 {filename} 时发生连接错误: {e}")
    except FileNotFoundError:
        log.error(f"[{infohash}] 找不到待上传的文件: {filepath}")

async def on_task_finished(session: aiohttp.ClientSession, status: str, infohash: str, message: str, **kwargs):
    """
    当 ScreenshotService 完成一个任务（无论成功或失败）时被调用的回调函数。
    它负责将任务的最终状态报告给调度器。
    """
    log.info(f"[{infohash}] 任务完成，状态: {status.upper()}。消息: {message}")
    url = f"{SCHEDULER_URL}/tasks/{infohash}/status"
    payload = {"status": status, "message": str(message), "resume_data": kwargs.get("resume_data")}

    # 如果存在 resume_data，需要对其内部的 extradata (bytes) 进行 Base64 编码，使其能被 JSON 序列化。
    if resume_data := payload.get("resume_data"):
        if extradata := resume_data.get("extractor_info", {}).get("extradata"):
            if isinstance(extradata, bytes):
                resume_data["extractor_info"]["extradata"] = base64.b64encode(extradata).decode('ascii')

    try:
        async with session.post(url, json=payload) as response:
            if response.status == 200:
                log.info(f"[{infohash}] 成功报告最终状态。")
            else:
                log.error(f"[{infohash}] 报告最终状态失败。状态码: {response.status}, 响应: {await response.text()}")
    except aiohttp.ClientError as e:
        log.error(f"[{infohash}] 报告最终状态时发生连接错误: {e}")

async def send_heartbeat(session: aiohttp.ClientSession, service: ScreenshotService):
    """一个独立的协程，定期向调度器发送心跳。"""
    url = f"{SCHEDULER_URL}/workers/heartbeat"
    while True:
        status = "busy" if service.active_tasks else "idle"
        payload = {"worker_id": WORKER_ID, "status": status}
        try:
            async with session.post(url, json=payload) as response:
                if response.status == 200:
                    log.info(f"心跳发送成功 (状态: {status})。")
                else:
                    log.warning(f"发送心跳失败。状态码: {response.status}")
        except aiohttp.ClientError as e:
            log.warning(f"发送心跳时发生连接错误: {e}")
        await asyncio.sleep(HEARTBEAT_INTERVAL)

async def main():
    """工作节点的主函数，负责初始化和协调所有服务。"""
    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    # 注册信号处理器以实现优雅停机
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop_event.set)

    async with aiohttp.ClientSession() as session:
        if not await register_worker(session):
            log.error("无法向调度器注册，程序退出。")
            return

        settings = Settings()
        # 使用 functools.partial 将 session 对象绑定到回调函数上
        status_cb = partial(on_task_finished, session)
        screenshot_cb = partial(on_screenshot_saved, session)

        # 初始化并运行核心的截图服务
        service = ScreenshotService(settings=settings, loop=loop, status_callback=status_cb, screenshot_callback=screenshot_cb)
        await service.run()
        log.info("ScreenshotService 已在后台运行。")

        # 启动心跳任务
        heartbeat_task = asyncio.create_task(send_heartbeat(session, service))
        log.info("心跳任务已启动。")

        # --- 主轮询循环 ---
        log.info("启动主任务轮询循环...")
        while not stop_event.is_set():
            try:
                # 仅当本地服务空闲时才请求新任务，实现一次只处理一个任务
                if not service.active_tasks:
                    url = f"{SCHEDULER_URL}/tasks/next?worker_id={WORKER_ID}"
                    async with session.get(url, timeout=15) as response:
                        if response.status == 200:
                            if task_data := await response.json():
                                log.info(f"收到新任务: {task_data['infohash']}，提交到本地服务。")
                                if metadata := task_data.get('metadata'):
                                    task_data['metadata'] = base64.b64decode(metadata)
                                await service.submit_task(**task_data)
                        elif response.status == 204:
                            log.info("调度器中无可用任务，等待中...")

                # 等待下一次轮询或等待停机信号
                await asyncio.sleep(POLL_INTERVAL)

            except aiohttp.ClientError as e:
                log.error(f"连接调度器时出错: {e}，将在 {HEARTBEAT_INTERVAL} 秒后重试。")
                await asyncio.sleep(HEARTBEAT_INTERVAL)
            except asyncio.CancelledError:
                break # 响应停机信号

        log.info("收到停机信号，正在停止服务...")
        heartbeat_task.cancel()
        await service.stop()
        log.info("工作节点已成功关闭。")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("程序被手动中断。")

# -*- coding: utf-8 -*-
"""
本文件是截图服务的工作节点 (Worker) 的主程序。
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
from config import Settings

# --- 全局配置 ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
log = logging.getLogger("Worker")

WORKER_ID = f"worker-{uuid.uuid4()}"
HEARTBEAT_INTERVAL = 30
POLL_INTERVAL = 10

async def register_worker(session: aiohttp.ClientSession, scheduler_url: str) -> bool:
    """向调度器注册当前工作节点。"""
    url = f"{scheduler_url}/workers/register"
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

async def on_screenshot_saved(session: aiohttp.ClientSession, scheduler_url: str, infohash: str, filepath: str):
    """当 ScreenshotService 成功保存一个截图文件时被调用的回调函数。"""
    log.info(f"[{infohash}] 截图已保存至 {filepath}。准备上传...")
    filename = os.path.basename(filepath)
    upload_url = f"{scheduler_url}/screenshots/{infohash}"
    try:
        with open(filepath, "rb") as f:
            data = aiohttp.FormData()
            data.add_field('file', f, filename=filename, content_type='image/jpeg')
            async with session.post(upload_url, data=data) as response:
                if response.status != 200:
                    log.error(f"[{infohash}] 上传/记录截图 {filename} 失败。状态码: {response.status}, 响应: {await response.text()}")
    except aiohttp.ClientError as e:
        log.error(f"[{infohash}] 上传截图 {filename} 时发生连接错误: {e}")
    except FileNotFoundError:
        log.error(f"[{infohash}] 找不到待上传的文件: {filepath}")

async def on_task_finished(session: aiohttp.ClientSession, scheduler_url: str, status: str, infohash: str, message: str, **kwargs):
    """当 ScreenshotService 完成一个任务时被调用的回调函数。"""
    log.info(f"[{infohash}] 任务完成，状态: {status.upper()}。消息: {message}")
    url = f"{scheduler_url}/tasks/{infohash}/status"
    payload = {"status": status, "message": str(message), "resume_data": kwargs.get("resume_data")}

    if resume_data := payload.get("resume_data"):
        if extradata := resume_data.get("extractor_info", {}).get("extradata"):
            if isinstance(extradata, bytes):
                resume_data["extractor_info"]["extradata"] = base64.b64encode(extradata).decode('ascii')

    try:
        async with session.post(url, json=payload) as response:
            if response.status != 200:
                log.error(f"[{infohash}] 报告最终状态失败。状态码: {response.status}, 响应: {await response.text()}")
    except aiohttp.ClientError as e:
        log.error(f"[{infohash}] 报告最终状态时发生连接错误: {e}")

async def send_heartbeat(session: aiohttp.ClientSession, service: ScreenshotService, scheduler_url: str):
    """一个独立的协程，定期向调度器发送心跳。"""
    url = f"{scheduler_url}/workers/heartbeat"
    while True:
        status = "busy" if service.active_tasks else "idle"
        payload = {"worker_id": WORKER_ID, "status": status}
        try:
            async with session.post(url, json=payload) as response:
                if response.status != 200:
                    log.warning(f"发送心跳失败。状态码: {response.status}")
        except aiohttp.ClientError as e:
            log.warning(f"发送心跳时发生连接错误: {e}")
        await asyncio.sleep(HEARTBEAT_INTERVAL)

async def main(session: aiohttp.ClientSession):
    """工作节点的主函数，负责初始化和协调所有服务。"""
    settings = Settings()
    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop_event.set)

    if not await register_worker(session, settings.scheduler_url):
        log.error("无法向调度器注册，程序退出。")
        return

    status_cb = partial(on_task_finished, session, settings.scheduler_url)
    screenshot_cb = partial(on_screenshot_saved, session, settings.scheduler_url)

    service = ScreenshotService(settings=settings, loop=loop, status_callback=status_cb, screenshot_callback=screenshot_cb)
    await service.run()
    log.info("ScreenshotService 已在后台运行。")

    heartbeat_task = asyncio.create_task(send_heartbeat(session, service, settings.scheduler_url))
    log.info("心跳任务已启动。")

    log.info("启动主任务轮询循环...")
    while not stop_event.is_set():
        try:
            if not service.active_tasks:
                url = f"{settings.scheduler_url}/tasks/next?worker_id={WORKER_ID}"
                async with session.get(url, timeout=15) as response:
                    if response.status == 200:
                        if task_data := await response.json():
                            log.info(f"收到新任务: {task_data['infohash']}，提交到本地服务。")
                            if metadata := task_data.get('metadata'):
                                task_data['metadata'] = base64.b64decode(metadata)
                            await service.submit_task(**task_data)
                    elif response.status == 204:
                        log.info("调度器中无可用任务，等待中...")

            await asyncio.sleep(POLL_INTERVAL)

        except aiohttp.ClientError as e:
            log.error(f"连接调度器时出错: {e}，将在 {HEARTBEAT_INTERVAL} 秒后重试。")
            await asyncio.sleep(HEARTBEAT_INTERVAL)
        except asyncio.CancelledError:
            break

    log.info("收到停机信号，正在停止服务...")
    heartbeat_task.cancel()
    await service.stop()
    log.info("工作节点已成功关闭。")

if __name__ == "__main__":
    async def run():
        async with aiohttp.ClientSession() as session:
            await main(session)

    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        log.info("程序被手动中断。")

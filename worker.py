# -*- coding: utf-8 -*-
"""
本文件是截图服务的工作节点 (Worker) 的主程序。
它包含与调度器通信的客户端、回调逻辑以及运行截图服务的主循环。
"""
import asyncio
import aiohttp
import uuid
import logging
import base64
import os
import signal
from functools import partial
from typing import Optional, Any, Dict

from screenshot.service import ScreenshotService
from config import Settings

# --- 全局配置 ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
log = logging.getLogger("Worker")

WORKER_ID = f"worker-{uuid.uuid4()}"
HEARTBEAT_INTERVAL = 30
POLL_INTERVAL = 10


class SchedulerAPIClient:
    """
    一个封装了与调度器所有 API 交互的客户端。
    这使得网络逻辑集中化，并简化了测试（通过 mock 这个类而不是网络请求）。
    """
    def __init__(self, session: aiohttp.ClientSession, scheduler_url: str):
        self._session = session
        self._url = scheduler_url

    async def register(self, worker_id: str) -> bool:
        """向调度器注册当前工作节点。"""
        url = f"{self._url}/workers/register"
        payload = {"worker_id": worker_id, "status": "idle"}
        try:
            async with self._session.post(url, json=payload) as response:
                if response.status == 200:
                    log.info(f"工作节点 {worker_id} 注册成功。")
                    return True
                log.error(f"注册工作节点失败。状态码: {response.status}, 响应: {await response.text()}")
                return False
        except aiohttp.ClientError as e:
            log.error(f"注册工作节点时发生连接错误: {e}")
            return False

    async def get_next_task(self, worker_id: str) -> Optional[Dict[str, Any]]:
        """向调度器请求下一个待处理的任务。"""
        url = f"{self._url}/tasks/next?worker_id={worker_id}"
        try:
            async with self._session.get(url, timeout=15) as response:
                if response.status == 200:
                    return await response.json()
                elif response.status == 204:
                    log.info("调度器中无可用任务，等待中...")
                    return None
        except aiohttp.ClientError as e:
            log.error(f"连接调度器获取任务时出错: {e}")
        return None

    async def upload_screenshot(self, infohash: str, filepath: str):
        """上传一个已生成的截图文件。"""
        log.info(f"[{infohash}] 截图已保存至 {filepath}。准备上传...")
        filename = os.path.basename(filepath)
        upload_url = f"{self._url}/screenshots/{infohash}"
        try:
            with open(filepath, "rb") as f:
                data = aiohttp.FormData()
                data.add_field('file', f, filename=filename, content_type='image/jpeg')
                async with self._session.post(upload_url, data=data) as response:
                    if response.status != 200:
                        log.error(f"[{infohash}] 上传/记录截图 {filename} 失败。状态码: {response.status}, 响应: {await response.text()}")
        except aiohttp.ClientError as e:
            log.error(f"[{infohash}] 上传截图 {filename} 时发生连接错误: {e}")
        except FileNotFoundError:
            log.error(f"[{infohash}] 找不到待上传的文件: {filepath}")

    async def update_task_status(self, infohash: str, status: str, message: str, resume_data: Optional[dict]):
        """向调度器报告任务的最终状态。"""
        log.info(f"[{infohash}] 任务完成，状态: {status.upper()}。消息: {message}")
        url = f"{self._url}/tasks/{infohash}/status"

        if resume_data:
            if extractor_info := resume_data.get("extractor_info"):
                if extradata := extractor_info.get("extradata"):
                    if isinstance(extradata, bytes):
                        extractor_info["extradata"] = base64.b64encode(extradata).decode('ascii')

        payload = {"status": status, "message": str(message), "resume_data": resume_data}
        try:
            async with self._session.post(url, json=payload) as response:
                if response.status != 200:
                    log.error(f"[{infohash}] 报告最终状态失败。状态码: {response.status}, 响应: {await response.text()}")
        except aiohttp.ClientError as e:
            log.error(f"[{infohash}] 报告最终状态时发生连接错误: {e}")

    async def send_heartbeat(self, worker_id: str, service: ScreenshotService, processed_tasks_count: int):
        """定期发送心跳以保持工作节点活动状态。"""
        url = f"{self._url}/workers/heartbeat"
        status = "busy" if service.active_tasks else "idle"

        # 从 ScreenshotService 实例动态获取队列大小和活动任务数
        queue_size = service.get_queue_size()
        # 正在执行的任务数 = 总任务数 - 队列中的任务数
        active_tasks_count = len(service.active_tasks) - queue_size

        payload = {
            "worker_id": worker_id,
            "status": status,
            "active_tasks_count": active_tasks_count,
            "queue_size": queue_size,
            "processed_tasks_count": processed_tasks_count
        }
        try:
            async with self._session.post(url, json=payload) as response:
                if response.status != 200:
                    log.warning(f"发送心跳失败。状态码: {response.status}")
        except aiohttp.ClientError as e:
            log.warning(f"发送心跳时发生连接错误: {e}")


# --- 回调函数定义 ---

async def on_screenshot_saved(client: SchedulerAPIClient, infohash: str, filepath: str):
    """当 ScreenshotService 成功保存一个截图文件时被调用的回调函数。"""
    await client.upload_screenshot(infohash, filepath)

async def on_task_finished(client: SchedulerAPIClient, status: str, infohash: str, message: str, **kwargs):
    """当 ScreenshotService 完成一个任务时被调用的回调函数。"""
    await client.update_task_status(infohash, status, message, kwargs.get("resume_data"))


# --- 主程序逻辑 ---

async def heartbeat_loop(client: SchedulerAPIClient, service: ScreenshotService, processed_tasks_counter: dict):
    """一个独立的协程，定期向调度器发送心跳。"""
    while True:
        await client.send_heartbeat(WORKER_ID, service, processed_tasks_counter['count'])
        await asyncio.sleep(HEARTBEAT_INTERVAL)

async def main(session: aiohttp.ClientSession):
    """工作节点的主函数，负责初始化和协调所有服务。"""
    settings = Settings()
    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()
    processed_tasks_counter = {'count': 0}

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop_event.set)

    client = SchedulerAPIClient(session, settings.scheduler_url)

    if not await client.register(WORKER_ID):
        log.error("无法向调度器注册，程序退出。")
        return

    async def on_task_finished_with_counter(client: SchedulerAPIClient, status: str, infohash: str, message: str, **kwargs):
        """当 ScreenshotService 完成一个任务时被调用的回调函数，并更新计数器。"""
        if status == 'success':
            processed_tasks_counter['count'] += 1
            log.info(f"任务 {infohash} 成功完成。已处理任务总数: {processed_tasks_counter['count']}")
        await on_task_finished(client, status, infohash, message, **kwargs)

    status_cb = partial(on_task_finished_with_counter, client)
    screenshot_cb = partial(on_screenshot_saved, client)

    service = ScreenshotService(settings=settings, loop=loop, status_callback=status_cb, screenshot_callback=screenshot_cb)
    await service.run()
    log.info("ScreenshotService 已在后台运行。")

    heartbeat_task = asyncio.create_task(heartbeat_loop(client, service, processed_tasks_counter))
    log.info("心跳任务已启动。")

    log.info("启动主任务轮询循环...")
    while not stop_event.is_set():
        try:
            # Concurrency logic: keep fetching tasks as long as we are below the concurrency limit
            if len(service.active_tasks) >= settings.WORKER_CONCURRENCY:
                await asyncio.sleep(POLL_INTERVAL)
                continue

            task_data = await client.get_next_task(WORKER_ID)
            if task_data:
                log.info(f"收到新任务: {task_data['infohash']}，提交到本地服务。")
                if metadata := task_data.get('metadata'):
                    task_data['metadata'] = base64.b64decode(metadata)
                await service.submit_task(**task_data)
                await asyncio.sleep(0.1) # Brief pause to prevent spamming the scheduler
            else:
                log.info("调度器中无更多可用任务，进入等待状态。")
                await asyncio.sleep(POLL_INTERVAL)

        except asyncio.CancelledError:
            break
        except Exception as e:
            log.error(f"主轮询循环发生意外错误: {e}", exc_info=True)
            await asyncio.sleep(POLL_INTERVAL)


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

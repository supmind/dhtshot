# -*- coding: utf-8 -*-
"""
本脚本用于测试从一个 'resume_data.json' 文件恢复截图任务。

工作原理:
1.  检查 `resume_data.json` 文件是否存在。
2.  读取并解析 JSON 文件，获取 `resume_data` 和 `infohash`。
3.  `resume_data` 中有一个 `extradata` 字段，它在生成时被编码成了
    Base64 字符串，在这里需要先解码回 bytes 类型。
4.  启动 ScreenshotService。
5.  提交一个新任务，但这次附带上我们加载的 `resume_data`。
6.  等待任务执行，并通过 `status_callback` 监控其最终状态。
7.  如果最终状态是 'success'，则证明恢复任务成功。
"""
import asyncio
import json
import logging
import os
import base64
import signal

from screenshot.service import ScreenshotService

# --- 配置 ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
RESUME_FILE = "resume_data.json"


async def main():
    """主程序入口"""
    # 1. 检查并读取文件
    if not os.path.exists(RESUME_FILE):
        logging.error(f"错误: 未找到 resume 数据文件 '{RESUME_FILE}'。")
        logging.error("请先运行 `generate_resume_data.py` 脚本。")
        return

    with open(RESUME_FILE, 'r', encoding='utf-8') as f:
        resume_data = json.load(f)

    infohash = resume_data.get('infohash')
    if not infohash:
        logging.error("错误: resume 数据文件中缺少 'infohash'。")
        return

    logging.info(f"从 {RESUME_FILE} 加载了 infohash: {infohash} 的 resume 数据。")

    # 2. 解码 Base64 字段
    # 'extradata' 在保存时被编码为 base64，现在需要解码回 bytes
    extractor_info = resume_data.get('extractor_info')
    if extractor_info and extractor_info.get('extradata'):
        extradata_str = extractor_info.get('extradata')
        if isinstance(extradata_str, str):
            try:
                extractor_info['extradata'] = base64.b64decode(extradata_str)
            except (base64.binascii.Error, TypeError) as e:
                logging.error(f"解码 extradata 失败: {e}")
                return

    loop = asyncio.get_running_loop()
    # 使用 Event 来等待任务完成
    completed_event = asyncio.Event()

    async def status_callback(status: str, infohash: str, **kwargs):
        """监控任务状态，并在任务终结时发出信号。"""
        message = kwargs.get('message', '')
        logging.info(f"CALLBACK: status={status.upper()}, infohash={infohash}, message='{message}'")
        if status == 'success':
            logging.info("✅ 任务成功恢复并完成！")
            completed_event.set()
        elif status in ('permanent_failure', 'recoverable_failure'):
            # 在恢复任务中，任何失败都意味着恢复失败
            logging.error(f"❌ 任务恢复失败，最终状态: {status}")
            completed_event.set()

    service = ScreenshotService(loop=loop, status_callback=status_callback)
    await service.run()

    logging.info("正在提交恢复任务...")
    await service.submit_task(infohash, resume_data=resume_data)

    logging.info("⏳ 任务已提交，等待其完成... (最多可等待5分钟)")
    try:
        await asyncio.wait_for(completed_event.wait(), timeout=300)
    except asyncio.TimeoutError:
        logging.error("❌ 任务在5分钟内未完成。")
    finally:
        logging.info("正在停止服务...")
        service.stop()
        logging.info("服务已停止。")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("\n程序被用户中断。")

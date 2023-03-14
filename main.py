import asyncio
import os
import time
import uuid
import logging
from typing import Dict
import requests
from fastapi import FastAPI, HTTPException, UploadFile, File
import uvicorn

app = FastAPI()

# 存储所有任务状态的字典
tasks: Dict[str, Dict] = {}
node_ip = {
    '0': 'http://34.135.240.45',
    '1': 'http://34.170.128.54',
    '2': 'http://34.30.67.124',
    '3': 'http://34.28.200.46',
}

# 设置日志记录器
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@app.get("/")
async def root():
    return {"message": "Hello World"}


# 创建侦查任务
@app.get("/task/init/{task_type}/{destination_type}")
async def init_task(task_type: str, destination_type: str):
    destination = ''
    task_name = ''
    if task_type == '1':
        task_name = 'reconnaissance'
    elif task_type == '2':
        task_name = 'track'
    else:
        raise HTTPException(status_code=404, detail=f"Task {task_type} not found")
    # 生成任务id
    task_id = str(uuid.uuid4())[0:8]
    # 创建任务
    if destination_type == 'master':
        for i in range(4):
            try:
                response = requests.request('GET', f"{node_ip[i]}/task/init/{task_name}/{task_id}/{str(i)}")
                logger.info(f"Created reconnaissance task {task_id} for {destination_type} node {str(i)}")
                logger.info(f'url: {node_ip[i]}/task/init/{task_name}/{task_id}/{str(i)}, response: {response.text}')
            except Exception as e:
                logger.error(f"Failed to create reconnaissance task {task_id} for {destination_type} node {str(i)}: {e}")
    elif destination_type == 'edge':
        try:
            response = requests.request('GET', f"http://34.130.234.56/task/init/{destination_type}/{task_name}/{task_id}")
            logger.info(f"Created reconnaissance task {task_id} for {destination_type}")
            logger.info(f'url: http://34.130.234.56/task/init/{task_name}/{task_id}, response: {response.text}')
        except Exception as e:
            logger.error(f"Failed to create reconnaissance task {task_id} for {destination_type}: {e}")
    # 将任务id和任务名称和状态和目的地和创建时间存入tasks字典
    tasks[task_id] = {"task_name": task_name, "status": "created", "destination_type": destination_type,
                      "creat_time": time.time()}
    # 详细记录日志
    logger.info(
        f"Created {task_name} task {task_name}:{task_id} for {destination_type}:{destination}, creat_time:{tasks[task_id]['creat_time']}")
    # 返回任务id和任务创建状态
    return {"task_id": task_id, "task_name": task_name, "destination_type": destination_type, "status": "init"}


# 接受图片并储存，并记录图片数量
@app.post("/image/{task_id}")
async def receive_image(task_id: str, image: UploadFile = File(...)):
    # 保存图片到image文件夹
    with open(f"image/{task_id}_image.jpg", "wb") as buffer:
        buffer.write(await image.read())
    # 记录任务获取图片数量
    if "image_count" not in tasks[task_id]:
        tasks[task_id]["image_count"] = 1
    else:
        tasks[task_id]["image_count"] += 1
    # 如果图片数量达到1440，则任务结束，并启用协程删除任务相关图片
    if tasks[task_id]["image_count"] == 1440:
        tasks[task_id]["status"] = "finished"
        # 记录任务结束时间
        tasks[task_id]["end_time"] = time.time()
        asyncio.create_task(delete_image(task_id))

    # 详细记录日志
    logger.info(f"Received image {task_id}_image.jpg for task {task_id}, image_count:{tasks[task_id]['image_count']}")
    # 返回成功消息
    return {"message": f"Image {task_id}_image.jpg received"}


# 查询任务状态
@app.get("/task/status/{task_id}")
async def get_task_status(task_id: str):
    # 如果任务id在tasks字典中，则返回任务状态
    if task_id in tasks:
        return {"status": tasks[task_id]}
    # 否则返回任务不存在
    else:
        raise HTTPException(status_code=404, detail=f"Task {task_id} not found")


# 停止任务
@app.get("/task/stop/{task_id}")
async def stop_task(task_id: str):
    # 如果任务id在tasks字典中，则停止任务
    if task_id not in tasks:
        raise HTTPException(status_code=404, detail=f"Task {task_id} not found")

    if tasks[task_id]["status"] == "stopped":
        raise HTTPException(status_code=400, detail=f"Task {task_id} already stopped")

    tasks[task_id]["status"] = "stopped"
    # 记录任务结束时间
    tasks[task_id]["end_time"] = time.time()

    if tasks[task_id]["destination_type"] == 'master':
        for node in node_ip:
            try:
                response = requests.request('GET', f"{node}/task/stop/{task_id}")
                logger.info(f"Stopped task {task_id} for node {node}")
                logger.info(f'url: {node}/task/stop/{task_id}, response: {response.text}')
            except requests.exceptions.RequestException as e:
                logger.error(f"Failed to stop task {task_id} for node {node}: {e}")
    elif tasks[task_id]["destination_type"] == 'edge':
        try:
            response = requests.request('GET', f"http://34.130.234.56/task/stop/{task_id}")
            logger.info(f"Stopped task {task_id} for edge")
            logger.info(f'url: http://34.130.234.56/task/stop/{task_id}, response: {response.text}')
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to stop task {task_id} for edge: {e}")

    # 启用协程删除任务相关图片
    asyncio.create_task(delete_image(task_id))

    # 详细记录日志
    logger.info(f"Stopped task {task_id}")
    # 返回任务停止状态
    return {"status": "stopped", "task_id": task_id}


async def delete_image(task_id):
    # 删除图片，如果名称包含task_id
    for file in os.listdir("image"):
        if task_id in file:
            # 删除图片
            os.remove(f"image/{file}")
            # 详细记录日志
            logger.info(f"Deleted image {file}")


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080)

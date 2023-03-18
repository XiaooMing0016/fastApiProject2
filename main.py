import json
import time
import uuid
import logging
from typing import Dict
import requests
from fastapi import FastAPI, HTTPException
import uvicorn

app = FastAPI()

# 存储所有任务状态的字典
_tasks: Dict[str, Dict] = {}

_node_ip = {
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
    try:
        # 从json文件中读取更新task
        with open('tasks.json', 'r') as f:
            file_tasks = json.load(f)
        for task_id in file_tasks:
            _tasks[task_id] = file_tasks[task_id]
    except Exception as e:
        logger.warning(f"Failed to read tasks.json: {e}")
    return {"message": "Hello World"}


# 创建任务
@app.get("/task/init/{task_type}/{destination_type}/{task_name}/{priority}")
async def init_task(task_type: str, destination_type: str, task_name: str, priority: str):
    destination = ''
    task_type_name = ''
    if task_type == '1':
        task_type_name = 'reconnaissance'
    elif task_type == '2':
        task_type_name = 'track'
    else:
        raise HTTPException(status_code=404, detail=f"Task {task_type} not found")
    # 生成任务id
    task_id = str(uuid.uuid4())[0:8]
    # 创建任务
    if destination_type == 'master':
        for i in range(4):
            try:
                response = requests.request('GET', f"{_node_ip[i]}/task/init/{task_type_name}/{task_id}/{str(i)}/"
                                                   f"{task_name}/{priority}")
                if response.status_code == 200:
                    logger.info(
                        f"Created {task_type_name} task {task_name}:{task_id} for {destination_type} node {str(i)} is "
                        f"successful, priority: {priority}")
                    _tasks[task_id] = {}
                    _tasks[task_id][str(i)] = {"task_id": task_id, "task_node": str(i),
                                               "task_name": task_name, "task_type": task_type,
                                               "task_type_name": task_type_name, "task_priority": priority,
                                               "task_destination": destination_type, "task_status": "created",
                                               "creat_time": (
                                                   time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))}

                    # 将tasks字典写入tasks.json文件
                    with open('tasks.json', 'w') as f:
                        json.dump(_tasks, f)
                else:
                    logger.warning(
                        f"Created {task_type_name} task {task_name}:{task_id} for {destination_type} node {str(i)} "
                        f"is failed, priority: {priority}, response: ")
            except Exception as e:
                logger.error(
                    f"Created {task_type_name} task {task_name}:{task_id} for {destination_type} node {str(i)} "
                    f"is error, priority: {priority}, error: {e}")
    elif destination_type == 'edge':
        try:
            logger.info(
                f"Start init task, task_id: {task_id}, task_name: {task_name}, task_type_name: {task_type_name}, "
                f"priority: {priority}")
            response = requests.request('GET', f"http://34.130.234.56/task/init/{task_type_name}/{task_id}/"
                                               f"{task_name}/{priority}")
            if response.status_code == 200:
                logger.info(
                    f"Created {task_type_name} task {task_name}:{task_id} for {destination_type} node Edge is "
                    f"successful, priority: {priority}")
                _tasks[task_id] = {}
                _tasks[task_id]['edge'] = {"task_id": task_id, "task_node": 'edge',
                                           "task_name": task_name, "task_type": task_type,
                                           "task_type_name": task_type_name, "task_priority": priority,
                                           "task_destination": destination_type, "task_status": "created",
                                           "creat_time": (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))}
                # 将tasks字典写入tasks.json文件
                with open('tasks.json', 'w') as f:
                    json.dump(_tasks, f)
            else:
                logger.warning(
                    f"Created {task_type_name} task {task_name}:{task_id} for {destination_type} node Edge"
                    f"is failed, priority: {priority}, response: {response.text}")
                return {"task_id": task_id, "task_name": task_name, "task_type_name": task_type_name,
                        "destination_type": destination_type, "status": "failed", "response": response.text}
        except Exception as e:
            logger.error(
                f"Created {task_type_name} task {task_name}:{task_id} for {destination_type} node Edge "
                f"is error, priority: {priority}, error: {e}")
    return {"task_id": task_id, "task_name": task_name, "task_type_name": task_type_name,
            "destination_type": destination_type, "status": "created"}


# 接受数据并处理
@app.get("/task/process/{task_id}/{node_id}/{image_num}/{node_num}")
async def task_process(task_id: str, node_id: str, image_num: int, node_num: int):
    # 如果任务id在tasks字典中，则更新任务状态
    if task_id in _tasks:
        _tasks[task_id][node_id]['task_status'] = 'processing'
        logger.info(f"Received {image_num} data from {node_id} node, task id: {task_id}")
        logger.info(f"Start to process data, task id: {task_id}, task_node: {node_id}")
        if node_id == 'edge':
            # 模拟处理数据,0.5秒
            time.sleep(0.5)
            _tasks[task_id][node_id]['task_progress'] = int(image_num) / node_num*125
            logger.info(f"data processing, task id: {task_id}, task_node: {node_id}, "
                        f"progress: {_tasks[task_id][node_id]['task_progress']}")
        else:
            # 模拟处理数据,1秒
            time.sleep(1)
            _tasks[task_id][node_id]['task_progress'] = int(image_num) / 125
        logger.info(f"End to process data, task id: {task_id}, task_node: {node_id}, "
                    f"progress: {_tasks[task_id][node_id]['task_progress']}")
        try:
            # 将tasks字典写入tasks.json文件
            with open('tasks.json', 'w') as f:
                json.dump(_tasks, f)
        except Exception as e:
            logger.error(f'Update tasks error, {e}')
        # 如果所有任务都完成，则更新任务状态
        if all(_tasks[task_id][node_id]['task_status'] == 'finished' for node_id in _tasks[task_id]):
            logger.info(f"All nodes have finished processing data, task id: {task_id}")
            _tasks[task_id]['task_status'] = 'finished'
    return {"task_process": "success"}


# 查询任务状态
@app.get("/task/status/{task_id}")
async def get_task_status(task_id: str):
    if task_id in _tasks:
        return {_tasks[task_id]}
    else:
        return {'message': 'task_id does not exist'}


# 停止任务
@app.get("/task/end/{task_id}")
async def stop_task(task_id: str):
    if task_id in _tasks:
        if _tasks[task_id]['edge']:
            try:
                response = requests.request('GET', f"http://34.130.234.56/task/end/{task_id}")
                if response.status_code == 200:
                    _tasks[task_id]['edge']['task_status'] = 'end'
                    _tasks[task_id]['edge']['task_end_time'] = (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
                    logger.info(f"Stop task {task_id} is successful")
                    # 将tasks字典写入tasks.json文件
                    with open('tasks.json', 'w') as f:
                        json.dump(_tasks, f)
                else:
                    logger.warning(f'Stop task {task_id} is failed')
            except Exception as e:
                logger.error(f'Stop task {task_id} is error, error: {e}')
        else:
            for i in range(4):
                try:
                    response = requests.request('GET', f"{_node_ip[i]}/task/end/{task_id}")
                    if response.status_code == 200:
                        _tasks[task_id][str(i)]['task_status'] = 'end'
                        _tasks[task_id][str(i)]['task_end_time'] = (
                            time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
                    else:
                        logger.warning(f'Stop task {task_id} is failed')
                except Exception as e:
                    logger.error(f'Stop task {task_id} is error, error: {e}')
    else:
        return {'message': 'task_id does not exist'}


# 完成任务
@app.get("/task/finish/{task_id}/{node_id}")
async def finish_task(task_id: str, node_id: str):
    logger.info(f"Received finish task {task_id} from node {node_id}")
    if task_id in _tasks:
        _tasks[task_id][node_id]['task_status'] = 'finished'
        _tasks[task_id][node_id]['task_end_time'] = (
            time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
        with open('tasks.json', 'w') as f:
            json.dump(_tasks, f)
    else:
        return {'message': 'task_id does not exist'}
    # 如果所有节点都完成任务，则完成任务
    if _tasks[task_id]['edge']['task_status'] == 'finished' and _tasks[task_id]['0']['task_status'] == 'finished' \
            and _tasks[task_id]['1']['task_status'] == 'finished' and _tasks[task_id]['2']['task_status'] == 'finished' \
            and _tasks[task_id]['3']['task_status'] == 'finished':
        _tasks[task_id]['task_status'] = 'finished'
        _tasks[task_id]['task_end_time'] = (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
        with open('tasks.json', 'w') as f:
            json.dump(_tasks, f)
        logger.info(f"Finish task {task_id} is successful")
        return {"task_finish": "success"}
    else:
        return {"message": f"Task {task_id} node {node_id} is finished, waiting for other nodes to finish"}


# 边缘节点完成任务
@app.get("/task/finish/{task_id}")
async def finish_task(task_id: str):
    logger.info(f"Received finish task {task_id} from edge node")
    if task_id in _tasks:
        _tasks[task_id]['edge']['task_status'] = 'finished'
        _tasks[task_id]['edge']['task_end_time'] = (
            time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
        with open('tasks.json', 'w') as f:
            json.dump(_tasks, f)
        logger.info(f"Finish task {task_id} is successful")
    else:
        return {'message': 'task_id does not exist'}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080)

# 基于Python 3.9镜像构建
FROM python:3.9

# 设置工作目录
WORKDIR /app

# 复制文件
COPY requirements.txt main.py ./

# 安装依赖
RUN pip install --no-cache-dir -r requirements.txt

# 暴露端口
EXPOSE 80

# 启动应用程序
CMD ["python", "main.py"]

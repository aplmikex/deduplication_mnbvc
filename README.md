# MNBVC 去重部分

### 项目描述

本项目的主要目的是：

1. 将外界输入的文件以文件md5和文件大小进行重复检测，删除不同来源的同一文件。
2. 将大量文本文件（目前仅有txt文件）转换为格式化的、易于查询的数据。
3. 在个人电脑上，实现对百万个文件的量级的快速去重操作。
4. （TODO）在集群上，对全部类型的文件进行重复检测。

### 环境安装

1. 从gtihub下载本项目
   ```shell
   git clone https://github.com/aplmikex/deduplication_mnbvc
   ```
2. 使用 `pip`命令安装所需的库
   ```shell
   # 进入这个库的目录
   cd deduplication_mnbvc
   # 安装项目所需要的依赖
   pip install -r requirements.txt
   ```

### jsonl格式说明

1. 对于每个jsonl文件，其大小略大于500MiB，这个数值定义在 `utils.py`中的 `max_size`，可根据需要更改
3. 对于每一个文件，他的json结构层次如下：

   ```python
   {
       '文件名': '文件.txt',
       '是否待查文件': False,
       '是否重复文件': False,
       '文件大小': 1024,
       'simhash': 0,
       '最长段落长度': 0,
       '段落数': 0,
       '去重段落数': 0,
       '低质量段落数': 0,
       '段落': []
   }
   ```

   将每一行为一个段落，段落的json结构层次如下：

   ```python
   {
       '行号': line_number,
       '是否重复': False,
       '是否跨文件重复': False,
       'md5': md5,
       '内容': line
   }
   ```

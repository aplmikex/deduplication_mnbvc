### 项目描述
本项目的主要目的是：

1. 将大量文本文件转换为格式化的、易于查询的数据。

2. 快速标注同一文件内是否有明显重复的情况。



### 环境安装

1. 从gtihub下载本项目

    ```bash
    git clone https://github.com/aplmikex/deduplication_mnbvc
    ```

2. 使用`pip`命令安装所需的库

    ```bash
    # 进入这个库的目录
    cd deduplication_mnbvc
    # 安装项目所需要的依赖
    pip install -r requirements.txt
    ```



### 使用说明

1. 运行`convert.py`文件并设置必要的参数。

    ```bash
    python convert.py --src_dir /path/to/source/directory
    ```

    其中`--src_dir`参数是必须的，它指定了要转换的源文件夹路径。如果未提供此参数，则会引发错误。

2. 可选参数

    - `--src`：指定源文件类型，默认为`txt`。
    - `--dst`：指定目标文件类型，默认为`jsonl`。
    - `--dst_dir`：指定转换后文件的输出目录，默认为`converted/`。
    - `--n_process`：指定要使用的进程数，默认为4。
    - `--threshold`：指定文件被认为是“待查文件”的阈值，默认为0.95。

3. 作为包供函数使用

    ```python
    from convert import convert
    import multiprocessing
    
    if __name__ == '__main__':
        convert('/path/to/source/directory', dst_dir='converted/', n_process=multiprocessing.cpu_count()-1)
    ```



### 注意

1. 本项目假设所有需要被去重的txt文件编码均为UTF-8编码，批量转换请参考[chatset-mnbvc](https://github.com/alanshi/charset_mnbvc)
2. 本项目暂时只实现了从txt到jsonl的转换，其他转换正在施工中
2. 本项目将处理极多的数据，主要集中在文件因为一些问题自我重复的情况，故只统计完全相同的情况



### 功能介绍

这个工具的主要功能是将指定目录中的文本文件(.txt)转换为json格式(.jsonl)，并进行分组处理。主要包括以下几个步骤：

1. 将每个文本文件转换为json格式。

2. 根据输入的阈值判断该段是否疑似重复，并计算文件的其他各种统计信息。

4. 根据文件的是否疑似重复，将处理后的json数据分别写入到正常文件与待查的文件中。

    

### 代码说明

这个工具包括以下几个Python文件：

- `convert.py`：主要的执行文件，用于执行转换过程。
- `utils.py`：包含一些用于辅助执行文件的工具函数。

主要的执行文件(`convert.py`)包括以下几个函数：

- `from_txt_to_json(file_path, threshold)`：将给定的文本文件转换为json格式。
- `run_process(file_path_queue, json_to_write_queue, threshold)`：多进程执行文件转换任务。
- `write_jsonl(json_to_write_queue, file_nums, dst_dir)`：将处理后的json数据写入到目标文件中。
- `convert(src_dir, src='txt', dst='jsonl', dst_dir='converted/', n_process=4, threshold=0.95)`：整个文件转换的主要函数。



### 完整使用范例

1. 快速入手示例

    ```python
    python convert.py --src_dir /home/xiang/文档/20230124 --dst_dir converted/ --n_process 4 --threshold 0.95
    ```
    
2. 测试结果

    ```bash
    python convert.py --src_dir /home/xiang/文档/20230124 --dst_dir converted/ --n_process 4 --threshold 0.95
    █████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████| 34674/34674 [06:34<00:00, 87.97it/s]
    ```

3. 测试环境结果说明（开发测试用机):
    - Lenovo 拯救者R7000 R7 4800h，内存:16 GB，硬盘:1T，系统版本:ubuntu 2004
    - 使用的[mnbvc](https://github.com/esbatmop/MNBVC)主项目的20230124.zip压缩包，约25.2Gib
    - 使用默认参数，n_process=4，threshold=095
    - 转换时间6分34秒，转换后大小39.9Gib


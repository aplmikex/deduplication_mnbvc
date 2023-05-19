# MNBVC 格式化

### 项目描述

本项目的主要目的是：

1. 将大量文本文件转换为格式化的、易于查询的jsonl数据。
2. 快速标注同一文件内是否有明显重复的情况，统一放在 `problem_i.jsonl`里面。

### convert.py 使用说明

1. 使用说明：
   * `convert.py`是快速把txt文件转化为jsonl文件，并挑出明显自我重复的txt文件留待观察。
   * 本项目假设所有需要被去重的txt文件编码均为UTF-8编码，批量转换请参考[chatset-mnbvc](https://github.com/alanshi/charset_mnbvc)。
   * 本项目暂时只实现了从txt到jsonl的转换，暂未考虑其他类型数据。
   * 本项目删去了原始txt文件中的空行，以及行首与行尾的空白符。
2. 运行 `convert.py`文件并设置必要的参数。
   ```shell
   python convert.py --src_dir /path/to/source/directory --dst_dir /path/to/destination/directory --n_process 4 --threshold 0.7
   ```

   其中 `--src_dir`参数是必须的，它指定了要转换的源文件夹路径。如果未提供此参数，则会引发错误。
3. 可选参数
   * `--src`：指定源文件类型，默认为 `txt`。
   * `--dst`：指定目标文件类型，默认为 `jsonl`。
   * `--dst_dir`：指定转换后文件的输出目录，默认为 `converted/`。
   * `--n_process`：指定要使用的进程数，默认为4。

### 输出的jsonl格式说明

1. 根据文件内段落的重复率是否高于给定的阈值，将文件分成正常文件和待查文件，其中正常文件数字加jsonl，如 `10.jsonl`，而待查文件则是problem_加数字加jsonl，如 `problem_7.jsonl`
2. 对于每个jsonl文件，其大小略大于500MiB，这个数值定义在 `utils.py`中的 `max_size`，可根据需要更改
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

### DEMO示例

```bash
python convert.py --src_dir /home/xiang/文档/mnbvcfiles --dst_dir ./mnbvcfiles --n_process 8 --threshold 0.7
```

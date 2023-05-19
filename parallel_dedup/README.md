# MNBVC 单机多进程文件去重

### 项目描述

本项目的主要目的是：

1. 在个人电脑上，实现对百万个文件的量级的快速去重操作。

### 使用说明

1. #### convert_jsonl_to_csv.py

   1. 使用说明：

      - `convert_jsonl_to_csv.py`是把jsonl文件转化为对应txt的元数据组成的csv文件，用于储存文件的md5的集合。
      - 本项目依赖的是从 `convert.py`输出的jsonl文件，请不要使用其他格式。
      - 本项目使用对jsonl文件名的MD5值以及对原始文件的MD5值连接起来作为输出csv的文件名，会有极小概率丢失文件元数据，导致去重过程不包含该文件，如对全部文件的完整度非常在意，请检查生成的csv文件数与原始txt文件数量。
   2. 运行 `convert_jsonl_to_csv.py`文件并设置必要的参数。

      ```bash
      python convert_jsonl_to_csv.py --src_dir /path/to/source/directory --dst_dir /path/to/destination/directory 
      ```

      其中 `--src_dir`参数是必须的，它指定了要转换的jsonl源文件夹路径。如果未提供此参数，则会引发错误。
   3. 可选参数

      - `--dst_dir`：指定转换后文件的输出目录，默认为 `output_csv/`。
2. #### multiprocess_deduplication.py

   1. 使用说明：

      - `multiprocess_deduplication.py`是MNBVC单机多进程文件去重部分的主要代码。
      - 本项目依赖的是从 `convert_jsonl_to_csv.py`输出的csv文件，请不要使用其他格式。
      - 本项目最少使用2个进程，最多无上限，建议使用电脑cpu核心数的进程数量。其中一个进程进行去重的比较工作，其他所有进程用于读取重复的文件的csv列表，用于二次验证是否重复。
   2. 运行 `multiprocess_deduplication.py`文件并设置必要的参数。

      ```bash
      python multiprocess_deduplication.py --src_dir /path/to/source/directory --n_process 10 --simhash_threshold 3 --jaccard_thresold 0.8
      ```

      其中 `--src_dir`参数是必须的，它指定了要转换的csv源文件夹路径。如果未提供此参数，则会引发错误。
   3. 可选参数

      - `--simhash_threshold`：指定simhash阈值，默认设置为3，效果较好。这个值如果大于5，则会极慢无比。
      - `--jaccard_thresold`：指定jaccard阈值，默认为0.8。低于这个数的可以手动看，进行决策是否重复。一般来说simhash阈值在3的时候，极少有真正重复的。
      - `--n_process`：指定要使用的进程数，默认为13。
3. #### reset_csv.py

   1. 使用说明：

      - `reset_csv.py`是清除csv文件去重结果代码。我们会用 `multiprocess_deduplication.py`把去重结果写到csv文件中，若某次 `multiprocess_deduplication.py`参数选择出错，可以用本代码清除csv状态，重新进行去重。
   2. 运行 `reset_csv.py`文件并设置必要的参数。

      ```bash
      python reset_csv.py --src_dir /path/to/source/directory 
      ```

      其中 `--src_dir`参数是必须的，它指定了要转换的csv源文件夹路径。如果未提供此参数，则会引发错误。
4. #### write_output_to_jsonl.py

   1. 使用说明：

      - `write_output_to_jsonl.py`是将csv去重的结果保存到原始的jsonl文件中去，属于去重最后一步。
   2. 运行 `write_output_to_jsonl.py`文件并设置必要的参数。

      ```bash
      python write_output_to_jsonl.py --csv_dir /path/to/source/csvdirectory --jsonl_dir /path/to/source/jsonldirectory 
      ```

      其中 `--csv_dir`参数是必须的，它指定了csv源文件夹路径。如果未提供此参数，则会引发错误。

      其中 `--jsonl_dir`参数是必须的，它指定了jsonl文件夹路径。如果未提供此参数，则会引发错误。

### 输出的csv格式说明

1. 对于每个jsonl文件，输出他jsonl路径名的MD5哈希以及对应每个txt文件MD5哈希的csv名，放入指定文件夹中
2. 对于每一个文件，他的csv文件结构层次如下：

   第一行：

   | 是否重复（0代表不重复，1代表重复） | jsonl文件名 | txt文件名 | simhash值 |
   | :--------------------------------: | :---------: | :-------: | :-------: |
   |                 0                 | jsonl文件名 | txt文件名 | simhash值 |

   第二行：

   MD5列表，每一列对应一个MD5值，为节省空间算力，只截取第8位到24位，既中间16位的MD5值。

### Demo示例

```bash
python convert_jsonl_to_csv.py --src_dir ./mnbvcfiles --dst_dir ./output_csv
# 假如simhash参数设置错误，simhash_threshold设置成12，导致速度极慢
python multiprocess_deduplication.py --src_dir ./output_csv --n_process 15 --simhash_threshold 12 --jaccard_thresold 0.8
# 先强行结束，在运行reset
python reset_csv.py --src_dir ./output_csv
# 正常跑一遍
python multiprocess_deduplication.py --src_dir ./output_csv --n_process 15 --simhash_threshold 3 --jaccard_thresold 0.8
python write_output_to_jsonl.py --csv_dir ./output_csv --jsonl_dir ./mnbvcfiles
```

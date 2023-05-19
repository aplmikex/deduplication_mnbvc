# MNBVC 基本去重

### 项目描述

本项目的主要目的是：

1. 指定一个文件夹，设定定时任务，定时新增从外界获取的文件的信息追加至pkl二进制文件中。
2. 按照文件大小与文件md5值，输出完全相同的文件至一个txt中。
3. 根据用户需要，用户手动写脚本删除txt中的完全相同的文件名。

### 使用说明

1. #### write_meta_data_pkl.py

   1. 使用说明：

      - `write_meta_data_pkl.py`是把文件夹内不同格式的文件追加的写入到pkl文件中。
      - 可以对一个文件夹反复运行此代码，只要路径不改变，pkl文件不会重复添加。
      - 如果改变原始文件路径，请删除pkl重新生成，不然会多次删除同一个文件。
      - 在运行中若增加或删减文件，可能导致文件出错或者pkl文件较大，建议运行一段时间后删除pkl，重新生成。
   2. 运行 `write_meta_data_pkl.py`文件并设置必要的参数。

      ```bash
      python write_meta_data_pkl.py --dir_path /path/to/directory --pkl_file file.pkl
      ```
2. #### find_duplicates.py

   1. 使用说明：

      - `find_duplicates.py`是输入pkl文件，输出除了第一次出现以外其他完全重复的文件。
      - 他的结果默认会输出到duplicates.txt文件中，是覆盖写，所以建议每次去重完直接删除duplicates.txt中的完全相同的文件名。
      - 去重后的pkl会覆盖到原来pkl文件。
   2. 运行 `multiprocess_deduplication.py`文件并设置必要的参数。

      ```bash
      python find_duplicates.py --pkl_file file.pkl
      ```

### DEMO示例

按照上面示例的使用说明执行就行了。

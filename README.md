### 项目描述
本项目旨在对大量txt文本文件进行读取，转换成jsonl格式，并快速标注是否重复。



#### 实现机制

1.  **convert.py**
    1. 递归读取文件夹下所有txt文件路径列表，存入queue中。
    2. 开启自定义数量进程，将txt文件分段，储存hash值，minhash值，hash值数量，及段落数至jsonl文件中。

2.  **de_duplication.py**
    1. 递归读取文件夹下所有jsonl文件路径列表，存入queue中。
    2. 开启自定义数量进程，按行读取jsonl文件，检查重复度，写入临时文件中，并覆盖源文件。
    3. 按照hash值的集合的数量与文章段落数的比，认为有10%以上重复的文件值得进一步过滤。
    4. 按照windows_size，既滑窗大小，对每一个滑窗hash求和，认为有10%以上连续段落重复的文件属于重复bug。
    5. 对重复段落的句子标注软删除。



#### 完整使用范例

```python
from de_duplication import remove_duplicates
from convert import convert
import multiprocessing

if __name__ == '__main__':
    convert('/home/xiang/文档/20230124', dst_dir='converted/', n_process=multiprocessing.cpu_count()-1)
    remove_duplicates('converted/', n_process=multiprocessing.cpu_count()-1, windows_size=5)
```



#### 测试环境说明（开发测试用机):

* Lenovo 拯救者R7000 R7 4800h，内存:16 GB，硬盘:1T，系统版本:ubuntu 2004
* 使用默认参数，windows_size=5，n_process=15


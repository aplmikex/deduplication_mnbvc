import os, sys
current_path = os.path.abspath(__file__)
sys.path.append(os.path.dirname(os.path.dirname(current_path)))
from multiprocessing import Process
import multiprocessing
import argparse
import tqdm
from utils.utils import max_size, get_all_files
import jsonlines, json
import hashlib
import utils.customSimhash as customSimhash
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def from_wudaojson_to_json(file_path, one_json):

    # 定义json结构
    file_json = {'文件名': os.path.abspath(file_path)+':'+one_json['title'],
                 '是否待查文件': False,
                 '是否重复文件': False,
                 '文件大小': len(json.dumps(one_json)),
                 'simhash': 0,
                 '最长段落长度': len(one_json['content']),
                 '数据类型': one_json['dataType'],
                 '段落数': 1,
                 '去重段落数': 1,
                 '低质量段落数': 0,
                 '段落': []}
    
    lines = [one_json['content']]

    # 定义用于去重的set
    hashs = set()


    texts = []
    for line in lines:
        # 去除行首尾空格
        line = line.strip()
        # 计算最长段落长度
        file_json['最长段落长度'] = max(file_json['最长段落长度'], len(line))
        # 删除空行
        if len(line) == 0:
            continue
        # 计算每一行的md5值
        md5 = hashlib.md5(line.encode()).hexdigest()
        # 将每一行内容添加到json中
        file_json['段落'].append({'行号': 1,
                                '是否重复': md5 in hashs,
                                '是否跨文件重复': False,
                                'md5': md5,
                                '内容': line
                                })
        if md5 not in hashs:
            texts.append(line)

        # 将md5值添加到set中，用于去重
        hashs.add(md5)

    if len(hashs) == 0:
        return None

    # 计算simhash
    file_json['simhash'] = customSimhash.Simhash(texts).value

    return file_json




def convert(src_dir, dst_dir='converted/'):
    # 检查输入参数是否合理
    assert os.path.exists(src_dir)

    file_name = 0
    if os.path.exists(os.path.join(dst_dir, str(file_name) + '.jsonl')):
        logging.warning('Warning: ' + str(file_name) + '.jsonl' + ' already exists.')

    # 如果输出目录不存在，则创建
    os.makedirs(dst_dir, exist_ok=True)

    # 获取源文件列表
    file_path_list, file_nums = get_all_files(src_dir,legal_file_type=('.json', ),  return_file_type='list')
    for _ in tqdm.tqdm(range(file_nums)):
        file = file_path_list.pop()
        with open(file, 'r', encoding='utf-8') as f:
            file_json = json.load(f)
            for one_json in file_json:
                one_json = from_wudaojson_to_json(file, one_json)
            
                with jsonlines.open(os.path.join(dst_dir, str(file_name) + '.jsonl'), mode='a') as last_file:
                    last_file.write(one_json)
                    # 如果当前文件大小超过限制，则更换文件名
                    if last_file._fp.tell() > max_size:
                        file_name += 1
                        if os.path.exists(os.path.join(dst_dir, str(file_name) + '.jsonl')):
                            logging.warning('Warning: ' + str(file_name) + '.jsonl' + ' already exists.')


if __name__ == '__main__':
    # 设置参数解析器
    parser = argparse.ArgumentParser()
    # 添加必须指定的参数
    parser.add_argument('--src_dir', type=str, required=True, help="源文件夹路径")
    # 添加可选参数，指定转换后文件存放路径，默认为converted/
    parser.add_argument('--dst_dir', type=str, default='converted/', help="指定转换后文件存放路径，默认为converted/")
    # 解析参数
    args = parser.parse_args()
    # 调用convert函数
    convert(args.src_dir, args.dst_dir)

import os, sys
current_path = os.path.abspath(__file__)
sys.path.append(os.path.dirname(os.path.dirname(current_path)))
from multiprocessing import Process
import multiprocessing
import argparse
import tqdm
from utils.utils import max_size, get_all_files
import jsonlines
import hashlib
import utils.customSimhash as customSimhash
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def from_txt_to_json(file_path, threshold):

    # 定义json结构
    file_json = {'文件名': os.path.abspath(file_path),
                 '是否待查文件': False,
                 '是否重复文件': False,
                 '文件大小': os.path.getsize(file_path),
                 'simhash': 0,
                 '最长段落长度': 0,
                 '段落数': 0,
                 '去重段落数': 0,
                 '低质量段落数': 0,
                 '段落': []}
    # 定义用于去重的set
    hashs = set()

    # 读取每一行
    with open(file_path, 'r', encoding='utf-8', errors='strict') as f:
        texts = []
        for line_number, line in enumerate(f):
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
            file_json['段落'].append({'行号': line_number,
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
    # 计算段落数和去重段落数
    file_json['段落数'] = len(file_json['段落'])
    file_json['去重段落数'] = len(hashs)
    # 计算simhash
    file_json['simhash'] = customSimhash.Simhash(texts).value
    # 判断是否是待查文件
    if (file_json['去重段落数'] / file_json['段落数']) < threshold:
        file_json['是否待查文件'] = True
    return file_json


def run_process(file_path_queue, json_to_write_queue, threshold):
    # 不断从队列中获取文件路径
    while not file_path_queue.empty():
        # 获取文件路径
        try:
            file_path = file_path_queue.get(timeout=0.2)
        except:
            break
        # 将文件转换为json
        try:
            one_json = from_txt_to_json(file_path, threshold)
        except UnicodeDecodeError:
            logging.error(f"Error: {file_path} is not encoded in utf-8.")
            json_to_write_queue.put(UnicodeDecodeError)
            exit(-1)
        # 把json写入到队列中
        json_to_write_queue.put(one_json)


def write_jsonl(json_to_write_queue, file_nums, dst_dir):

    # 定义文件名
    file_name = 0
    problem_file_name = 0
    if os.path.exists(os.path.join(dst_dir, str(file_name) + '.jsonl')):
        logging.warning('Warning: ' + str(file_name) + '.jsonl' + ' already exists.')
    if os.path.exists(os.path.join(dst_dir, 'problem_' + str(problem_file_name) + '.jsonl')):
        logging.warning('Warning: problem_' + str(problem_file_name) + '.jsonl' + ' already exists.')
    # 遍历文件数量
    for _ in tqdm.tqdm(range(file_nums)):
        # 从队列中获取一个json
        one_json = json_to_write_queue.get()
        if one_json is None:
            continue
        if one_json == UnicodeDecodeError:
            return -1
        # 根据是否待查文件，写入不同的文件
        if one_json['是否待查文件']:
            with jsonlines.open(os.path.join(dst_dir, 'problem_' + str(problem_file_name) + '.jsonl'),
                                mode='a') as last_problem_file:
                last_problem_file.write(one_json)
                # 如果当前文件大小超过限制，则更换文件名
                if last_problem_file._fp.tell() > max_size:
                    problem_file_name += 1
                    if os.path.exists(os.path.join(dst_dir, 'problem_' + str(problem_file_name) + '.jsonl')):
                        logging.warning('Warning: problem_' + str(problem_file_name) + '.jsonl' + ' already exists.')
        else:
            with jsonlines.open(os.path.join(dst_dir, str(file_name) + '.jsonl'), mode='a') as last_file:
                last_file.write(one_json)
                # 如果当前文件大小超过限制，则更换文件名
                if last_file._fp.tell() > max_size:
                    file_name += 1
                    if os.path.exists(os.path.join(dst_dir, str(file_name) + '.jsonl')):
                        logging.warning('Warning: ' + str(file_name) + '.jsonl' + ' already exists.')
    return 0

def convert(src_dir, src='txt', dst='jsonl', dst_dir='converted/', n_process=4, threshold=0.95):
    # 检查输入参数是否合理
    assert os.path.exists(src_dir)
    assert src in ['txt', 'jsonl']
    assert dst in ['txt', 'jsonl']

    # 如果输出目录不存在，则创建
    if not os.path.exists(dst_dir):
        os.mkdir(dst_dir)

    # 如果源文件和目标文件类型不匹配，则抛出异常
    if src != 'txt' or dst != 'jsonl':
        raise NotImplementedError('Only support converting from txt to jsonl now.')

    # 获取源文件列表
    file_path_queue, file_nums = get_all_files(src_dir)
    json_to_write_queue = multiprocessing.Queue(200)

    # 启动多进程，将源文件转换为json
    processes = []
    for _ in range(n_process):
        p = Process(target=run_process, args=(file_path_queue, json_to_write_queue, threshold))
        p.start()
        processes.append(p)

    # 将json写入文件
    exit_code = write_jsonl(json_to_write_queue, file_nums, dst_dir)

    if exit_code == -1:
        for p in processes:
            p.terminate()
    else:
        for p in processes:
            p.join()


if __name__ == '__main__':
    # 设置参数解析器
    parser = argparse.ArgumentParser()
    # 添加必须指定的参数
    parser.add_argument('--src_dir', type=str, required=True, help="源文件夹路径")
    # 添加可选参数，指定源文件类型，默认为txt
    parser.add_argument('--src', type=str, default='txt', help="指定源文件类型，默认为txt")
    # 添加可选参数，指定目标文件类型，默认为jsonl
    parser.add_argument('--dst', type=str, default='jsonl', help="指定目标文件类型，默认为jsonl")
    # 添加可选参数，指定转换后文件存放路径，默认为converted/
    parser.add_argument('--dst_dir', type=str, default='converted/', help="指定转换后文件存放路径，默认为converted/")
    # 添加可选参数，指定进程数，默认为1
    parser.add_argument('--n_process', type=int, default=1, help="指定进程数，默认为1")
    # 添加可选参数，指定去重阈值，默认为0.5
    parser.add_argument('--threshold', type=float, default=0.5, help="指定去重阈值，默认为0.5")
    # 解析参数
    args = parser.parse_args()
    # 调用convert函数
    convert(args.src_dir, args.src, args.dst, args.dst_dir, args.n_process, args.threshold)

from utils import str_encode, max_size, get_all_files, string_to_number
import os
import jsonlines
from multiprocessing import Process, Queue
import tempfile
import shutil
import argparse
import tqdm
import simhash

def run_process(file_path_queue, file_process_done_queue, dst_dir):
    while file_path_queue.qsize() > 0:
        file_path = file_path_queue.get()

        with jsonlines.open(file_path, 'r') as reader:
            with tempfile.NamedTemporaryFile(mode='w', delete=False) as temp:
                for file_json in reader:
                    spilted_text = []
                    for i in range(len(file_json['段落'])):
                        if file_json['段落'][i]['是否重复'] == True:
                            continue
                        spilted_text.extend([file_json['段落'][i]['内容']])
                    
                    file_json['simhash'] = simhash.Simhash('\n'.join(spilted_text)).value

                    jsonlines.Writer(temp).write(file_json)


        # 将临时文件覆盖原始文件
        shutil.move(temp.name, os.path.join(dst_dir, os.path.basename(file_path)))
        file_process_done_queue.put(True)


def calcu_simhash(src_dir, dst_dir, n_process=4):
    assert os.path.exists(src_dir)

    if not os.path.exists(dst_dir):
        # 如果不存在，创建文件夹
        os.makedirs(dst_dir)
        print(f"文件夹 '{dst_dir}' 已创建。")
    else:
        print(f"文件夹 '{dst_dir}' 已存在。")

    file_path_queue, file_path_nums = get_all_files(src_dir, legal_file_type=['.jsonl'])

    file_process_done_queue = Queue()

    processes = []
    for _ in range(n_process):
        p = Process(target=run_process, args=(file_path_queue, file_process_done_queue, dst_dir))
        p.start()
        processes.append(p)

    for _ in tqdm.tqdm(range(file_path_nums), desc='计算simhash'):
        file_process_done_queue.get()

    for p in processes:
        p.join()


if __name__ == '__main__':
    # 设置参数解析器
    parser = argparse.ArgumentParser()
    # 添加必须指定的参数
    parser.add_argument('--src_dir', type=str, required=True, help="源文件夹路径")
    # 添加可选参数，指定转换后文件存放路径，默认为converted/
    parser.add_argument('--dst_dir', type=str, required=True, help="指定转换后文件存放路径，默认为converted/")
    # 添加可选参数，指定进程数，默认为4
    parser.add_argument('--n_process', type=int, default=4, help="指定进程数，默认为4")
    # 解析参数
    args = parser.parse_args()
    # 调用convert函数
    calcu_simhash(args.src_dir, args.dst_dir, args.n_process)

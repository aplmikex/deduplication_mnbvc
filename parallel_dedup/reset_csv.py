import argparse, tqdm
import os, sys
current_path = os.path.abspath(__file__)
sys.path.append(os.path.dirname(os.path.dirname(current_path)))
from utils.utils import get_all_files

if __name__ == '__main__':
    # 设置参数解析器
    parser = argparse.ArgumentParser()
    # 添加必须指定的参数
    parser.add_argument('--src_dir', type=str, required=True, help="csv源文件夹路径")

    # 解析参数
    args = parser.parse_args()

    # 获取所有jsonl文件
    file_path_list, file_nums = get_all_files(args.src_dir, ['.csv'], 'list')

    for i in tqdm.tqdm(range(file_nums)):
        with open(file_path_list[i], 'r+', encoding='utf-8') as f:
            f.write('0')

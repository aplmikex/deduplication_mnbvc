import os
import shutil
import argparse

def move_files(input_dir, output_dir, suffix, keywords:str):
    if os.path.exists(input_dir) == False:
        raise ValueError('输入目录不存在')
    if os.path.abspath(input_dir) == os.path.abspath(output_dir):
        raise ValueError('输入目录和输出目录不能相同')

    keywords = [keyword for keyword in keywords.split(',') if keyword != '']

    os.makedirs(output_dir, exist_ok=True)
    for root, _, files in os.walk(input_dir):
        # 获取相对于输入目录的路径
        relative_path = os.path.relpath(root, input_dir)
        
        # 创建目标目录
        target_dir = os.path.join(output_dir, relative_path)
        first_create = True
        # 移动符合条件的文件
        for file in files:
            file_path = os.path.join(root, file)
            # 检查file_path是否包含全部关键词
            in_keywords = True
            for keyword in keywords:
                if keyword not in file_path:
                    in_keywords = False
                    break
            if file.lower().endswith(suffix) and in_keywords:
                if first_create:
                    os.makedirs(target_dir, exist_ok=True)
                    first_create = False
                source_file = os.path.join(root, file)
                target_file = os.path.join(target_dir, file)
                shutil.move(source_file, target_file)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_dir', type=str, required=True, help="输入目录")
    parser.add_argument('--output_dir', type=str, required=True, help="输出目录")
    parser.add_argument('--suffix', type=str, required=True, help="后缀名")
    parser.add_argument('--keywords', type=str, default='', help="关键词")

    args = parser.parse_args()
    move_files(args.input_dir, args.output_dir, args.suffix, args.keywords)
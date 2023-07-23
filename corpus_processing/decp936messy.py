import os, sys
from charset_mnbvc import api
import argparse, shutil

def get_all_files_list(dir_path):
    file_path_list = []
    for root, _, files in os.walk(dir_path):
        for file in files:
            file_path = os.path.join(root, file)
            file_path_list.append(file_path)
    return file_path_list, len(file_path_list)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--folder_path', type=str, required=True, help="乱码文件的路径，请输入最后一个正常非乱码的文件夹，会在同文件夹生成新的文件夹，但不会删除原来的")
    args = parser.parse_args()

    file_path_list, file_nums = get_all_files_list(args.folder_path)
    for file in file_path_list:
        relative_path = os.path.relpath(file, args.folder_path)
        try:
            coding_name = api.from_data(data=relative_path.encode('cp437'), mode=2)
        
            ret = api.convert_encoding(
                source_data=relative_path.encode('cp437'),
                source_encoding=coding_name,
                target_encoding="utf-8",
            )
            os.makedirs(os.path.dirname(os.path.join(args.folder_path, ret)), exist_ok=True)
            shutil.move(file, os.path.join(args.folder_path, ret))
        except UnicodeEncodeError:
            print(f"{file} :为非cp437的路径，不改变")
        except Exception as e:
            print(f"Move {file} failed: {e}")

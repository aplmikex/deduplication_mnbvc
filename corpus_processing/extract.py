import os
import argparse
import shutil
import tarfile
import zipfile
import bz2
import gzip
import rarfile
import py7zr

def get_extension(file_path):
    filename, extension = os.path.splitext(file_path)

    extensions = []
    if extension:
        extensions.insert(0, extension)
        filename_1, extension = os.path.splitext(filename)
        if extension == '.tar':
            extensions.insert(0, extension)
            filename = filename_1
    return filename, ''.join(extensions)

def extract_archive(file_path, extract_path, file):
    filename, extension = get_extension(file)
    extract_succcessful = True
    try:
        if extension == '.tar':
            with tarfile.open(file_path, 'r') as tar:
                tar.extractall(extract_path)
        elif extension == '.tbz2' or extension == '.tar.bz2':
            with tarfile.open(file_path, 'r:bz2') as tar:
                tar.extractall(extract_path)
        elif extension == '.tgz' or extension == '.tar.gz' or extension == '.tar.Z':
            with tarfile.open(file_path, 'r:gz') as tar:
                tar.extractall(extract_path)
        elif extension == '.tar.xz':
            with tarfile.open(file_path, 'r:xz') as tar:  
                tar.extractall(extract_path)
        elif extension == '.bz2':
            if not os.path.exists(extract_path):
                os.mkdir(extract_path)
            
            with bz2.open(file_path, 'rb') as f_in:
                with open(os.path.join(extract_path, filename), 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
        elif extension == '.rar':
            with rarfile.RarFile(file_path, 'r') as rar:
                rar.extractall(extract_path)
        elif extension == '.gz':
            if not os.path.exists(extract_path):
                os.mkdir(extract_path)

            with gzip.open(file_path, 'rb') as f_in:
                with open(os.path.join(extract_path, filename), 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
        elif extension == '.zip':
            with zipfile.ZipFile(file_path, 'r') as zip:
                zip.extractall(extract_path)
        elif extension == '.7z':
            with py7zr.SevenZipFile(file_path, mode='r') as seven_zip:
                seven_zip.extractall(extract_path)
        else:
            print(f"Unsupported file format: {extension}")
            extract_succcessful = False
    except Exception as e:
        print(f"Extracting {file_path} failed: {e}")
        extract_succcessful = False
    
    if extract_succcessful:
        os.remove(file_path)
    
    return extract_succcessful

def traverse_directory(folder_path):

    for root, dirs, files in os.walk(folder_path):
        extract_path_set = set(dirs)

        for file in files:
            # 判断文件是否为压缩包类型
            if file.endswith(('.tar', '.tbz2', '.tgz', '.tar.bz2', '.tar.gz', '.tar.xz', '.tar.Z', '.bz2', '.rar', '.gz', '.zip', '.xz', '.7z')):

                file_path = os.path.join(root, file)
                # 把压缩包解压到的文件夹名
                extract_path = file.split('.')[0]

                if extract_path in extract_path_set:
                    for i in range(1, 10000):
                        if f"{extract_path}_{i}" not in extract_path_set:
                            extract_path = f"{extract_path}_{i}"
                            break

                extract_path = os.path.join(root, extract_path)
                extract_succcessful = extract_archive(file_path, extract_path, file)
                if extract_succcessful:
                    traverse_directory(extract_path)
                    extract_path_set.add(file.split('.')[0])

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--folder_path', type=str, required=True, help="压缩包路径")
    args = parser.parse_args()

    traverse_directory(args.folder_path)
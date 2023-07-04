import os, hashlib
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

def extract_archive(file_path, extract_full_path, file, password=None):

    filename, extension = get_extension(file)
    extract_succcessful = True
    try:
        if extension == '.tar':
            with tarfile.open(file_path, 'r') as tar:
                tar.extractall(extract_full_path)
        elif extension == '.tbz2' or extension == '.tar.bz2':
            with tarfile.open(file_path, 'r:bz2') as tar:
                tar.extractall(extract_full_path)
        elif extension == '.tgz' or extension == '.tar.gz' or extension == '.tar.Z':
            with tarfile.open(file_path, 'r:gz') as tar:
                tar.extractall(extract_full_path)
        elif extension == '.tar.xz':
            with tarfile.open(file_path, 'r:xz') as tar:  
                tar.extractall(extract_full_path)
        elif extension == '.bz2':
            if not os.path.exists(extract_full_path):
                os.mkdir(extract_full_path)
            with bz2.open(file_path, 'rb') as f_in:
                with open(os.path.join(extract_full_path, filename), 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
        elif extension == '.rar':
            with rarfile.RarFile(file_path, 'r') as rar:
                rar.setpassword(password)
                for file in rar.namelist():
                    paths = file.split('/')
                    file_name = paths[-1]
                    if len(file_name.encode()) > 255 and len(os.path.join(extract_full_path, file).encode()) < 4095:
                        print(f"File name too long: {os.path.join(extract_full_path, file)}")
                        basename, extensions =  get_extension(file_name)
                        length = (255-len(extensions.encode())-8)//2
                        basename = basename.encode()[:length].decode('utf-8', errors='ignore')+hashlib.md5(file_name.encode()).hexdigest()[:8]+basename.encode()[-length:].decode('utf-8', errors='ignore')
                        new_name = basename + extensions
                        os.makedirs(file[:-len(file_name)], exist_ok=True)
                        with rar.open(file, 'r') as f_in:
                            data = f_in.read()
                            with open(os.path.join(extract_full_path, file[:-len(file_name)], new_name), 'wb') as f_out:
                                f_out.write(data)
                        print(f"File extract to: {os.path.join(extract_full_path, file[:-len(file_name)], new_name)}")
                    
                    elif any(len(path.encode()) > 255 for path in paths) or len(os.path.join(extract_full_path, file).encode()) > 4095:
                        print(f"File name too long: {os.path.join(extract_full_path, file)}")
                        os.makedirs(os.path.join(extract_full_path, 'long_name'), exist_ok=True)
                        length = min(255, 4096-len(os.path.join(extract_full_path, 'long_name').encode()))
                        new_name = file.encode()[:length//2-1].decode('utf-8', errors='ignore') +'_'+ file.encode()[1-length//2:].decode('utf-8', errors='ignore')
                        new_name = '_'.join(new_name.split('/'))
                        with rar.open(file, 'r') as f_in:
                            data = f_in.read()
                            with open(os.path.join(extract_full_path, 'long_name', new_name), 'wb') as f_out:
                                f_out.write(data)
                        print(f"File extract to: {os.path.join(extract_full_path, 'long_name', new_name)}")
                    else:
                        rar.extract(file, extract_full_path)
        elif extension == '.gz':
            if not os.path.exists(extract_full_path):
                os.mkdir(extract_full_path)

            with gzip.open(file_path, 'rb') as f_in:
                with open(os.path.join(extract_full_path, filename), 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
        elif extension in ('.zip', '.exe'):
            with zipfile.ZipFile(file_path, 'r') as zip:
                zip.setpassword(password)
                for file in zip.namelist():
                    paths = file.split('/')
                    file_name = paths[-1]
                    if len(file_name.encode()) > 255 and len(os.path.join(extract_full_path, file).encode()) < 4095:
                        print(f"File name too long: {os.path.join(extract_full_path, file)}")
                        basename, extensions =  get_extension(file_name)
                        length = (255-len(extensions.encode())-8)//2
                        basename = basename.encode()[:length].decode('utf-8', errors='ignore')+hashlib.md5(file_name.encode()).hexdigest()[:8]+basename.encode()[-length:].decode('utf-8', errors='ignore')
                        new_name = basename + extensions
                        os.makedirs(file[:-len(file_name)], exist_ok=True)
                        with zip.open(file, 'r') as f_in:
                            data = f_in.read()
                            with open(os.path.join(extract_full_path, file[:-len(file_name)], new_name), 'wb') as f_out:
                                f_out.write(data)
                        print(f"File extract to: {os.path.join(extract_full_path, file[:-len(file_name)], new_name)}")

                    elif any(len(path.encode()) > 255 for path in paths) or len(os.path.join(extract_full_path, file).encode()) > 4095:
                        print(f"File name too long: {os.path.join(extract_full_path, file)}")
                        os.makedirs(os.path.join(extract_full_path, 'long_name'), exist_ok=True)
                        length = min(255, 4096-len(os.path.join(extract_full_path, 'long_name').encode()))
                        new_name = file.encode()[:length//2-1].decode('utf-8', errors='ignore') +'_'+ file.encode()[1-length//2:].decode('utf-8', errors='ignore')
                        new_name = '_'.join(new_name.split('/'))
                        with zip.open(file, 'r') as f_in:
                            data = f_in.read()
                            with open(os.path.join(extract_full_path, 'long_name', new_name), 'wb') as f_out:
                                f_out.write(data)
                        print(f"File extract to: {os.path.join(extract_full_path, 'long_name', new_name)}")
                    else:
                        zip.extract(file, extract_full_path)
        elif extension == '.7z':
            with py7zr.SevenZipFile(file_path, mode='r', password=password) as seven_zip:
                seven_zip.extractall(extract_full_path)
        else:
            print(f"Unsupported file format: {extension}")
            extract_succcessful = False
    
    except Exception as e:
        print(f"Extracting {file_path} failed: {e}")
        extract_succcessful = False
    
    if extract_succcessful:
        os.remove(file_path)
    
    return extract_succcessful


def traverse_directory(folder_path, passwords=None):
    if not os.path.exists(folder_path):
        print(f"{folder_path} does not exist!")
        return
    if not passwords is None:
        with open(passwords, 'r') as f:
            balsklist = f.readlines()
        passwords = [x.strip() for x in balsklist]
    else :
        passwords = []


    for root, dirs, files in os.walk(folder_path):
        extract_path_set = set(dirs)

        for file in files:
            # 判断文件是否为压缩包类型
            if file.endswith(('.tar', '.tbz2', '.tgz', '.tar.bz2', '.tar.gz', '.tar.xz', '.tar.Z', '.bz2', '.rar', '.gz', '.zip', '.xz', '.7z', '.exe')):

                file_path = os.path.join(root, file)
                # 把压缩包解压到的文件夹名
                extract_path = file.split('.')[0]

                if extract_path in extract_path_set:
                    for i in range(1, 10000):
                        if f"{extract_path}_{i}" not in extract_path_set:
                            extract_path = f"{extract_path}_{i}"
                            break

                extract_full_path = os.path.join(root, extract_path)
                
                extract_succcessful = extract_archive(file_path, extract_full_path, file)
                
                if not extract_succcessful:
                    for password in passwords:
                        print(f"Try password: {password}")
                        extract_succcessful = extract_archive(file_path, extract_full_path, file, password=password.encode())
                        if extract_succcessful:
                            break
                
                if extract_succcessful:
                    traverse_directory(extract_full_path)
                    extract_path_set.add(file.split('.')[0])

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--folder_path', type=str, required=True, help="压缩包路径")
    parser.add_argument('--passwords_files', type=str, default=None, help="压缩包密码文件路径")
    args = parser.parse_args()

    traverse_directory(args.folder_path, args.passwords_files)
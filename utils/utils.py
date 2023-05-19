import os
import multiprocessing

str_encode = '0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ'
max_size = 500 * 1024 * 1024
max_memory = 1024 * 1024 * 1024


# 递归读取文件夹下所有文件
def get_all_files(dir_path, legal_file_type=('.txt',), return_file_type='queue'):
    if return_file_type == 'queue':
        return get_all_files_queue(dir_path, legal_file_type)
    elif return_file_type == 'list':
        return get_all_files_list(dir_path, legal_file_type)


def get_all_files_queue(dir_path, legal_file_type=('.txt',)):
    file_nums = 0
    file_path_queue = multiprocessing.Manager().Queue()

    for root, _, files in os.walk(dir_path):
        for file in files:
            if os.path.splitext(file)[-1] not in legal_file_type:
                continue
            file_path = os.path.join(root, file)
            file_path_queue.put(file_path)
            file_nums += 1
    return file_path_queue, file_nums


def get_all_files_list(dir_path, legal_file_type=('.txt',)):
    file_path_list = []
    for root, _, files in os.walk(dir_path):
        for file in files:
            if os.path.splitext(file)[-1] not in legal_file_type:
                continue
            file_path = os.path.join(root, file)
            file_path_list.append(file_path)
    file_path_list = sorted(file_path_list)
    return file_path_list, len(file_path_list)


def get_common_prefix_and_removed_list(strs):
    if not strs:
        return "", []
    prefix = strs[0]
    for s in strs:
        while not s.startswith(prefix):
            prefix = prefix[:-1]
            if not prefix:
                return "", strs
    return prefix, [s[len(prefix):] for s in strs]


def jaccard_distance(md5_list1, md5_list2):
    nominator = md5_list1.intersection(md5_list2)
    # 求集合 A 和集合 B 的并集
    denominator = md5_list1.union(md5_list2)
    # 计算比率
    similarity = len(nominator) / len(denominator)
    return similarity


# 递归读取文件夹下所有文件夹
def get_dictory_path(dir_path, return_file_type='queue'):
    if return_file_type == 'queue':

        def get_dictory_path_queue(dir_path):
            dictory_path_queue = multiprocessing.Queue()
            for root, dirs, _ in os.walk(dir_path):
                for dir in dirs:
                    dictory_path = os.path.join(root, dir)
                    dictory_path_queue.put(dictory_path)
            return dictory_path_queue

        return get_dictory_path_queue(dir_path)

    elif return_file_type == 'list':

        def get_dictory_path_list(dir_path):
            dictory_path_list = []
            for root, dirs, _ in os.walk(dir_path):
                for dir in dirs:
                    dictory_path = os.path.join(root, dir)
                    dictory_path_list.append(dictory_path)
            return dictory_path_list

        return get_dictory_path_list(dir_path)


# 不递归的读取当前文件夹的文件
def get_files(dir_path, legal_file_type=('.txt',), return_file_type='queue'):
    if return_file_type == 'queue':
        def get_files_queue(dir_path, legal_file_type=('.txt',)):
            file_path_queue = multiprocessing.Queue()
            file_nums = 0
            for file in os.listdir(dir_path):
                if os.path.splitext(file)[-1] not in legal_file_type:
                    continue
                file_path = os.path.join(dir_path, file)
                file_path_queue.put(file_path)
                file_nums += 1
            return file_path_queue, file_nums

        return get_files_queue(dir_path, legal_file_type)

    elif return_file_type == 'list':

        def get_files_list(dir_path, legal_file_type=('.txt',)):
            file_path_list = []
            for file in os.listdir(dir_path):
                if os.path.splitext(file)[-1] not in legal_file_type:
                    continue
                file_path = os.path.join(dir_path, file)
                file_path_list.append(file_path)
            return file_path_list, len(file_path_list)

        return get_files_list(dir_path, legal_file_type)

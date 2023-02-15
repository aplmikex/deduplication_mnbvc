from de_duplication import remove_duplicates
from convert import convert
import multiprocessing


if __name__ == '__main__':
    convert('/home/xiang/文档/20230124', dst_dir='converted/', n_process=multiprocessing.cpu_count()-1)
    remove_duplicates('converted/', n_process=multiprocessing.cpu_count()-1, windows_size=5)

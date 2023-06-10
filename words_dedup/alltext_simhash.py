import argparse
import os
import tqdm
import jsonlines
import os, sys
current_path = os.path.abspath(__file__)
sys.path.append(os.path.dirname(os.path.dirname(current_path)))
import utils.customSimhash as customSimhash
import pickle
import tempfile
from utils.utils import get_all_files


def deduplication(file_path_list, rs_pkl, simhash_threshold):
    lsh = customSimhash.SimhashIndex([], f=64, k=simhash_threshold)

    if rs_pkl is not None:
        try:
            lsh.bucket = pickle.load(open(rs_pkl, 'rb'))
        except:
            print('不存在该pkl文件，无法读取')

    count_dedup = 0
    for i in tqdm.tqdm(range(len(file_path_list))):
        with jsonlines.open(file_path_list[i]) as input_file, tempfile.NamedTemporaryFile(mode='w', delete=False) as output_file:
            for one_json in input_file:
                simhash_value = customSimhash.Simhash(one_json['alltext_simhash'])
                similar = lsh.add(file_path_list[i] + one_json['文件名'], simhash_value, return_similar=True)
                if similar != "":
                    count_dedup += 1
                    one_json['是否重复文件'] = True
                    with open('重复文件.txt', 'a') as f:
                        f.write(file_path_list[i] + one_json['文件名'] + '和' + similar + '是重复的\n')

                jsonlines.Writer(output_file).write(one_json)

        output_file.close()
        os.replace(output_file.name, file_path_list[i])

    print('一共有:', count_dedup, '个重复文件被检查出来')

    if rs_pkl is not None:
        pickle.dump(lsh.bucket, open(rs_pkl, 'wb'))
        print('已经把文件记录保存到', rs_pkl, '中')


def files_deplication(src_dir='output_csv/', rs_pkl=None, simhash_threshold=3):
    """
    将多个csv中的文件进行去重
    :param src_dir: csv文件路径
    :param rs_pkl: 保存去重结果的pkl文件路径
    :param simhash_threshold: 指定去重阈值，默认为3，也就是simhash值相差3以内算相似
    :param n_process: 指定进程数，最低是2，也就是一个主进程一个检验去重结果进程，默认是13
    """
    file_path_list, file_nums = get_all_files(src_dir, ['.jsonl'], 'list')

    deduplication(file_path_list, rs_pkl, simhash_threshold)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--src_dir', type=str, default='xxqgfilesjsonl copy/', help="源文件夹路径")
    parser.add_argument('--rs_pkl', required=False, help="源文件夹路径")
    parser.add_argument('--simhash_threshold', type=int, default=3, help="指定simhash去重阈值，默认为3")

    args = parser.parse_args()
    with open('重复文件.txt', 'w') as f:
        f.write('')
    files_deplication(args.src_dir, args.rs_pkl, args.simhash_threshold)
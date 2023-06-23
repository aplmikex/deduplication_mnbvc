import argparse
import jsonlines
import tqdm, os
import tempfile
import os, sys
current_path = os.path.abspath(__file__)
sys.path.append(os.path.dirname(os.path.dirname(current_path)))
import utils.customSimhash as customSimhash
from utils.utils import max_size, get_all_files
import multiprocessing

from cityhash import CityHash64

def hashfunc(x):
    return CityHash64(x)

def calculate_simhash(args):
    one_json, hashfunc = args
    text = ''
    for line_json in one_json['段落']:
        text += line_json['内容']

    simhash = customSimhash.Simhash(text, hashfunc=hashfunc)
    one_json['alltext_simhash'] = simhash.value

    return one_json

def convert(src_dir, num_processes):
    file_path_list, file_nums = get_all_files(src_dir, ['.jsonl'], 'list')

    for i in tqdm.tqdm(range(file_nums)):
        with jsonlines.open(file_path_list[i]) as input_file, tempfile.NamedTemporaryFile(mode='w', delete=False) as output_file:
            with multiprocessing.Pool(num_processes) as pool:
                args = [(one_json, hashfunc) for one_json in input_file]
                results = pool.imap_unordered(calculate_simhash, args)

                for result in results:
                    jsonlines.Writer(output_file).write(result)

        output_file.close()
        os.replace(output_file.name, file_path_list[i])

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--directory", help="Directory to convert", required=True)
    parser.add_argument("-p", "--processes", help="Number of processes to use", type=int, default=multiprocessing.cpu_count())
    args = parser.parse_args()

    convert(args.directory, args.processes)
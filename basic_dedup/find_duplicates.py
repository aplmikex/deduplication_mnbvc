import pandas as pd
import argparse


# 查找重复文件
def find_duplicates(pkl_file):
    df = pd.read_pickle(pkl_file)
    duplicates = df[df.duplicated(['SHA256', 'Size'], keep=False)]
    groups = duplicates.groupby(['SHA256', 'Size'])
    with open('duplicates.txt', 'w') as f:
        for _, group in groups:
            files = group['File'].tolist()
            for file in files[1:]:
                f.write(file+'\n')
    df.drop_duplicates(subset=['SHA256', 'Size'], keep='first', inplace=True)
    df.to_pickle(pkl_file)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--pkl_file', required=True, help='The pickle file to read from')

    args = parser.parse_args()
    find_duplicates(args.pkl_file)
import os
import argparse



def clean_file(folder_path, blacklist_file):
    with open(blacklist_file, 'r') as f:
        balsklist = f.readlines()
    
    balsklist = [x.strip() for x in balsklist]

    with open('tobereomve.txt', 'w') as f:
        for root, _, files in os.walk(folder_path):
            for file in files:
                file_path = os.path.join(root, file)
                filename, extension = os.path.splitext(file_path)

                extension = extension.lower()

                # 扩展名带前面的.的，要多算一个
                if extension == '' or len(extension) > 7:
                    f.write(file_path+'\n')
                elif extension[1:] in balsklist:
                    f.write(file_path+'\n')
                elif not all(ord(c) < 128 for c in extension):
                    f.write(file_path+'\n')

            

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--folder_path', type=str, required=True, help="所有文件路径")
    parser.add_argument('--blacklist_file', type=str, required=True, help="后缀名黑名单文件路径")

    args = parser.parse_args()
    clean_file(args.folder_path, args.blacklist_file)
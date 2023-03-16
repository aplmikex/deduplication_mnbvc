from convert import convert
import convert_jsonl_to_csv
import files_deplication
import multiprocessing


if __name__ == '__main__':
    convert('/home/xiang/文档/mnbvcfiles/20230124', dst_dir='./20230124/', n_process=multiprocessing.cpu_count()-2, threshold=0.8)
    convert('/home/xiang/文档/mnbvcfiles/20230101', dst_dir='./20230101/', n_process=multiprocessing.cpu_count()-2, threshold=0.8)
    convert('/home/xiang/文档/mnbvcfiles/20230107', dst_dir='./20230107/', n_process=multiprocessing.cpu_count()-2, threshold=0.8)
    convert_jsonl_to_csv.convert_jsonl_to_csv('./20230124/', './20230124.csv')
    convert_jsonl_to_csv.convert_jsonl_to_csv('./20230101/', './20230101.csv')
    convert_jsonl_to_csv.convert_jsonl_to_csv('./20230107/', './20230107.csv')
    files_deplication.files_deplication(['./20230124.csv', './20230101.csv', './20230107.csv'], 8, 0.85, 1, './20230124_20230101_20230107_step_1.jsonl')
    files_deplication.files_deplication(['./20230124.csv', './20230101.csv'], 8, 0.85, 2, './20230124_20230101_20230107_step_2.jsonl')
    files_deplication.files_deplication(['./20230124.csv', './20230107.csv'], 8, 0.85, 2, './20230124_20230101_20230107_step_2.jsonl')
    files_deplication.files_deplication(['./20230101.csv', './20230107.csv'], 8, 0.85, 2, './20230124_20230101_20230107_step_2.jsonl')
"""
download path list from commoncrawl
"""
import gzip
import os
import requests
import shutil

# Parameter
# 今回処理するwarcのパスリストが圧縮されているURL
# CC-MAIN-2023-50以外にも存在するが, 一旦このURLのみで行う
path_urls = [
    "https://data.commoncrawl.org/crawl-data/CC-MAIN-2023-50/warc.paths.gz",
]

def download_file(url, save_path):

    response = requests.get(url, stream=True)

    if response.status_code == 200:
        with open(save_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=128):
                f.write(chunk)
        print(f"ファイルが正常にダウンロードされました: {save_path}")
    else:
        print(f"ファイルのダウンロードに失敗しました。ステータスコード: {response.status_code}")


def decompress_gz(gz_path, output_path, remove_gz=True, fill_blank_gz=False):
    with gzip.open(gz_path, 'rb') as f_in:
        with open(output_path, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
    print(f"{gz_path}が解凍され、{output_path}に保存されました。")
    if remove_gz:
        os.remove(gz_path)

    if fill_blank_gz:
        with open(gz_path, 'w') as f:
            f.write("")


if __name__ == "__main__":
    # Process
    # Parameterで指定したURLからパス(gz)をダウンロードし,解凍する
    for url in path_urls:
        file_name = url.split("/")[-2]+".gz"
        try:
            # パスリストが格納されているgzファイルをdata_list配下に保存
            download_file(url, f"data/path_list/{file_name}")
            # 保存されたgzファイルを解凍する
            decompress_gz(f"data/path_list/{file_name}",
                        f"data/path_list/{os.path.splitext(file_name)[0]}")
        except:
            pass
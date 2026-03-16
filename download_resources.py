import os
import requests
from urllib.parse import urlparse


import os
import requests
from urllib.parse import urlparse
from tqdm import tqdm


def download_file(url, save_dir=".", filename=None, chunk_size=8192):
    """
    下载文件到指定目录（带进度条）
    """

    os.makedirs(save_dir, exist_ok=True)

    # 推断文件名
    if filename is None:
        filename = os.path.basename(urlparse(url).path)
        if not filename:
            filename = "download_file"

    save_path = os.path.join(save_dir, filename)

    with requests.get(url, stream=True) as r:
        r.raise_for_status()

        total_size = int(r.headers.get("content-length", 0))

        with open(save_path, "wb") as f, tqdm(
            total=total_size,
            unit="B",
            unit_scale=True,
            unit_divisor=1024,
            desc=filename,
        ) as bar:

            for chunk in r.iter_content(chunk_size=chunk_size):
                if chunk:
                    f.write(chunk)
                    bar.update(len(chunk))

    return save_path

HADOOP_VERSION="3.3.6"
SPARK_VERSION="3.5.3"
HIVE_VERSION="3.1.3"

HADOOP_TAR_URL=f"https://www.apache.org/dist/hadoop/common/hadoop-{HADOOP_VERSION}/hadoop-{HADOOP_VERSION}.tar.gz"
SPARK_TAR_URL=f"https://archive.apache.org/dist/spark/spark-{SPARK_VERSION}/spark-{SPARK_VERSION}-bin-hadoop3.tgz"
HIVE_TAR_URL=f"https://archive.apache.org/dist/hive/hive-{HIVE_VERSION}/apache-hive-{HIVE_VERSION}-bin.tar.gz"

HADOOP_ASC_URL = HADOOP_TAR_URL+".asc"
HIVE_ASC_URL = HIVE_TAR_URL+".asc"
SPARK_ASC_URL = SPARK_TAR_URL+".asc"

HADOOP_KEYS = "https://archive.apache.org/dist/hadoop/common/KEYS"
SPARK_KEYS = "https://archive.apache.org/dist/spark/KEYS"
HIVE_KEYS = "https://archive.apache.org/dist/hive/KEYS"

BASE_DIR = "./base/resources"


download_file(HADOOP_TAR_URL,BASE_DIR,"hadoop.tar.gz")
download_file(SPARK_TAR_URL,BASE_DIR,"spark.tgz")
download_file(HIVE_TAR_URL,BASE_DIR,"hive.tar.gz")

download_file(HADOOP_ASC_URL,BASE_DIR,"hadoop.tar.gz.asc")
download_file(SPARK_ASC_URL,BASE_DIR,"spark.tgz.asc")
download_file(HIVE_ASC_URL,BASE_DIR,"hive.tar.gz.asc")

download_file(HADOOP_KEYS,BASE_DIR,"hadoop-KEYS")
download_file(SPARK_KEYS,BASE_DIR,"spark-KEYS")
download_file(HIVE_KEYS,BASE_DIR,"hive-KEYS")
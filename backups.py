import yaml
import os
from pymongo import MongoClient
import time
import re
from datetime import datetime


def main():
    with open("config.yaml", "r") as ymlfile:
        cfg = yaml.load(ymlfile, Loader=yaml.FullLoader)
    try:
        jsonArr = []
        for location in cfg['locations']['path']:
            dirName = location
            listOfFiles = list()
            for (dirpath, dirnames, filenames) in os.walk(dirName):
                for file in filenames:
                    # print(i)
                    if file.lower().endswith('.tib') or file.lower().endswith('.tibx'):
                        if file.lower().endswith('.tibx'):
                            backupType = "Cyber Protect"
                            pattern = re.compile(
                                "[-]\w\w\w\w\w\w\w\w[-]\w\w\w\w[-]\w\w\w\w[-]\w\w\w\w[-]\w\w\w\w\w\w\w\w\w\w\w\w[-]\w\w\w\w\w\w\w\w[-]\w\w\w\w[-]\w\w\w\w[-]\w\w\w\w[-]\w\w\w\w\w\w\w\w\w\w\w\w\w[-]")
                            pattern2 = re.compile("[-]\w\w\w\w\w\w\w\w[-]")
                            pattern3 = re.compile("[-][0-9]{4}[.]")
                            if pattern.match(file):
                                hostname = re.split(pattern, file)
                                # print(hostname)
                            elif pattern2.match(file):
                                hostname = re.split(pattern2, file)
                                # print(hostname)
                            elif pattern3.match(file):
                                hostname = re.split(pattern3, file)
                                # print(hostname)
                        else:
                            backupType = "True Image"
                            pattern = re.compile(
                                "[_][a-z]{3,4}[_][b][0-9]{1,2}[_][s][0-9]{0,2}[_][v][0-9]")
                            hostname = re.split(pattern, file)

                        extension = file.split(".")[-1]
                        path = os.path.join(dirpath, file)
                        date = time.ctime(os.path.getctime(path))
                        size = os.stat(path).st_size / (1024 * 1024)
                        jsonArr.append({
                            'hostname': hostname[0],
                            'backupType': backupType,
                            'extension': extension,
                            'size_MB': size,
                            'date': date,
                            'path': path,
                        })
                    else:
                        continue
    except:
        try:
            URI = "mongodb://" + \
                str(cfg['mongodb']['host']) + ":" + str(cfg['mongodb']['port'])
            client = MongoClient(URI)
            database = client[str(cfg['mongodb']['dbname'])]
            table = database[str(cfg['mongodb']['backup_error'])]
            errorArr = {
                'date': datetime.now(),
                'script': os.path.basename(__file__),
                'error': 'Error getting information',
            }
            print('some error')
        except:
            print('MongoDB Error')
        finally:
            client.close()
    finally:
        try:
            URI = "mongodb://" + \
                str(cfg['mongodb']['host']) + ":" + str(cfg['mongodb']['port'])
            client = MongoClient(URI)
            database = client[str(cfg['mongodb']['dbname'])]
            table = database[str(cfg['mongodb']['table'])]
            table.drop()
            table.insert_many(jsonArr)
        except:
            print('MongoDB Error')
        finally:
            client.close()


if __name__ == '__main__':
    main()

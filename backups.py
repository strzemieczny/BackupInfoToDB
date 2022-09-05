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
                            # FTPXI36-23C4870E-3CC0-46ED-AA8E-696CD921F27E-9C78F653-42F2-44B2-8E08-68C48A6A14ADA-0004.tibx
                            backupType = "Cyber Protect"
                            pattern = "[-]\w\w\w\w\w\w\w\w[-]\w\w\w\w[-]\w\w\w\w[-]\w\w\w\w[-]\w\w\w\w\w\w\w\w\w\w\w\w[-]\w\w\w\w\w\w\w\w[-]\w\w\w\w[-]\w\w\w\w[-]\w\w\w\w[-]\w\w\w\w\w\w\w\w\w\w\w\w\w[-]"
                            pattern2 = "[-]\w\w\w\w\w\w\w\w[-]\w\w\w\w[-]\w\w\w\w[-]\w\w\w\w[-]\w\w\w\w\w\w\w\w\w\w\w\w[-]\w\w\w\w\w\w\w\w[-]\w\w\w\w[-]\w\w\w\w[-]\w\w\w\w[-]\w\w\w\w\w\w\w\w\w\w\w\w\w"
                            if re.split(pattern, file):
                                hostname = re.split(pattern, file)
                            if re.split(pattern2, file):
                                hostname = re.split(pattern2, file)

                        else:
                            pattern = "[_][a-z]{3,4}[_][b][0-9]{1,2}[_][s][0-9][_][v][0-9]"
                            hostname = re.split(pattern, file)
                            backupType = "True Image"

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

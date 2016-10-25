# encoding=utf-8

import os
if __name__ == '__main__':
    for file in os.listdir('.'):
        if file.endswith('jpg') or file.endswith('jpeg'):
            os.remove(file)

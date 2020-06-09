#!/usr/bin/env python3

import site
import configs

SOURCE_CODE_FILEPATH = '/home/jovyan/work/src'


def set_import_path(import_path=configs.SOURCE_CODE_FILEPATH):
    site.addsitedir(import_path)
    print("Added the following path to the import paths "
          "list:\n{}".format(import_path))

if __name__ == '__main__':
    set_import_path()
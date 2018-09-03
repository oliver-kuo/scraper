# -*- coding: utf-8 -*-
"""
Created on Sat Aug 11 14:28:32 2018

@author: Oliver
"""

import json


def read_db_config(filename='config.json', section='mysql'):
    config = {}
    with open(filename, 'r') as f:
        config = json.load(f)
    
    db = {}
    if section in config:
        db = config[section]
    else:
        raise Exception('{0} not found in the {1} file'.format(section, filename))

    return db

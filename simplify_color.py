# -*- coding: utf-8 -*-
"""
Created on Sat Aug 18 14:05:05 2018

@author: Oliver
"""

import re
import logging
# import csv

def simplify_color(vin, stock_number, color):
    logger = logging.getLogger(__name__)

    if color is None or \
       not color or \
       re.search('not available', color, re.IGNORECASE):
        logger.warning('%s %s %s color not found', vin, stock_number, color)
        return None
    elif re.search('white', color, re.IGNORECASE):
        return 'White'
    elif re.search('black', color, re.IGNORECASE):
        return 'Black'
    elif re.search('silver', color, re.IGNORECASE):
        return 'Silver'
    elif re.search('grey', color, re.IGNORECASE):
        return 'Grey'
    elif re.search('blue', color, re.IGNORECASE):
        return 'Blue'
    elif re.search('red', color, re.IGNORECASE):
        return 'Red'
    elif re.search('violet', color, re.IGNORECASE):
        return 'Violet'
    elif re.search('(brown)|(mocha)|(terra)|(storm)', color, re.IGNORECASE):
        return 'Brown'
    elif re.search('(beige)|(ivory)|(oyster)', color, re.IGNORECASE):
        return 'Beige'
    elif re.search('green', color, re.IGNORECASE):
        return 'Green'
    elif re.search('yellow', color, re.IGNORECASE):
        return 'Yellow'
    elif re.search('orange', color, re.IGNORECASE):
        return 'Orange'
    else:
        logger.warning('%s %s %s no matching color', vin, stock_number, color)
        return color


'''
# testing
with open('mb_int_color.csv', newline='\n') as csvfile:
    r = csv.reader(csvfile, delimiter=',')
    f = open('test.txt','w')
    for row in r:
        if row[1] != simplify_mb_int_color(row[0]):
            print(row[0] + ',' + row[1] + ',' + simplify_mb_int_color(row[0]))
    f.close()
'''
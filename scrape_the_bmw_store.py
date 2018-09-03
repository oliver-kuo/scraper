# -*- coding: utf-8 -*-
"""
Created on Sun Aug 12 10:49:53 2018

@author: Oliver
"""

import csv
import json
import logging.config
import re
import requests
from datetime import datetime
from dbconfig import read_db_config
from mysql.connector import MySQLConnection
from simplify_color import simplify_color


def getPage(year_from, year_to, p = 1):
    url = domain + '/index/getsearchconditions'
    params = {'make':'BMW',
              'model':'All',
              'dealerid':20,
              'pagenumber':p,
              'certified':2,
              'yearfrom':year_from,
              'yearto':year_to,
              'carperpage':100,
              'action':'getsearchconditions'}
    r = requests.get(url, params=params)
    
    if r.status_code != requests.codes.ok:
        r.raise_for_status()

    return r.json()


def get_model(vin, stock_number, trim):
    if trim is None or trim == '\\N':
        logger.warning('%s %s model not found', vin, stock_number)
        return '\\N'
    if re.search('1\d\di', trim, re.IGNORECASE):
        return '1 Series'
    elif re.search('2\d\di', trim, re.IGNORECASE):
        return '2 Series'
    elif re.search('m2( |$)', trim, re.IGNORECASE):
        return 'M2'
    elif re.search('3\d\d(i|d)', trim, re.IGNORECASE):
        return '3 Series'
    elif re.search('m3( |$)', trim, re.IGNORECASE):
        return 'M3'
    elif re.search('4\d\di', trim, re.IGNORECASE):
        return '4 Series'
    elif re.search('m4( |$)', trim, re.IGNORECASE):
        return 'M4'
    elif re.search('5\d\d(i|d)', trim, re.IGNORECASE):
        return '5 Series'
    elif re.search('m5( |$)', trim, re.IGNORECASE):
        return 'M5'
    elif re.search('6\d\di', trim, re.IGNORECASE):
        return '6 Series'
    elif re.search('m6( |$)', trim, re.IGNORECASE):
        return 'M6'
    elif re.search('7\d\dl?(i|d)', trim, re.IGNORECASE):
        return '7 Series'
    elif re.search('x1 m$', trim, re.IGNORECASE):
        return 'X1 M'
    elif re.search('x1', trim, re.IGNORECASE):
        return 'X1'
    elif re.search('x2 m$', trim, re.IGNORECASE):
        return 'X2 M'
    elif re.search('x2', trim, re.IGNORECASE):
        return 'X2'
    elif re.search('x3 m$', trim, re.IGNORECASE):
        return 'X3 M'
    elif re.search('x3', trim, re.IGNORECASE):
        return 'X3'
    elif re.search('x4 m$', trim, re.IGNORECASE):
        return 'X4 M'
    elif re.search('x4', trim, re.IGNORECASE):
        return 'X4'
    elif re.search('x5 m$', trim, re.IGNORECASE):
        return 'X5 M'
    elif re.search('x5', trim, re.IGNORECASE):
        return 'X5'
    elif re.search('x6 m$', trim, re.IGNORECASE):
        return 'X6 M'
    elif re.search('x6', trim, re.IGNORECASE):
        return 'X6'
    elif re.search('z4', trim, re.IGNORECASE):
        return 'Z4'
    elif re.search('i3', trim, re.IGNORECASE):
        return 'i3'
    elif re.search('i8', trim, re.IGNORECASE):
        return 'i8'
    else:
        logger.warning('%s %s no matching model', vin, stock_number)
        return '\\N'


def get_body_type(vin, stock_number, trim):
    if trim is None or trim == '\\N':
        logger.warning('%s %s body type not found', vin, stock_number)
        return '\\N'
    if re.search('x\d', trim, re.IGNORECASE):
        return 'SUV'
    elif re.search('(sedan)|(gran coupe)', trim, re.IGNORECASE):
        return 'Sedan'
    elif re.search('coupe', trim, re.IGNORECASE):
        return 'Coupe'
    elif re.search('(cabriolet)|(roadster)|(convertible)', trim, re.IGNORECASE):
        return 'Convertible'
    elif re.search('(gran turismo)|(hatchback)', trim, re.IGNORECASE):
        return 'Hatchback'
    elif re.search('(touring)|(wagon)', trim, re.IGNORECASE):
        return 'Wagon'
    elif re.search('(5\d\d(i|d|e))|(m5( |$))|(7\d\dl?(i|d|e))', trim, re.IGNORECASE):
        return 'Sedan'
    else:
        logger.warning('%s %s no matching body type', vin, stock_number)
        return '\\N'


def scrub_xdrive1(matchobj):
    return matchobj.group(1) + matchobj.group(2)


def scrub_xdrive2(matchobj):
    return matchobj.group(1)


def scrub_trim(trim):
    t = trim.strip()
    t = re.sub('^(m|x|z)\d( |$)', '', t, flags=re.IGNORECASE)
    t = re.sub('(^| )(sedan)|(gran coupe)|(gran turismo)|(coupe)|(cabriolet)|(convertible)|(roadster)$', '', t, flags=re.IGNORECASE)
    t = re.sub('(xdrive) (\d\d(i|d|e))', scrub_xdrive1, t, flags=re.IGNORECASE)
    t = re.sub('(xdrive(\d\d(i|d|e))?).*$', scrub_xdrive2, t, flags=re.IGNORECASE)
    t = re.sub('l', 'L', t)
    t = t.strip()

    if t == '':
        t = '\\N'

    return t


def store(vehicles, csvwriter):
    for v in vehicles:
        vin = v.get('VinNumber')
        if vin is None:
            logger.warning('vin not found')
            vin = '\\N'
            
        stock_number = v.get('StockNumber')
        if stock_number is None:
            logger.warning('%s stock number not found', vin)
            stock_number = '\\N'

        type = v.get('Certified')
        if type is None:
            logger.warning('%s %s type not found', vin, stock_number)
            type = '\\N'
        elif type == 0:
            type = 'u'
        else:
            type = 'c'

        model_year = v.get('Year')
        if model_year is None:
            logger.warning('%s %s model year not found', vin, stock_number)
            model_year = '\\N'
        elif not model_year.isdigit():
            logger.warning('%s %s %s not a valid model year', vin, stock_number, model_year)
            model_year = '\\N'
            
        make = v.get('MakeName')
        if make is None:
            logger.warning('%s %s make not found', vin, stock_number)
            make = '\\N'
            
        model_name = v.get('ModelName')
        if model_name is None:
            logger.warning('%s %s model name not found', vin, stock_number)
            
        trim_line = v.get('TrimLine')
        if trim_line is None:
            logger.warning('%s %s trim line not found', vin, stock_number)
            
        if model_name is None and trim_line is None:
            trim = '\\N'
        elif model_name is None and trim_line is not None:
            trim = trim_line
        elif model_name is not None and trim_line is None:
            trim = model_name
        else:
            trim = model_name + ' ' + trim_line
            
        model = get_model(vin, stock_number, trim)
        body_type = get_body_type(vin, stock_number, trim)

        id = v.get('Id')
        drive = '\\N'
        if id is None:
            logger.warning('%s %s id not found', vin, stock_number)
        else:
            details_resp = requests.get('http://thebmwstore.ca/used-bmw-cars/' + \
                                        id + '/' + re.sub(' ', '-', trim))
            
            if details_resp.status_code == requests.codes.ok:
                if re.search('all wheel drive', details_resp.text, re.IGNORECASE):
                    drive = 'AWD'
                elif re.search('rear wheel drive', details_resp.text, re.IGNORECASE):
                    drive = 'RWD'
                elif re.search('front wheel drive', details_resp.text, re.IGNORECASE):
                    drive = 'FWD'
                else:
                    logger.warning('%s %s drive type not found', vin, stock_number)
            else:
                logger.warning('%s %s failed to retrieve details page (status code %s)',
                               vin, stock_number, details_resp.status_code)

        trim = scrub_trim(trim)

        with open('bmw_displacement.json', 'r') as f:
            disp_dict = json.load(f)

        disp_key = str(model_year) + ' ' + model
        if trim != '\\N':
            disp_key += ' ' + trim
        displacement = disp_dict.get(disp_key)
        if displacement is None:
            logger.warning('%s %s %s no matching displacement',
                           vin, stock_number, disp_key)
            displacement = '\\N'

        mileage = v.get('Kms')
        if mileage is None:
            logger.warning('%s %s mileage not found', vin, stock_number)
            mileage = '\\N'
        elif not mileage.isdigit():
            logger.warning('%s %s %s not a valid mileage', vin, stock_number, mileage)
            mileage = '\\N'
            
        ext_color = v.get('ExteriorColourName')
        ext_color = simplify_color(vin, stock_number, ext_color)

        int_color = v.get('InteriorColourName')
        int_color = simplify_color(vin, stock_number, int_color)

        transmission = v.get('TransmissionName')
        if transmission is None:
            logger.warning('%s %s transmission type not found', vin, stock_number)
            transmission = '\\N'
        else:
            if re.search('automatic', transmission, re.IGNORECASE):
                transmission = 'a' 
            elif re.search('manual', transmission, re.IGNORECASE):
                transmission = 'm'
            else:
                logger.warning('%s %s %s no matching transmission type',
                               vin, stock_number, transmission)
                transmission = '\\N'

        fuel = '\\N'
        if re.search('gas', v['FuelTypeName'], re.IGNORECASE):
            fuel = 'g'
        elif re.search('diesel', v['FuelTypeName'], re.IGNORECASE):
            fuel = 'd'
        elif re.search('hybrid', v['FuelTypeName'], re.IGNORECASE):
            fuel = 'h'

        price = v.get('SalePrice')
        if price is None:
            logger.warning('%s %s price not found', vin, stock_number)
            price = '\\N'
        else:
            price = re.sub('\.00$', '', price)
            if not price.isdigit():
                logger.warning('%s %s %s not a valid price', vin, stock_number, price)
                price = '\\N'

        thumbnail = v.get('MainPicture')
        if thumbnail is None:
            logger.warning('%s %s thumbnail not found', vin, stock_number)
            thumbnail = '\\N'
        else:
            thumbnail = domain + thumbnail
        
        dealer = 'The BMW Store'

        csvwriter.writerow([vin, stock_number, type, model_year, make,
                            model, trim, body_type, drive, displacement,
                            mileage, ext_color, int_color, transmission,
                            fuel, price, thumbnail, dealer])


if __name__ == '__main__':
    conn = cursor = None
    
    try:
        with open('scrape_the_bmw_store_config.json', 'r') as f:
            config = json.load(f)
    
        logging_config = config.get("logging")
        if logging_config is None:
            logging.basicConfig(filename='./log/scrape_the_bmw_store.log',level=logging.INFO)
        else:
            logging.config.dictConfig(logging_config)
        logger = logging.getLogger(__name__)

        domain = config.get('domain')
        if domain is None:
            raise Exception('domain not found in scrape_the_bmw_store_config.json')

        year_from = config.get('year_from')
        if year_from is None:
            raise Exception('year_from not found in scrape_the_bmw_store_config.json')
    
        year_to = config.get('year_to')
        if year_to is None:
            raise Exception('year_to not found in scrape_the_bmw_store_config.json')
    
        rjson = getPage(year_from, year_to)
        nPages = rjson[3]
        nVehicles = rjson[4]
    
        datafilename = './data/the-bmw-store-{0}.csv'.format(
                datetime.today().strftime('%Y-%m-%d'))
    
        with open(datafilename, 'wb') as tbsfile:
            tbswriter = csv.writer(tbsfile)
            store(rjson[1], tbswriter)
    
            for p in range(2, int(nPages) + 1):
                rjson = getPage(year_from, year_to, p)
                store(rjson[1], tbswriter)
    
        db_config = read_db_config()
        conn = MySQLConnection(**db_config)

        if conn.is_connected():
            cursor = conn.cursor()
            load = 'LOAD DATA LOCAL INFILE \'{0}\' ' \
                   'INTO TABLE car ' \
                   'COLUMNS TERMINATED BY \',\' ' \
                   'OPTIONALLY ENCLOSED BY \'"\' ' \
                   '(vin, stock_number, type, model_year, make, model, ' \
                   'trim, body_type, drive, displacement, mileage, ' \
                   'ext_color, int_color, transmission, fuel, price, ' \
                   'thumbnail, dealer)'.format(datafilename)
            cursor.execute(load)
        else:
            raise Exception('Failed to connect to database')

        conn.commit()
    except Exception as e:
        logger.exception(e)
    finally:
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()

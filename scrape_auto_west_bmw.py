# -*- coding: utf-8 -*-
"""
Created on Sun Aug 12 21:02:50 2018

@author: Oliver
"""

import csv
import json
import logging.config
import re
import requests
import sys
from bs4 import BeautifulSoup
from datetime import datetime
from dbconfig import read_db_config
from mysql.connector import MySQLConnection


def get_page(year, stock_type = 'used', p = 1):
    params = {'language':'en',
              'make':'BMW',
              'page':p,
              'page_length':100,
              'year':year}

    if stock_type == 'used' or stock_type == 'demo':
        params['stock_type'] = stock_type
    else:
        logger.warning('get_page stock_type must be either "used" or "demo", default to "used"')
        params['stock_type'] = 'used'

    r = requests.get(
            domain + '/wp-json/strathcom/vehicles/search',
            params=params)

    if r.status_code != requests.codes.ok:
        r.raise_for_status()

    data = r.json().get('data')

    if data is None:
        raise Exception('data section not found in json on %s page %s for year %s',
                        stock_type, p, year)

    return data


def store(vehicles, csvwriter):
    for v in vehicles:
        identifiers = v.get('identifiers')
        vin = '\\N'
        if identifiers is None:
            logger.warning('identifiers not found')
        else:
            vin = identifiers.get('vin')
            if vin is None:
                logger.warning('vin not found')
                vin = '\\N'

        stock_number = v.get('stock_number')
        if stock_number is None:
            logger.warning('%s stock number not found', vin)
            stock_number = '\\N'

        certification = v.get('certification')
        if certification is None or not certification:
            type = 'u'
        else:
            type = 'c'

        model_year = v.get('year')
        if model_year is None:
            logger.warning('%s %s model year not found', vin, stock_number)
            model_year = '\\N'
        elif not isinstance(model_year, (int, long)):
            logger.warning('%s %s %s not a valid model year', vin, stock_number, model_year)
            model_year = '\\N'
            
        make = v.get('make')
        if make is None:
            logger.warning('%s %s make not found', vin, stock_number)
            model_year = '\\N'

        model = v.get('model')
        if model is None:
            logger.warning('%s %s model not found', vin, stock_number)
            model = '\\N'
        elif model == 'I3':
            model = 'i3'
        elif model == 'I8':
            model = 'i8'

        trim = '\\N'
        trim_section = v.get('trim')
        if trim_section is None:
            logger.warning('%s %s trim section not found', vin, stock_number)
        else:
            trim = trim_section.get('value')
            if trim is None:
                logger.warning('%s %s trim value not found', vin, stock_number)
                trim = '\\N'
            else:
                if trim == 'Base':
                    trim = '\\N'

        body_type = v.get('body_type')
        if body_type is None:
            logger.warning('%s %s body type not found', vin, stock_number)
            body_type = '\\N'
            
        drive = v.get('driveType')
        if drive is None:
            logger.warning('%s %s drive type not found', vin, stock_number)
            drive = '\\N'

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

        mileage = v.get('odometer_value')
        if mileage is None:
            logger.warning('%s %s mileage not found', vin, stock_number)
            mileage = '\\N'
        else:
            mileage = re.sub(',', '', mileage)
            if not mileage.isdigit():
                logger.warning('%s %s %s not a valid mileage', vin, stock_number, mileage)
                mileage = '\\N'

        color = v.get('color')
        ext_color = int_color = '\\N'
        if color is None:
            logger.warning('%s %s color not found', vin, stock_number)
        else:
            exterior = color.get('exterior')
            if exterior is None:
                logger.warning('%s %s exterior not found', vin, stock_number)
            else:
                ext_color = exterior.get('generic')
                if ext_color is None:
                    logger.warning('%s %s exterior color not found', vin, stock_number)
                    ext_color = '\\N'

            interior = color.get('interior')
            if interior is None:
                logger.warning('%s %s interior not found', vin, stock_number)
            else:
                int_color = interior.get('generic')
                if int_color is None:
                    logger.warning('%s %s interior color not found', vin, stock_number)
                    int_color = '\\N'

        transmission = v.get('transmission')
        if transmission is None:
            logger.warning('%s %s transmission type not found', vin, stock_number)
            transmission = '\\N'
        elif re.search('automatic', transmission, re.IGNORECASE):
            transmission = 'a'
        elif re.search('manual', transmission, re.IGNORECASE):
            transmission = 'm'
        else:
            logger.warning('%s %s %s no matching transmission',
                           vin, stock_number, transmission)
            transmission = '\\N'

        pricing_data = v.get('pricingData')
        price = '\\N'
        if pricing_data is None:
            logger.warning('%s %s pricing data not found', vin, stock_number)
        else:
            price = pricing_data.get('current')
            if price is None:
                logger.warning('%s %s price not found', vin, stock_number)
                price = '\\N'
            elif not isinstance(price, (int, long, float)):
                logger.warning('%s %s %s not a valide price', vin, stock_number, price)
                price = '\\N'

        fuel = '\\N'
        engine = v.get('engine')
        if engine is None:
            logger.warning('%s %s engine not found', vin, stock_number)
        else:
            fuel = engine.get('fuel_type')
            if fuel is None:
                logger.warning('%s %s fuel type not found', vin, stock_number)
                fuel = '\\N'
            else:
                if re.search('unleaded', fuel, re.IGNORECASE):
                    fuel = 'g'
                elif re.search('diesel', fuel, re.IGNORECASE):
                    fuel = 'd'
                elif re.search('(electric)|(flex)', fuel, re.IGNORECASE):
                    fuel = 'h'
                else:
                    logger.warning('%s %s %s no matching fuel type',
                                   vin, stock_number, fuel)
                    fuel = '\\N'

        id = v.get('id')
        if id is None:
            logger.warning('%s %s id not found', vin, stock_number)
        
        equipment = '\\N'
        if model_year != '\\N' and make != '\\N' and \
           model != '\\N' and id is not None:
            url = domain + '/inventory/used/' + str(model_year) + '-' + \
                  make + '-' + re.sub(' ', '-', model) + \
                  '-richmond-british-columbia/' + str(id)
            details_resp = requests.get(url)

            if details_resp.status_code == requests.codes.ok:
                soup = BeautifulSoup(details_resp.text, 'lxml')

                equipment_list = soup.find('dl', attrs={'id':'equipment-list'})
                if equipment_list is None:
                    logger.warning('%s %s equipment-list not found',
                                   vin, stock_number)
                else:
                    equipment = equipment_list.get_text('\n', strip=True)
            else:
                logger.warning('%s %s failed to retrieve details page (status code %s)',
                               vin, stock_number, details_resp.status_code)

        carproof = '\\N'
        carproof_section = v.get('carproof')
        if carproof_section is not None:
            has_carproof = carproof_section.get('hasCarproof')
            if has_carproof is not None and has_carproof == 'true':
                logger.info('%s %s has CarProof', vin, stock_number)

        thumbnail = '\\N'
        photos = v.get('photos')
        if photos is None:
            logger.warning('%s %s photos not found', vin, stock_number)
        else:
            user_photos = photos.get('user')
            stock_photos = photos.get('stock')
            if user_photos is not None and len(user_photos) > 0:
                thumbnail = 'http:' + user_photos[0]
            elif stock_photos is not None and len(stock_photos) > 0:
                thumbnail = 'http:' + stock_photos[0]
            else:
                logger.warning('%s %s thumbnail not found', vin, stock_number)
                thumbnail = '\\N'

        dealer = 'Auto West BMW'

        csvwriter.writerow([vin, stock_number, type, model_year, make,
                            model, trim, body_type, drive, displacement,
                            mileage, ext_color, int_color, transmission,
                            fuel, price, equipment, carproof, thumbnail,
                            dealer])


if __name__ == '__main__':
    conn = cursor = None
    
    try:
        with open('scrape_auto_west_bmw_config.json', 'r') as f:
            config = json.load(f)

        logging_config = config.get("logging")
        if logging_config is None:
            logging.basicConfig(filename='./log/scrape_auto_west_bmw.log',
                                level=logging.INFO)
        else:
            logging.config.dictConfig(logging_config)
        logger = logging.getLogger(__name__)

        if sys.argv[1] == 'used' or sys.argv[1] == 'demo':
            stock_type = sys.argv[1]
        else:
            logger.warning('1st command-line argument must either be "used" or "demo", default to "used"')
            stock_type = 'used'

        domain = config.get('domain')
        if domain is None:
            raise Exception('domain not found in scrape_auto_west_bmw_config.json')
            
        year_from = config.get('year_from')
        if year_from is None:
            raise Exception('year_from not found in scrape_auto_west_bmw_config.json')
    
        year_to = config.get('year_to')
        if year_to is None:
            raise Exception('year_to not found in scrape_auto_west_bmw_config.json')
    
        datafilename = './data/auto-west-bmw-{0}-{1}.csv'.format(
                stock_type, datetime.today().strftime('%Y-%m-%d'))
    
        with open(datafilename, 'wb') as awbfile:
            awbwriter = csv.writer(awbfile)
    
            for year in range(year_from, year_to + 1):
                rjson = get_page(year, stock_type, 1)
                nPages = rjson['pages']
                nVehicles = rjson['total']
    
                store(rjson['vehicles'], awbwriter)
    
                for p in range(2, int(nPages) + 1):
                    rjson = get_page(year, stock_type, p)
                    store(rjson['vehicles'], awbwriter)
    
        db_config = read_db_config()
        conn = MySQLConnection(**db_config)

        if conn.is_connected():
            cursor = conn.cursor()
            query = 'LOAD DATA LOCAL INFILE \'{0}\' ' \
                    'INTO TABLE car ' \
                    'COLUMNS TERMINATED BY \',\' ' \
                    'OPTIONALLY ENCLOSED BY \'"\' ' \
                    '(vin, stock_number, type, model_year, make, model, ' \
                    'trim, body_type, drive, displacement, mileage, ' \
                    'ext_color, int_color, transmission, fuel, price, ' \
                    'equipment, carproof, thumbnail, dealer)'.format(datafilename)
            cursor.execute(query)
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

# -*- coding: utf-8 -*-
"""
Created on Sun Aug  5 09:25:32 2018

@author: Oliver
"""

import csv
import json
import logging.config
import re
import requests
from bs4 import BeautifulSoup
from datetime import datetime
from dbconfig import read_db_config
from mysql.connector import MySQLConnection
from simplify_color import simplify_color


def get_body_type(vin, stock_number, body_type):
    if body_type is None:
        logger.warning('%s %s body type not found', vin, stock_number)
        return '\\N'
    elif re.search('(hatchback)|(sports tourer)', body_type, re.IGNORECASE):
        return 'Hatchback'
    elif re.search('suv(/coupe)?', body_type, re.IGNORECASE):
        return 'SUV'
    elif re.search('sedan', body_type, re.IGNORECASE):
        return 'Sedan'
    elif re.search('coupe', body_type, re.IGNORECASE):
        return 'Coupe'
    elif re.search('(cabriolet)|(convertible)|(roadster)', body_type, re.IGNORECASE):
        return 'Convertible'
    elif re.search('wagon', body_type, re.IGNORECASE):
        return 'Wagon'
    else:
        logger.warning('%s %s %s no matching body type', vin, stock_number, body_type)
        return '\\N'


def get_drive(vin, stock_number, drive):
    if drive is None:
        logger.warning('%s %s drive type not found', vin, stock_number)
        return '\\N'
    elif re.search('all( |-)wheel drive', drive, re.IGNORECASE):
        return 'AWD'
    elif re.search('rear( |-)wheel drive', drive, re.IGNORECASE):
        return 'RWD'
    elif re.search('front( |-)wheel drive', drive, re.IGNORECASE):
        return 'FWD'
    else:
        logger.warning('%s %s %s no matching drive type', vin, stock_number, drive)
        return '\\N'


def get_int_color(vin, stock_number, color):
    if color is None or not color:
        logger.warning('%s %s interior color not found', vin, stock_number)
        return None
    elif re.search('black.*((black)|(grey)|(red))', color, re.IGNORECASE):
        return 'Black'
    elif re.search('brown.*black', color, re.IGNORECASE):
        return 'Brown'
    elif re.search('grey.*black', color, re.IGNORECASE):
        return 'Grey'
    elif re.search('red.*black', color, re.IGNORECASE):
        return 'Red'
    elif re.search('black', color, re.IGNORECASE):
        return 'Black'
    elif re.search('(beige)|(porcelain)|(stone)', color, re.IGNORECASE):
        return 'Beige'
    elif re.search('brown', color, re.IGNORECASE):
        return 'Brown'
    elif re.search('grey', color, re.IGNORECASE):
        return 'Grey'
    elif re.search('red', color, re.IGNORECASE):
        return 'Red'
    else:
        logger.warning('%s %s %s no matching interior color',
                       vin, stock_number, color)
        return color


def get_transmission(vin, stock_number, transmission):
    if transmission is None:
        logger.warning('%s %s transmission type not found', vin, stock_number)
        return '\\N'
    elif transmission == 'Automatic':
        return 'a'
    elif transmission == 'Manual':
        return 'm'
    else:
        logger.warning('%s %s %s no matching transmission type',
                       vin, stock_number, transmission)
        return '\\N'
    
    
def get_nonce():
    r = requests.get('https://www.inventory.mercedes-benz.ca/used-vehicles/')
    
    if r.status_code != requests.codes.ok:
        r.raise_for_status()

    m = re.search('"ajax_nonce":"(.+?)"', r.text)
    
    if m is None:
        raise Exception('Nonce not found')

    return m.group(1)


def get_page(nonce, years, p = 1):
    r = requests.post(
            'https://www.inventory.mercedes-benz.ca/en',
            data={
               'action':'im_ajax_call',
               'perform':'get_results',
               'order':'V6Y 0C1',
               'orderby':'distance',
               'page':p,
               'year[]':years,
               'type[]':['Certified Used', 'Used'],
               'location[]':[
                   'Mercedes-Benz Boundary<br>3550 Lougheed Hwy<br>Vancouver, BC V5M 2A3<br>604-639-3300',
                   'Mercedes-Benz North Vancouver<br>1375 Marine Drive<br>North Vancouver, BC V7P 3E5<br>604-331-2369',
                   'Mercedes-Benz Richmond<br>5691 Parkwood Way<br>Richmond, BC V6V 2M6<br>604-278-7662',
                   'Mercedes-Benz Vancouver<br>550 Terminal Avenue<br>Vancouver, BC V6A 0C3<br>604-736-7411'],
               '_nonce':nonce,
               '_post_id':5,
               '_referer':'/used-vehicles/'},
            headers={
                'X-Requested-With':'XMLHttpRequest'})
    
    if r.status_code != requests.codes.ok:
        r.raise_for_status()
        
    return r.json()


def store(html, csvwriter):
    soup = BeautifulSoup(html, 'lxml')
    pattern = re.compile('vehicle list-view used-vehicle (cpo-vehicle )?publish')

    for vehicle in soup.find_all('div', class_=pattern):
        vid = vehicle.get('id')
        vin = '\\N'
        if vid is None:
            logger.warning('vehicle id not found')
        else:
            vid_split = vid.split('-')
            if vid_split:
                vin = vid_split[-1]
            else:
                logger.warning('vin not found')

        stock_number = vehicle.get('data-stock')
        if stock_number is None:
            logger.warning('%s stock number not found', vin)
            stock_number = '\\N'

        type = vehicle.get('data-type')
        if type is None:
            logger.warning('%s %s type not found', vin, stock_number)
        elif type == 'Certified Used':
            type = 'c'
        elif type == 'Used':
            type = 'u'
        else:
            logger.warning('%s %s %s no matching type', vin, stock_number, type)
            type = '\\N'

        model_year = vehicle.get('data-year')
        if model_year is None:
            logger.warning('%s %s model year not found', vin, stock_number)
            model_year = '\\N'
        elif not model_year.isdigit():
            logger.warning('%s %s %s not a valid model year', vin, stock_number, model_year)
            model_year = '\\N'
            
        make = vehicle.get('data-make')
        if make is None:
            logger.warning('%s %s make not found', vin, stock_number)
            make = '\\N'

        model = vehicle.get('data-model')
        if model is None:
            logger.warning('%s %s model not found', vin, stock_number)
            model = '\\N'
        elif model == 'M-Class':
            model = 'ML-Class'

        trim = vehicle.get('data-trim')
        if trim is None:
            logger.warning('%s %s trim not found', vin, stock_number)
            trim = '\\N'

        body_type = vehicle.get('data-body')
        body_type = get_body_type(vin, stock_number, body_type)

        options = vehicle.find('div', attrs={'class':'options'})
        drive = '\\N'
        if options is None:
            logger.warning('%s %s options not found', vin, stock_number)
        else:
            for li in options.find_all('li'):
                detail_label = li.find('span', attrs={'class':'detail-label'})
                if detail_label is not None:
                    if re.search('drivetrain:', detail_label.text, re.IGNORECASE):
                        drive = li.find('span', attrs={'class':'detail-content'}).text
                        drive = get_drive(vin, stock_number, drive)

        with open('mb_displacement.json', 'r') as f:
            disp_dict = json.load(f)
            
        disp_key = model_year + ' ' + trim
        displacement = disp_dict.get(disp_key)
        if displacement is None:
            logger.warning('%s %s %s no matching displacement',
                           vin, stock_number, disp_key)
            displacement = '\\N'
        
        mileage = vehicle.get('data-mileage')
        if mileage is None:
            logger.warning('%s %s mileage not found', vin, stock_number)
            mileage = '\\N'
        elif not mileage.isdigit():
            logger.warning('%s %s %s not a valid mileage', vin, stock_number, mileage)
            mileage = '\\N'

        ext_color = vehicle.get('data-ext-color')
        ext_color = simplify_color(vin, stock_number, ext_color)

        int_color = vehicle.get('data-int-color')
        int_color = get_int_color(vin, stock_number, int_color)

        transmission = vehicle.get('data-transmission')
        transmission = get_transmission(vin, stock_number, transmission)

        price = vehicle.find('span', attrs={'class':'price'})
        if price is None:
            logger.warning('%s %s price not found', vin, stock_number)
            price = '\\N'
        else:
            price = re.sub('[$,]', '', price.text.strip())
            if not price.isdigit():
                logger.warning('%s %s %s not a valid price', vin, stock_number, price)
                price = '\\N'

        vehicle_image = vehicle.find('div', attrs={'class':'vehicle-image'})
        fuel = equipment = '\\N'
        if vehicle_image is None:
            logger.warning('%s %s vehicle image not found', vin, stock_number)
        else:
            vehicle_image_a = vehicle_image.find('a')
            if vehicle_image_a is None:
                logger.warning('%s %s vehicle image a tag not found', vin, stock_number)
            else:
                details_link = vehicle_image_a.get('href')
                if details_link is None:
                    logger.warning('%s %s details link not found', vin, stock_number)
                else:
                    details_resp = requests.get(details_link)
                    if details_resp.status_code == requests.codes.ok:
                        details_soup = BeautifulSoup(details_resp.text, 'lxml')

                        basic_info = details_soup.find('div', attrs={'class':'basic-info-wrapper'})
                        if basic_info is None:
                            logger.warning('%s %s basic info not found in details page',
                                           vin, stock_number)
                        else:
                            info = basic_info.find_all('dl')
                            for i in info:
                                if i.dt.text == 'Fuel:':
                                    fuel = i.dd.text.strip()
                                    if re.search('gas', fuel, re.IGNORECASE):
                                        fuel = 'g'
                                    elif re.search('diesel', fuel, re.IGNORECASE):
                                        fuel = 'd'
                                    else:
                                        logger.warning('%s %s %s no matching fuel type', vin, stock_number, fuel)

                        features_container = details_soup.find('div', attrs={'id':'ctabox-premium-features'})
                        desc_container = details_soup.find('div', attrs={'class':'vehicle-description-text'})
                        if features_container is not None:
                            equipment = features_container.find('div').get_text('\n', strip=True)
                        elif desc_container is not None:
                            equipment = desc_container.p.get_text('\n', strip=True)
                        else:
                            logger.warning('%s %s equipment not found', vin, stock_number)
                    else:
                        logger.warning('%s %s failed to retrieve details page (status code %s)',
                                       vin, stock_number,
                                       details_resp.status_code)

        carproof = vehicle.find('a', attrs={'target':'lightbox'})['href']
        if carproof is None:
            logger.warning('%s %s carproof not found', vin, stock_number)
            carproof = '\\N'
            
        thumbnail = vehicle_image.find('img')['data-src']
        if thumbnail is None:
            logger.warning('%s %s thumbnail not found', vin, stock_number)
            thumbnail = '\\N'

        location = vehicle.find('div', attrs={'class':'price-block bottom-text'}).get_text('\n', strip=True)
        dealer = '\\N'
        if location is None:
            logger.warning('%s %s location not found', vin, stock_number)
        else:
            location_split = re.split('\n', location)
            if location_split and len(location_split) > 1:
                dealer = location_split[1]
            else:
                logger.warning('%s %s dealer not found', vin, stock_number)

        csvwriter.writerow([vin, stock_number, type, model_year, make,
                            model, trim, body_type, drive, displacement,
                            mileage, ext_color, int_color, transmission,
                            fuel, price, equipment, carproof, thumbnail,
                            dealer])


if __name__ == '__main__':
    conn = cursor = None
    
    try:
        with open('scrape_mb_config.json', 'r') as f:
            config = json.load(f)

        logging_config = config.get("logging")
        if logging_config is None:
            logging.basicConfig(filename='./log/scrape_mb.log',level=logging.INFO)
        else:
            logging.config.dictConfig(logging_config)
        logger = logging.getLogger(__name__)

        years = []
        if 'years' in config:
            years = config['years']
        else:
            raise Exception('years not found in scrape_mb_config.json')

        nonce = get_nonce()
        rjson = get_page(nonce, years)
        nVehicles = rjson['num_rows']
        nPages = rjson['page_count']
        pageSize = rjson['page_size']

        datafilename = './data/mb-{0}.csv'.format(
                datetime.today().strftime('%Y-%m-%d'))
        
        with open(datafilename, 'wb') as mbfile:
            mbwriter = csv.writer(mbfile)
            store(rjson['results'], mbwriter)
    
            for p in range(2, int(nPages) + 1):
                rjson = get_page(nonce, years, p)
                store(rjson['results'], mbwriter)

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

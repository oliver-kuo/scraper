# -*- coding: utf-8 -*-
"""
Created on Sun Aug 12 10:49:53 2018

@author: Oliver
"""

import requests
import re
from bs4 import BeautifulSoup
from mysql.connector import MySQLConnection, Error
from dbconfig import read_db_config
import urllib
import sys
import json


used_domain = 'https://www.brianjesselbmwpreowned.com'
demo_domain = 'https://www.brianjesselbmw.com'
start_year = '2015'
end_year = '2018'


def lower_i(matchobj):
    return matchobj.group(1) + 'i'


def scrub_e(matchobj):
    return matchobj.group(1)


def scrub_trim(model_trim):
    trim = model_trim
    if re.search('^M\d ', trim, re.IGNORECASE):
        trim = ''
    trim = re.sub('^((\d Series)|(x\d)|(z\d)|(i\d)) ?', '', trim, flags=re.IGNORECASE)
    trim = re.sub(' ((gran ((coupe)|(turismo)))|(suv)|(sedan)|(coupe)|(cabriolet)|(convertible)|(roadster)|(hatchback)|(touring)|(wagon)|-).*$', '', trim, flags=re.IGNORECASE)
    trim = re.sub('(\d{3}L?)I', lower_i, trim)
    trim = re.sub('(\d{3}e).*', scrub_e, trim, flags=re.IGNORECASE)
    trim = re.sub(' ((m sport)|(((luxury)|(modern)|(sport)) line))', '', trim, flags=re.IGNORECASE)
    trim = re.sub('^m$', '', trim, flags=re.IGNORECASE)
    
    if trim == '':
        trim = None

    return trim


def get_body_type(model_trim):
    body_type = None
    
    if re.search('x\d', model_trim, re.IGNORECASE):
        body_type = 'SUV'
    elif re.search('(sedan)|(gran coupe)', model_trim, re.IGNORECASE):
        body_type = 'Sedan'
    elif re.search('coupe', model_trim, re.IGNORECASE):
        body_type = 'Coupe'
    elif re.search('(cabriolet)|(roadster)|(convertible)', model_trim, re.IGNORECASE):
        body_type = 'Convertible'
    elif re.search('(gran turismo)|(hatchback)', model_trim, re.IGNORECASE):
        body_type = 'Hatchback'
    elif re.search('(touring)|(wagon)', model_trim, re.IGNORECASE):
        body_type = 'Wagon'
    elif body_type is None and re.search('(5\d\d(i|d|e))|(m5( |$))|(7\d\dl?(i|d|e))', model_trim, re.IGNORECASE):
        body_type = 'Sedan'
    elif body_type is None and re.search('i8', model_trim, re.IGNORECASE):
        body_type = 'Coupe'
        
    return body_type


def getPage(type = 'used', p = 0):
    print(p)
    
    if type == 'demo':
        url = demo_domain + '/en-CA/used/demonstrator/bmw/-/' + start_year + '-' + \
              end_year + '/' + str(p) + '?search=1'
    else:
        url = used_domain + '/en-CA/used/inventory/bmw/-/' + start_year + '-' + \
              end_year + '/' + str(p) + '?search=1'

    r = requests.get(url)
    return BeautifulSoup(r.text, 'lxml')


def store(soup, cursor, type = 'used'):
    pattern = re.compile('row product ( alternate)?')
    vehicles = soup.find_all('div', class_=pattern)
    
    for v in vehicles:
        vin = v['data-serial-number']
        stock_number = v.find('span', attrs={'class':'stock-number'}).text.split(' ')[-1]

        print(vin + ' ' + stock_number)
    
        title = v.find('div', attrs={'class':'vehicle'}).a
        i = 0
        model_year = model = trim = None
        make = 'BMW'
        for t in title.stripped_strings:
            if i == 0:
                ymm = t.split(' BMW ')
                model_year = ymm[0]
                model = re.sub('-', ' ', ymm[-1])
            elif i == 1:
                trim = t
            i += 1

        body_type = displacement = None
        if trim is None:
            body_type = get_body_type(model)
            trim = scrub_trim(model)
        else:
            body_type = get_body_type(model + ' ' + trim)
            trim = scrub_trim(model + ' ' + trim)
            
        if trim is None:
            displacement = disp_dict.get(str(model_year) + ' ' + model)
        else:            
            displacement = disp_dict.get(str(model_year) + ' ' + model + ' ' + trim)
        
        type = mileage = ext_color = int_color = transmission = fuel = None
        options = v.find('div', attrs={'class':'options'}).find_all('li')
        for o in options:
            t = o.text.split(' : ')
            if t[0] == 'Condition':
                if t[-1] == 'Used' or t[-1] == 'Demonstrator':
                    type = 'u'
                elif t[-1] == 'Certified':
                    type = 'c'
            elif t[0] == 'Mileage':
                mileage = t[-1]
            elif t[0] == 'Exterior color':
                ext_color = t[-1]
            elif t[0] == 'Interior color':
                int_color = t[-1]
            elif t[0] == 'Transmission':
                transmission = 'a' if t[-1] == 'Automatic' else 'm'
            elif t[0] == 'Fuel':
                if t[-1] == 'Gas':
                    fuel = 'g'
                elif t[-1] == 'Diesel':
                    fuel = 'd'
                elif t[-1] == 'Electric':
                    fuel = 'h'

        price = None
        if v['data-price'] != '':
            price = v['data-price']

        image_container = v.find('a', attrs={'class':'product-link'})

        if type == 'demo':
            details_link = demo_domain + image_container['href']
        else:
            details_link = used_domain + image_container['href']
            
        details_resp = requests.get(details_link)
        details_soup = BeautifulSoup(details_resp.text, 'lxml')
        tech_specs = details_soup.find('div', attrs={'class':'technical-specifications'})
        specs = tech_specs.find_all('li', class_='group')

        drive = None
        for s in specs:
            for li in s.find_all('li'):
                key = li.find('span', attrs={'class':'key'}).text
                value = li.find('span', attrs={'class':'value'}).text
    
                if key == 'Drivetrain':
                    if value == 'All wheel drive':
                        drive = 'AWD'
                    elif value == 'Rear wheel drive':
                        drive = 'RWD'
                    elif value == 'Front wheel drive':
                        drive = 'FWD'
    
        equipment = details_soup.find(
                        'div',
                        attrs={'class':'text comment-free-text'}).text.strip()
    
        carproof_params = {'ThirdPartyKey':'+x7he01Q/RaPDTQsi1ot0Q==',
                           'Vin':vin,
                           'Language':'en',
                           'Logo':'true',
                           'Width':'374'}
        carproof_resp = requests.get(
                'https://badging.carproof.com/api/Badging/GetBadgesJSONP?',
                params=carproof_params)
        match = re.search('(https://reports\.carproof\.com/main\?id=.+?)"',
                          urllib.parse.unquote(carproof_resp.text))
        carproof = None
        if match:
            carproof = match.group(1)
    
        thumbnail = image_container.img['src']
        dealer = 'Brian Jessel BMW'
        
        query = 'INSERT INTO car(vin, stock_number, type, model_year, make, ' \
                'model, trim, body_type, drive, displacement, mileage, ' \
                'ext_color, int_color, transmission, fuel, price, ' \
                'equipment, carproof, thumbnail, dealer) ' \
                'VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, ' \
                '%s, %s, %s, %s, %s, %s, %s)'
        args = (vin, stock_number, type, model_year, make, model, trim, \
                body_type, drive, displacement, mileage, ext_color, \
                int_color, transmission, fuel, price, equipment, carproof, \
                thumbnail, dealer)
        cursor.execute(query, args)
        

if __name__ == '__main__':
    with open('bmw_displacement.json', 'r') as f:
        disp_dict = json.load(f)

    soup = getPage(sys.argv[1], 0)
    legend = soup.find('legend').text.strip()
    match = re.search('(\d+) vehicle\(s\) found, (\d+) page\(s\)', legend)
    nVehicles = match.group(1)
    nPages = match.group(2)
    
    db_config = read_db_config()

    try:
        conn = MySQLConnection(**db_config)

        if conn.is_connected():
            cursor = conn.cursor()
            store(soup, cursor, sys.argv[1])

            for p in range(1, int(nPages)):
                soup = getPage(sys.argv[1], p)
                store(soup, cursor, sys.argv[1])
        else:
            print('connection failed')

        conn.commit()

    except Error as e:
        print(e)

    finally:
        cursor.close()
        conn.close()

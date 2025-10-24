########################################################################################################################
# Copyright (c) Martin Bustos @FronkonGames <fronkongames@gmail.com>
#
# Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
# documentation files (the "Software"), to deal in the Software without restriction, including without limitation the
# rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all copies or substantial portions of
# the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE
# WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
# COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
########################################################################################################################
__author__ = "Martin Bustos <fronkongames@gmail.com>"
__copyright__ = "Copyright 2022, Martin Bustos"
__license__ = "MIT"
__version__ = "1.2.2"
__email__ = "fronkongames@gmail.com"

import sys
import os
import re
from ssl import SSLError
import requests
import time
import random
import logging
from tqdm import tqdm
import boto3
import json
import io
from global_scripts.utils import ingest_data

import dotenv

dotenv.load_dotenv(dotenv.find_dotenv())

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - [%(levelname)s] - %(message)s',
    force=True  # override any existing config
)


# IDs for best 100 games
VALID_IDS = [ "1687950", "632470", "271590", "7670", "220", "70", "659", "620", "2310", "1245620", 
"34440", "8870", "12110", "72850", "900883", "3900", "373420", "22330", "24980", "2362420", 
"292030", "13230", "1778820", "12120", "4560", "1003590", "12100", "435150", "975370", "620980", 
"546560", "1593500", "1174180", "1145360", "228200", "402430", "915100", "2679460", "3314060", "440", 
"211600", "587620", "13240", "238210", "391540", "4760", "13570", "7940", "202200", "205100", 
"1147860", "35010", "32380", "1041210", "57400", "269710", "1989270", "200260", "224760", "813230", 
"236430", "22370", "17300", "287700", "22300", "1158310", "273620", "418530", "1250410", "47810", 
"21660", "13560", "35140", "17450", "6310", "2620", "238010", "6910", "3910", "225080", 
"50100", "22000", "1504500", "590380", "15210", "12210", "201270", "1284210", "10500", "1123450", 
"385760", "354400", "26800", "52003", "34330", "558990", "353540", "239350", "1163060", "8930"]


def SanitizeText(text):
  '''
  Removes HTML codes, escape codes and URLs.
  '''
  text = text.replace('\n\r', ' ')
  text = text.replace('\r\n', ' ')
  text = text.replace('\r \n', ' ')
  text = text.replace('\r', ' ')
  text = text.replace('\n', ' ')
  text = text.replace('\t', ' ')
  text = text.replace('&quot;', "'")
  text = re.sub(r'(https|http)?:\/\/(\w|\.|\/|\?|\=|\&|\%)*\b', '', text, flags=re.MULTILINE)
  text = re.sub('<[^<]+?>', ' ', text)
  text = re.sub(' +', ' ', text)
  text = text.lstrip(' ')

  return text

def PriceToFloat(price, decimals=2):
  '''
  Price in text to float. Use locate?
  '''
  price = price.replace(',', '.')

  return round(float(re.findall('([0-9]+[,.]+[0-9]+)', price)[0]), decimals)

def DoRequest(url, parameters=None, retryTime=5, successCount=0, errorCount=0, retries=0):
  '''
  Makes a Web request. If an error occurs, retry.
  '''
  response = None
  try:
    response = requests.get(url=url, params=parameters, timeout=float(os.getenv("DEFAULT_TIMEOUT")))
  except (requests.exceptions.HTTPError, requests.exceptions.ConnectionError,
          requests.exceptions.Timeout, requests.exceptions.RequestException,
          SSLError) as ex:
    logging.exception(f'An exception of type {type(ex).__name__} ocurred while fetching data for {url}.')
    response = None

  if response and response.status_code == 200:
    errorCount = 0
    successCount += 1
    if successCount > retryTime:
      retryTime = min(5, retryTime / 2)
      successCount = 0
  else:
    if retries == 0 or errorCount < retries:
      errorCount += 1
      successCount = 0
      retryTime = min(retryTime * 2, 500)
      if response is not None:
        logging.warning(f'{response.reason}, retrying in {retryTime} seconds')
      else:
        logging.warning(f'Request failed, retrying in {retryTime} seconds.')

      time.sleep(retryTime)

      return DoRequest(url, parameters, retryTime, successCount, errorCount, retries)
    else:
      logging.warning(f'Request failed {errorCount} times, no more retries.')
      sys.exit()

  return response

def SteamRequest(appID, retryTime, successRequestCount, errorRequestCount, retries, currency=os.getenv("DEFAULT_CURRENCY"), language=os.getenv("DEFAULT_LANGUAGE")):
  '''
  Request and parse information about a Steam app.
  '''
  url = "http://store.steampowered.com/api/appdetails/"
  response = DoRequest(url, {"appids": appID, "cc": currency, "l": language}, retryTime, successRequestCount, errorRequestCount, retries)
  if response.status_code == 200 and response.text.strip():
    try:
      data = response.json()
      app = data[appID]
      if app['success'] == False:
        return None
      elif app['data']['type'] != 'game':
        return None
      elif app['data']['is_free'] == False and 'price_overview' in app['data'] and app['data']['price_overview']['final_formatted'] == '':
        return None
      elif 'developers' in app['data'] and len(app['data']['developers']) == 0:
        return None
      else:
        return app['data']
    except Exception as ex:
      logging.exception(f'An exception of type {type(ex).__name__} ocurred.')
      logging.error(f"url: {url}")
      logging.error(f"Response text (first 200 chars): {response.text[:200]!r}")
      return None
  else:
    logging.error(f"Bad or empty response from {url}: {response.status_code}")
    return None

def SteamSpyRequest(appID, retryTime, successRequestCount, errorRequestCount, retries):
  '''
  Request and parse information about a Steam app using SteamSpy.
  '''
  url = f"https://steamspy.com/api.php?request=appdetails&appid={appID}"
  response = DoRequest(url, None, retryTime, successRequestCount, errorRequestCount, retries)
  if response:
    try:
      data = response.json()
      if data['developer'] != "":
        return data
      else:
        return None
    except Exception as ex:
      logging.exception(f'An exception of type {type(ex).__name__} ocurred.')
      return None
  else:
    logging.error('Bad response')
    return None

def ParseSteamGame(app):
  '''
  Parse game info.
  '''
  game = {}
  game['name'] = app['name'].strip()
  game['release_date'] = app['release_date']['date'] if 'release_date' in app and not app['release_date']['coming_soon'] else ''
  game['required_age'] = int(str(app['required_age']).replace('+', '')) if 'required_age' in app else 0

  if app['is_free'] or 'price_overview' not in app:
    game['price'] = 0.0
  else:
    game['price'] = PriceToFloat(app['price_overview']['final_formatted'])

  game['dlc_count'] = len(app['dlc']) if 'dlc' in app else 0
  game['detailed_description'] = app['detailed_description'].strip() if 'detailed_description' in app else ''
  game['about_the_game'] = app['about_the_game'].strip() if 'about_the_game' in app else ''
  game['short_description'] = app['short_description'].strip() if 'short_description' in app else ''
  game['reviews'] = app['reviews'].strip() if 'reviews' in app else ''
  game['header_image'] = app['header_image'].strip() if 'header_image' in app else ''
  game['website'] = app['website'].strip() if 'website' in app and app['website'] is not None else ''
  game['support_url'] = app['support_info']['url'].strip() if 'support_info' in app else ''
  game['support_email'] = app['support_info']['email'].strip() if 'support_info' in app else ''
  game['windows'] = True if app['platforms']['windows'] else False
  game['mac'] = True if app['platforms']['mac'] else False
  game['linux'] = True if app['platforms']['linux'] else False
  game['metacritic_score'] = int(app['metacritic']['score']) if 'metacritic' in app else 0
  game['metacritic_url'] = app['metacritic']['url'] if 'metacritic' in app else ''
  game['achievements'] = int(app['achievements']['total']) if 'achievements' in app else 0
  game['recommendations'] = app['recommendations']['total'] if 'recommendations' in app else 0
  game['notes'] = app['content_descriptors']['notes'] if 'content_descriptors' in app and app['content_descriptors']['notes'] is not None else ''

  game['supported_languages'] = []
  game['full_audio_languages'] = []

  if 'supported_languages' in app:
    languagesApp = app['supported_languages']
    languagesApp = re.sub('<[^<]+?>', '', languagesApp)
    languagesApp = languagesApp.replace('languages with full audio support', '')

    languages = languagesApp.split(', ')
    for lang in languages:
      if '*' in lang:
        game['full_audio_languages'].append(lang.replace('*', ''))
      game['supported_languages'].append(lang.replace('*', ''))

  game['packages'] = []
  if 'package_groups' in app:
    for package in app['package_groups']:
      subs = []
      if 'subs' in package:
        for sub in package['subs']:
          subs.append({'text': SanitizeText(sub['option_text']),
                       'description': sub['option_description'],
                       'price': round(float(sub['price_in_cents_with_discount']) * 0.01, 2) }) 

      game['packages'].append({'title': SanitizeText(package['title']), 'description': SanitizeText(package['description']), 'subs': subs})

  game['developers'] = []
  if 'developers' in app:
    for developer in app['developers']:
      game['developers'].append(developer.strip())

  game['publishers'] = []
  if 'publishers' in app:
    for publisher in app['publishers']:
      game['publishers'].append(publisher.strip())

  game['categories'] = []
  if 'categories' in app:
    for category in app['categories']:
      game['categories'].append(category['description'])

  game['genres'] = []
  if 'genres' in app:
    for genre in app['genres']:
      game['genres'].append(genre['description'])

  game['screenshots'] = []
  if 'screenshots' in app:
    for screenshot in app['screenshots']:
      game['screenshots'].append(screenshot['path_full'])

  game['movies'] = []
  if 'movies' in app:
    for movie in app['movies']:
      game['movies'].append(movie['mp4']['max'])

  game['detailed_description'] = SanitizeText(game['detailed_description'])
  game['about_the_game'] = SanitizeText(game['about_the_game'])
  game['short_description'] = SanitizeText(game['short_description'])
  game['reviews'] = SanitizeText(game['reviews'])
  game['notes'] = SanitizeText(game['notes'])

  return game

def UploadJSON(s3_client, data, filename, backup = False):
  try:
    name, ext = os.path.splitext(filename)
    target_name = f'{name}.bak' if backup else filename
    target_name = os.getenv("TEMPORAL_SUB_BUCKET") + '/' + target_name
    fileobj = io.BytesIO(json.dumps(data, indent=4, ensure_ascii=False).encode("utf-8"))
    fileobj.name = target_name  # In case of errors, for logging purposes
    ingest_data(s3_client, os.getenv("LANDING_ZONE_BUCKET"), fileobj, target_name)

  except Exception as ex:
    logging.exception(f'Error uploading {filename}.')
    sys.exit()


def Scraper(s3_client, steam_dataset, steamspy_dataset, appIDs = VALID_IDS):
  '''
  Search games in Steam.
  '''
  
  apps = appIDs

  if apps:
    gamesAdded = 0
    gamesDiscarted = 0
    successRequestCount = 0
    errorRequestCount = 0

    random.shuffle(apps)
    count = 0

    for appID in tqdm(apps):
      if count == 3:
        break
      app = SteamRequest(appID, min(4, float(os.getenv("DEFAULT_SLEEP"))), successRequestCount, errorRequestCount, int(os.getenv("DEFAULT_RETRIES")))
      if app:
        steam_game = ParseSteamGame(app)
        steamspy_game = {}
        extra = SteamSpyRequest(appID, min(4, float(os.getenv("DEFAULT_SLEEP"))), successRequestCount, errorRequestCount, int(os.getenv("DEFAULT_RETRIES")))
        if extra != None:
          steamspy_game['user_score'] = extra['userscore']
          steamspy_game['score_rank'] = extra['score_rank']
          steamspy_game['positive'] = extra['positive']
          steamspy_game['negative'] = extra['negative']
          steamspy_game['estimated_owners'] = extra['owners'].replace(',', '').replace('..', '-')
          steamspy_game['average_playtime_forever'] = extra['average_forever']
          steamspy_game['average_playtime_2weeks'] = extra['average_2weeks']
          steamspy_game['median_playtime_forever'] = extra['median_forever']
          steamspy_game['median_playtime_2weeks'] = extra['median_2weeks']
          steamspy_game['discount'] = extra['discount']
          steamspy_game['peak_ccu'] = extra['ccu']
          steamspy_game['tags'] = extra['tags']
        else:
          steamspy_game['user_score'] = 0
          steamspy_game['score_rank'] = ""
          steamspy_game['positive'] = 0
          steamspy_game['negative'] = 0
          steamspy_game['estimated_owners'] = "0 - 0"
          steamspy_game['average_playtime_forever'] = 0
          steamspy_game['average_playtime_2weeks'] = 0
          steamspy_game['median_playtime_forever'] = 0
          steamspy_game['median_playtime_2weeks'] = 0
          steamspy_game['discount'] = 0
          steamspy_game['peak_ccu'] = 0
          steamspy_game['tags'] = []

        steam_dataset[appID] = steam_game
        steamspy_dataset[appID] = steamspy_game
        logging.info(f'AppID {appID} added: {steam_game["name"]}')
        gamesAdded += 1

        if int(os.getenv("DEFAULT_AUTOSAVE")) > 0 and gamesAdded > 0 and gamesAdded % int(os.getenv("DEFAULT_AUTOSAVE")) == 0:
          UploadJSON(s3_client, steam_dataset, os.getenv("DEFAULT_STEAM_OUTFILE"), True)
          UploadJSON(s3_client, steamspy_dataset, os.getenv("DEFAULT_STEAMSPY_OUTFILE"), True)

      else:
        gamesDiscarted += 1
        logging.warning(f'AppID {appID} discarted.')

      time.sleep(float(os.getenv("DEFAULT_SLEEP")) if random.random() > 0.1 else float(os.getenv("DEFAULT_SLEEP")) * 2.0)
      count += 1

    logging.info(f'Scrape completed: {gamesAdded} new games added, {gamesDiscarted} discarted')
    UploadJSON(s3_client, steam_dataset, os.getenv("DEFAULT_STEAM_OUTFILE"))
    UploadJSON(s3_client, steamspy_dataset, os.getenv("DEFAULT_STEAMSPY_OUTFILE"))
  else:
    logging.error('Error requesting list of games')
    sys.exit()

def main():
  '''
  Main function.
  '''
  try:
    s3_client = boto3.client(
        "s3",
        endpoint_url=os.getenv("ENDPOINT_URL"),
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    )
    logging.info("Connected to MinIO.")

    # Preparation for incremental data ingestion
    steam_dataset = {}
    steamspy_dataset = {}

    try:
      Scraper(s3_client, steam_dataset, steamspy_dataset, appIDs=VALID_IDS)
    
    except (KeyboardInterrupt, SystemExit):
      UploadJSON(s3_client, steam_dataset, os.getenv("DEFAULT_STEAM_OUTFILE"), int(os.getenv("DEFAULT_AUTOSAVE")) > 0)
      UploadJSON(s3_client, steamspy_dataset, os.getenv("DEFAULT_STEAMSPY_OUTFILE"), int(os.getenv("DEFAULT_AUTOSAVE")) > 0)
    except Exception:
      logging.exception(f"Error during data ingestion.")
      return
    
    logging.info('Done')
    
  except Exception:
      logging.exception(f"Error connecting to MinIO.")
      return

if __name__ == "__main__":
  main()

  
  

from requests import get
from requests.exceptions import RequestException
from contextlib import closing
from bs4 import BeautifulSoup
import datetime as dt
import logging
from zipfile import ZipFile
from multiprocessing.dummy import Pool as ThreadPool 
# import pandas as pd
import re
from typing import Dict
import time

MAIN_PAGE_URL = "https://www.finder.com/credit-cards/complete-list-of-credit-card-companies"
OUTPUT_FOLDER = "markdown"
IMAGE_FOLDER = "images"
COULD_NOT_PARSE_FILE = '/'.join([OUTPUT_FOLDER, "could-not-parse.txt"])
DISCONTINUED_REGEX = re.compile("Discontinued", re.IGNORECASE)

def simple_get(url):
    """
    Attempts to get the content at url by making an HTTP GET request.
    If the content-type of response is some kind of HTML/XML, return the
    text content, otherwise return None.
    """
    try:
        with closing(get(url, stream=True)) as resp:
            if is_good_response(resp):
                return resp.content
            else:
                return None

    except RequestException as e:
        log_error('Error during requests to {0} : {1}'.format(url, str(e)))
        return None


def is_good_response(resp):
    """
    Returns True if the response seems to be HTML, False otherwise.
    """
    content_type = resp.headers['Content-Type'].lower()
    return (resp.status_code == 200 
            and content_type is not None 
            and content_type.find('html') > -1)


def log_error(e):
    """
    It is always a good idea to log errors. 
    This function just prints them, but you can
    make it do anything.
    """
    logging.info(e)


def get_time():
    return dt.datetime.fromtimestamp(time.time())


def elapsed_time_seconds(start_time: dt.datetime, end_time: dt.datetime):
    return (end_time - start_time).total_seconds()


def get_soup(url):
    start_time = get_time()
    logging.info(f"getting html at: {start_time}")
    output = simple_get(url).decode(encoding="UTF-8")
    end_time = get_time()
    logging.info(f"got html at : {end_time}. Elapsed time: {elapsed_time_seconds(start_time, end_time)} seconds. Now converting to soup.")
    soup = BeautifulSoup(output, "lxml")
    soup_end_time = get_time()
    logging.info(f"converted to soup at {soup_end_time}. Elapsed time: {elapsed_time_seconds(end_time, soup_end_time)} seconds")
    return soup


def az_listing_item_text_to_href(soup: BeautifulSoup) -> Dict[str, str]: 
  text_to_href = {}

  for list_item in soup.findAll("li", {"class": "az-listing__item"}):
    list_item = list_item.find("a", href=True)
    if list_item.getText().strip() != "":
      text_to_href[list_item.getText().strip()] = list_item["href"].strip()
  
  return text_to_href


def card_url_to_bank(bank, bank_url):
  url_to_bank = {}
  banksoup = get_soup(bank_url)
  for url in az_listing_item_text_to_href(banksoup).values():
    url_to_bank[url] = bank
  return url_to_bank


def is_discontinued_card(soup):
  masthead = soup.find("div", {"class": "creditCard__desktopInfo mastheadGrid__main"})
  if not masthead:
    return False
  h2_text = masthead.find("h2").getText() if masthead.find("h2") else ""
  h4_text = masthead.find("h4").getText() if masthead.find("h4") else ""
  if DISCONTINUED_REGEX.search(h2_text) or DISCONTINUED_REGEX.search(h4_text):
    return True
  else:
    return False


def image_link(soup):
  img = soup.find("img", {"class": "productImage"})
  if img:
    return img.get("src")
  else:
    return "No image found"


def download_image(url, file_name):
  img_data = get(url).content
  with open('/'.join([IMAGE_FOLDER, file_name]), 'wb') as handler:
    handler.write(img_data)


def card_page_info(url, bank):
  cardsoup = get_soup(url)

  start_time = get_time()
  logging.info(f"Parsing page soup at: {start_time}")

  info_tabs = cardsoup.findAll("div", {"class": "luna-tabpanel"})
  card_info = {}
  card_info["Bank"] = bank
  card_info["Url"] = url
  card_info["Discontinued"] = is_discontinued_card(cardsoup)
  card_info["Image"] =  image_link(cardsoup)
  for tab in info_tabs:
    for row in tab.findAll("tr"):
      key = row.find("th").getText().strip()
      if row.find("a", href=True):
        value = row.find("a").get("href")
      else:
        value = row.find("td").getText().strip()
      card_info[key] = value

  end_time = get_time()
  logging.info(f"Finished parsing soup at: {end_time}. Elapsed time: {elapsed_time_seconds(start_time, end_time)} seconds")
  return card_info


def log_cannot_parse_file(info, error):
  with open(COULD_NOT_PARSE_FILE, "a+") as file:
    file.write(f"{str(get_time())} -- could not parse for bank {info['Bank']}, {info['Url']}\n")
    file.write(f"Discontinued: {info['Discontinued']}\n")
    file.write(str(error) + "\n")


def main():
  main_page_soup = get_soup(MAIN_PAGE_URL)
  bank_to_url = az_listing_item_text_to_href(main_page_soup)

  with ThreadPool(16) as pool:
    url_to_bank = pool.map(lambda x: card_url_to_bank(x[0], x[1]), bank_to_url.items())

  result = url_to_bank[0]
  for r in url_to_bank:
    result.update(r)

  url_to_bank = result

  with ThreadPool(16) as pool:
    card_info = pool.map(lambda x: card_page_info(x[0], x[1]), url_to_bank.items())

  # clear out cannot parse file
  open(COULD_NOT_PARSE_FILE, 'w').close()

  # write files to markdown
  for info in card_info:
    try:
      product_name = re.sub("[^\s0-9a-zA-Z]+", "", info["Product Name"])
      product_name = re.sub("[\s]+", "-", product_name)
      product_name = product_name.lower()
      file_name = f"{product_name}.md"
      with open('/'.join([OUTPUT_FOLDER, file_name]), "w") as file:
        file.write("---\n")
        file.write(f"Card-Name: \"{product_name}\"\n")
        for key, value in info.items():
          key = re.sub("[\s]+", "-", key)
          file.write(f"{key}: \"{value}\"\n")
        file.write("---\n")
      download_image(url=info["Image"], file_name=product_image + info["Image"].split(".")[-1])

    except Exception as e:
      log_cannot_parse_file(info, e)
      

if _name_ == "_main_":
  main()
# import shutil
# shutil.rmtree("markdown")

# DISCONTINUED_REGEX.match("a")

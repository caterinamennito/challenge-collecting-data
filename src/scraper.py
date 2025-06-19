import requests 
from typing import Optional
from bs4 import BeautifulSoup
import logging
from selenium import webdriver
from selenium.webdriver.common.by import By
import pandas as pd
import time
from random import randint
import undetected_chromedriver as uc
from playwright.sync_api import sync_playwright
import re
from requests_html import HTMLSession
from fake_headers import Headers
import random
import cfscrape
import json
from concurrent.futures import ThreadPoolExecutor, as_completed


class Scraper:
    staticmethod
    def get_url(self, page_nr):
        return f"https://www.immoweb.be/fr/recherche/maison-et-appartement/a-vendre?countries=BE&page={page_nr}"
    
    def __init__(self):
        logging.basicConfig(level=logging.INFO)
        self.session: Optional[requests.Session] = None

    def __enter__(self) -> "Scraper":
        self.session = requests.Session()
        logging.info('Session created')
        return self


    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            logging.error(f"Exception: {exc_type}, {exc_val}, {exc_tb}")
        if self.session:
            self.session.close()
            logging.info('Session closed')
    
    def fetch_immo_list(self):

        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
        }

        # update range as needed
        for i in range(300):
            print(f"Fetching page {i + 1}...")
            url = self.get_url(i + 1)
            print(f"Requesting URL: {url}")
            
            try:
                response = self.session.get(url, headers=headers, timeout=15)
                print(f"Status code: {response.status_code}")
                
                if "Please enable JS" in response.text:
                    print("Blocked: Cloudflare challenge detected.")
                else:
                    print("Success! ")
                    html = response.text
                    soup = BeautifulSoup(html, 'html.parser')
                    # find all anchor tags with href attributes and class="card__title-link"
                    links_elements = soup.find_all('a', class_='card__title-link')
                    links = set()
                    for link in links_elements:
                        href = link.get('href')
                        if href:
                            print(f"Found link: {href}")
                            links.add(href)
                    print(f"Found {len(links)} links.")
                    pandas_df = pd.DataFrame(links, columns=['url'])
                    # append links to the existing CSV file
                    try:
                        existing_df = pd.read_csv('links.csv')
                        print(existing_df)
                        pandas_df = pd.concat([existing_df, pandas_df])
                        print(pandas_df)
                    except FileNotFoundError:
                        print("links.csv not found, creating a new one")
                    finally:    
                        print(f"Saving links to links.csv")
                        pandas_df.to_csv('links.csv', index=False)
                    # Delay before next request (randomized)
                    # wait = randint(5, 10)
                    # print(f"Waiting {wait} seconds before next page...\n")
                    # time.sleep(wait)

            except Exception as e:
                print(f"Request failed: {e}")
    
# Using selenium
    def fetch_details(self):
        # read from links.csv
        try:
            df = pd.read_csv('links.csv')
            urls = df['url'].tolist()
        except FileNotFoundError:
            print("links.csv not found, please run fetch_list first")
            return
        for url in urls[:1]:
            try:
                print(f"Fetching details for {url}")
                driver = webdriver.Firefox()
                driver.get(url)
                print(f"Page title: {driver.title}")
                assert "Immoweb" in driver.title
                locality = driver.find_elements(By.CLASS_NAME, 'classified__information--address-row')
                overview = driver.find_elements(By.CLASS_NAME, 'overview__text')
                price = driver.find_elements(By.CLASS_NAME, 'classified__price')
                data = driver.find_elements(By.CLASS_NAME, 'classified-table__data')

                if locality:
                    # add a column locality to the df 
                    print(f"Found locality: {locality[1].text}")
                    print(f"Found room number: {overview[0].text}")
                    print(f"Found number bathrooms: {overview[1].text}")
                    print(f"Found sq_mt: {overview[2].text}")
                    print(f"Found price: {price[0].text}")
                    for item in data:
                        print(f"Found data: {item.text}")
                else:
                    print("Address not found in elements,")
                type = driver.find_elements(By.CLASS_NAME, 'classified__title')
                if type:
                    print(f"Found type: {type[0].text}")
                    # find the word next to "à vendre" in the type text
                    if "à vendre" in type[0].text:
                        property_type = type[0].text.split("à vendre")[0].strip()
                        print(f"Property type: {property_type}")
                    else:
                        print("Property type not found in title")
                else:
                    print("Type not found in elements,")
        
                
            except Exception as e:
                print(f"Error fetching details for {url}: {e}")

    def get_headers(self) -> dict:
        """
        Generate randomized, realistic HTTP headers to reduce request blocking.
        
        The function selects random combinations of browser and OS
        to generate diverse and legitimate-looking headers.
        """
        browsers = ["chrome", "firefox", "opera", "safari", "edge"]
        os_choices = ["win", "mac", "linux"]

        headers = Headers(
            browser=random.choice(browsers),
            os=random.choice(os_choices),
            headers=True
        )
        return headers.generate()
    
    def fetch_details_soup(self):
        existing_df = pd.read_csv('data.csv')

        try:
            df = pd.read_csv('links.csv')
            urls = df['url'].tolist()
        except FileNotFoundError:
            print("links.csv not found, please run fetch_list first")
            return
        # Update index!
        for url in urls[14835:]:
            processed_urls = set(existing_df['url'].dropna().unique())

            if url in processed_urls:
                print(f"⏩ Skipping already-processed URL: {url}")
                continue
            if "projet-neuf" in url:
                print(f"Skipping {url} as it is a new project listing. (Data is weird on new projects)")
                continue
            try:
                print(f"Fetching details for {url}")
                scraper = cfscrape.create_scraper()  # returns a CloudflareScraper instance
                content = scraper.get(url).content
                soup = BeautifulSoup(content, 'html.parser')
                p = soup.find('p')
                if p and "Please enable JS and disable any ad blocker" in p.text:
                    print("⚠️ JS blocker detected.")
                    print('\a') #beep
                    return
                
                scripts = soup.find_all("script")
                for script in scripts:
                    script_content = script.string or script.text
                    # Regex pattern to extract av_items array
                    pattern = r'av_items\s*=\s*(\[\s*{[\s\S]*?}\s*])\s*;?'
                    match = re.search(pattern, script_content)
                    if match:
                        av_items_raw = match.group(1)

                        # Remove JS-style comments
                        cleaned = re.sub(r'//.*', '', av_items_raw)

                        # Replace unquoted variable references with string placeholder
                        cleaned = re.sub(r'("list_name"\s*:\s*)([A-Za-z_][A-Za-z0-9_]*)(\s*,?)',
                                        r'\1"REPLACED_CONTEXT"\3',
                                        cleaned)

                        # Remove trailing commas
                        cleaned = re.sub(r',(\s*[}\]])', r'\1', cleaned)

                        try:
                            av_items = json.loads(cleaned)[0]
                            selected_keys = ["id", "price", "nb_bedrooms", "nb_rooms","indoor_surface", "zip_code","subtype", "currency", "building_state", "year_of_construction", "energy_certificate", "outdoor_terrace_exists", "energy", "kitchen_type", "land_surface", "outdoor_surface", "country", "province", "city", "parking"]
                            filtered_items = {**{key: av_items.get(key) for key in selected_keys}, "url":url}
                            print(f"Filtered items: {filtered_items}")
                            try:
                                existing_df = pd.read_csv('data.csv')
                                df_result = pd.concat([existing_df, pd.DataFrame([filtered_items])], ignore_index=True)
                            except FileNotFoundError:
                                df_result = pd.DataFrame([filtered_items])
                            finally:
                                df_result.to_csv('data.csv', index=False)
                                print(f"✅ Saved data for: {url}")
                        except json.JSONDecodeError as e:
                            print("❌ Invalid JSON. Error:", e)
                            print("Cleaned string:\n", cleaned)

            except Exception as e:
                print(f"Error fetching details for {url}: {e}")

# Didn't work. Got immediately blocked when using multithreading
    def fetch_details_soup_multithread(self):
        # read from links.csv
        try:
            df = pd.read_csv('links.csv')
            urls = df['url'].tolist()
        except FileNotFoundError:
            print("links.csv not found, please run fetch_list first")
            return
        # Keys to extract
        selected_keys = ["id", "price", "nb_bedrooms", "nb_rooms", "indoor_surface", "zip_code",
                        "subtype", "currency", "building_state", "year_of_construction",
                        "energy_certificate", "outdoor_terrace_exists", "energy", "kitchen_type",
                        "land_surface", "outdoor_surface", "country", "province", "city", "parking"]

        existing_df = pd.read_csv('data.csv')

        def fetch_and_process(url):
            processed_urls = set(existing_df['url'].dropna().unique())

            if url in processed_urls:
                print(f"⏩ Skipping already-processed URL: {url}")
                return
            if "projet-neuf" in url:
                return

            print(f"Fetching: {url}")
            try:
                scraper = cfscrape.create_scraper()
                content = scraper.get(url, timeout=15).content
                # content = self.session.get(url, headers=self.get_headers()).text
                soup = BeautifulSoup(content, 'html.parser')

                # Check for fallback page
                p = soup.find('p')
                if p and "Please enable JS and disable any ad blocker" in p.text:
                    print(f"⚠️ JS block detected on {url}")
                    return None

                scripts = soup.find_all("script")

                for script in scripts:
                    script_content = script.string or script.text
                    pattern = r'av_items\s*=\s*(\[\s*{[\s\S]*?}\s*])\s*;?'
                    match = re.search(pattern, script_content)
                    if match:
                        av_items_raw = match.group(1)

                        # Sanitize raw JS object
                        cleaned = re.sub(r'//.*', '', av_items_raw)
                        cleaned = re.sub(r'("list_name"\s*:\s*)([A-Za-z_][A-Za-z0-9_]*)(\s*,?)',
                                        r'\1"REPLACED_CONTEXT"\3', cleaned)
                        cleaned = re.sub(r',(\s*[}\]])', r'\1', cleaned)

                        try:
                            av_items = json.loads(cleaned)[0]
                            filtered_items = {**{key: av_items.get(key) for key in selected_keys}, "url": url}
                            return filtered_items
                        except json.JSONDecodeError as e:
                            print(f"❌ JSON decode error on {url}: {e}")
                            return None
            except Exception as e:
                print(f"❌ Error on {url}: {e}")
                return None


        # --- Run multithreaded scraping ---
        results = []

        with ThreadPoolExecutor(max_workers=10) as executor:
            future_to_url = {executor.submit(fetch_and_process, url): url for url in urls[9760:9790]}

            for future in as_completed(future_to_url):
                result = future.result()
                if result:
                    results.append(result)
                    print(f"✅ Saved data for: {result['url']}")


        # --- Save results to CSV ---
        if results:
            try:
                df_result = pd.concat([existing_df, pd.DataFrame(results)], ignore_index=True)
            except FileNotFoundError:
                df_result = pd.DataFrame(results)

            df_result.to_csv('data.csv', index=False)
            print("✅ All results saved to data.csv")
        else:
            print("⚠️ No results were collected.")

    def fetch_details_playwright(self):
    # read from links.csv
        try:
            df = pd.read_csv('links.csv')
            urls = df['url'].tolist()
        except FileNotFoundError:
            print("links.csv not found, please run fetch_list first")
            return
        for url in urls[1301:1303]:
            if "projet-neuf" in url:
                print(f"Skipping {url} as it is a new project listing.")
                continue
            try:
                print(f"Fetching details for {url}")

                with sync_playwright() as p:
                    chrome_path = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"

                    browser = p.chromium.launch_persistent_context(
                        user_data_dir="/tmp/playwright",
                        headless=False,
                        executable_path=chrome_path,
                        args=[
                            "--disable-blink-features=AutomationControlled"
                        ],
                        viewport={"width": 1280, "height": 800},
                    )

                    page = browser.pages[0] if browser.pages else browser.new_page()
                    page.goto(url)

                    try:
                        page.wait_for_selector('.classified__information--address-row', timeout=5000)
                    except:
                        print("⚠️ Element not found, page may still be simplified.")

                    all_locality = page.query_selector_all('.classified__information--address-row')
                    locality_string = all_locality[1].inner_text().strip()
                    locality_parts = locality_string.split(' — ')
                    # locality_code, locality  = locality_string.split('-')
                    print(f"Found locality. locality_code: {locality_parts[0]}, locality: {locality_parts[1]}")
                    overview = page.query_selector_all('.overview__text')
                    price_string = page.query_selector_all('.sr-only')
                    price = re.sub(r'\D', '', price_string[0].inner_text().strip()) if price_string else None

                    room_nr_string = overview[0].inner_text().strip()
                    room_nr = re.search(r'\d+', room_nr_string).group() if room_nr_string else None
                    bathroom_string = overview[1].inner_text().strip()
                    bathroom_nr = re.search(r'\d+', bathroom_string).group() if bathroom_string else None
                    sq_mt_string = overview[2].inner_text().strip()
                    sq_mt = re.search(r'\d+', sq_mt_string).group() if sq_mt_string else None
                    data = page.query_selector_all('.classified-table__data')
                    property_type_el = page.query_selector_all('.classified__title')
                    facade_locator = page.locator('//tr[th[contains(@class, "classified-table__header") and normalize-space(text()) = "Nombre de façades"]]/td')
                    facade = facade_locator.first.inner_text().strip() if facade_locator.count() > 0 else None
                    print(f"Found facade: {facade}")
                    condition_locator = page.locator('//tr[th[normalize-space(text()) = "État du bâtiment"]]/td')
                    condition = condition_locator.first.inner_text().strip() if condition_locator.count() > 0 else None
                    terrace_locator = page.locator('//tr[th[normalize-space(text()) = "Surface de la terrasse"]]/td')
                    terrace_string = terrace_locator.first if terrace_locator.count() > 0 else None
                    terrace = re.search(r'\d+', terrace_string.inner_text().strip()).group() if terrace_string else None
                    kitchen_type_locator = page.locator('//tr[th[normalize-space(text()) = "Type de cuisine"]]/td')
                    kitchen_type = kitchen_type_locator.first.inner_text().strip() if kitchen_type_locator.count() > 0 else None
                    if kitchen_type is None:
                        equipped_kitchen = False
                        print("Kitchen type not found, setting to 'Non équipée'")
                    else:
                        print(f"Found kitchen type: {kitchen_type}")
                        equipped_kitchen = True if "équipée" in kitchen_type else False
                    swimming_pool_locator = page.locator('//tr[th[normalize-space(text()) = "Piscine"]]/td')
                    swimming_pool = True if swimming_pool_locator.count() > 0 else None
                 
                    energy_locator = page.locator('//tr[th[normalize-space(text()) = "Classe énergétique"]]/td')
                    energy_class = energy_locator.first.inner_text().strip() if energy_locator.count() > 0 else None
                    surface_area_locator = page.locator('//tr[th[normalize-space(text()) = "Surface du terrain"]]/td')
                    surface_area_string = surface_area_locator.first.inner_text().strip() if surface_area_locator.count() > 0 else None
                    surface_area = re.search(r'\d+', surface_area_string).group() if surface_area_string else None
                    garder_locator = page.locator('//tr[th[normalize-space(text()) = "Surface du jardin"]]/td')
                    garden_string = garder_locator.first.inner_text().strip() if garder_locator.count() > 0 else None
                    garden = re.search(r'\d+', garden_string).group() if garden_string else None
                    fireplace_locator = page.locator('//tr[th[normalize-space(text()) = "Feu ouvert"]]/td')
                    fireplace = fireplace_locator.first.inner_text().strip() if fireplace_locator.count() > 0 else None
                    print(f"Found fireplace: {fireplace if fireplace else 'Not found'}")
                    print(f"Found surface area: {surface_area if surface_area else 'Not found'}")
                    print(f"Found energy class: {energy_class if energy_class else 'Not found'}")
                    print(f"Found garden: {garden if garden else 'Not found'}")
                    print(f"Found swimming pool: {swimming_pool}")
                    print(f"Found equipped kitchen: {equipped_kitchen}")
                    print(f"Found condition: {condition}")
                    print("Found terrace: ", terrace if terrace else "Not found")
                    print(f"Found room number: {room_nr}")
                    print(f"Found number bathrooms: {bathroom_nr}")
                    print(f"Found sq_mt: {sq_mt}")
                    print(f"Found price: {price}. Price string: {price_string[0].inner_text().strip()}")

                    # create a dataframe with the data printed above, including the url
                    data_dict = {
                        "url": url,
                        "locality_code": locality_parts[0],
                        "locality": locality_parts[1],
                        "room_nr": room_nr,
                        "bathroom_nr": bathroom_nr,
                        "sq_mt": sq_mt,
                        "price": price,
                        "facade": facade,
                        "condition": condition,
                        "terrace": terrace,
                        "equipped_kitchen": equipped_kitchen,
                        "swimming_pool": swimming_pool,
                        "energy_class": energy_class,
                        "surface_area": surface_area,
                        "garden": garden,
                        "fireplace": fireplace
                    }
                    df = pd.DataFrame([data_dict])
                    # append the dataframe to a csv file
                    try:
                        existing_df = pd.read_csv('details.csv')
                        df = pd.concat([existing_df, df], ignore_index=True)
                    except FileNotFoundError:
                        print("details.csv not found, creating a new one")
                    finally:
                        print(f"Saving details to details.csv")
                        df.to_csv('details.csv', index=False)

                    if property_type_el:
                        type_text = property_type_el[0].inner_text().strip()
                        if "à vendre" in type_text:
                            property_type = type_text.split("à vendre")[0].strip()
                            print(f"Property type: {property_type}")

                    browser.close()
                wait = randint(1, 5)
                print(f"Waiting {wait} seconds before next page...\n")
                time.sleep(wait)  # Sleep to avoid being blocked by the website
                
            except Exception as e:
                print(f"Error fetching details for {url}: {e}")
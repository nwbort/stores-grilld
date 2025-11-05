# FILE: scrape.py
import requests
import json
import time
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from concurrent.futures import ThreadPoolExecutor, as_completed

BASE_URL = "https://grilld.com.au"
RESTAURANTS_LIST_URL = urljoin(BASE_URL, "/restaurants")
OUTPUT_FILE = "stores.json"
MAX_WORKERS = 10 

def get_store_urls():
    """
    Fetches the main restaurants page and extracts all individual store URLs.
    """
    print(f"Fetching store list from {RESTAURANTS_LIST_URL}...")
    try:
        response = requests.get(RESTAURANTS_LIST_URL)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching store list: {e}")
        return []

    soup = BeautifulSoup(response.content, "lxml")
    
    urls = set()
    for link in soup.select(".c-body-rich-text a[href]"):
        href = link.get('href')
        if href and 'restaurants/' in href:
            full_url = urljoin(BASE_URL, href)
            urls.add(full_url)
                
    if not urls:
        print("Warning: No store URLs found. The page structure might have changed.")

    print(f"Found {len(urls)} unique store URLs.")
    return sorted(list(urls))

def scrape_store_page(url):
    """
    Fetches a store page and extracts data from the embedded __NUXT_DATA__ JSON blob.
    This function is designed to be run in a separate thread.
    """
    try:
        response = requests.get(url, timeout=20)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"  -> Error fetching {url}: {e}")
        return None

    soup = BeautifulSoup(response.content, "lxml")
    
    nuxt_data_script = soup.find('script', id='__NUXT_DATA__', type='application/json')
    if not nuxt_data_script:
        print(f"  -> Error: __NUXT_DATA__ script tag not found on {url}")
        return None

    try:
        nuxt_data = json.loads(nuxt_data_script.string)
    except json.JSONDecodeError:
        print(f"  -> Error: Failed to parse JSON from __NUXT_DATA__ on {url}")
        return None

    store_info = None
    try:
        if len(nuxt_data) > 1 and isinstance(nuxt_data[1], dict):
             store_info = nuxt_data[1].get("state", {}).get("restaurant", {}).get("restaurant")
    except (IndexError, KeyError, AttributeError) as e:
        print(f"  -> Error navigating JSON structure on {url}: {e}")
        return None
            
    if not store_info or not isinstance(store_info, dict):
        print(f"  -> Error: Could not find valid store data object in __NUXT_DATA__ on {url}")
        return None

    description_copy = (store_info.get('description') or {}).get('copy')
    services = [service.get('name') for service in store_info.get('services', []) if service.get('name')]
        
    data = {
        'name': store_info.get('name'),
        'address': store_info.get('address'),
        'phone': store_info.get('phoneNumber'),
        'description': description_copy,
        'services': services,
        'opening_hours': store_info.get('tradingHours', []),
        'latitude': store_info.get('latitude'),
        'longitude': store_info.get('longitude'),
        'url': url,
    }
    return data

def main():
    """
    Main function to orchestrate the scraping process, using a thread pool for parallelism.
    """
    start_time = time.time()
    
    store_urls = get_store_urls()
    if not store_urls:
        print("Aborting due to no URLs being found.")
        return

    all_stores_data = []
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_url = {executor.submit(scrape_store_page, url): url for url in store_urls}
        
        total_urls = len(store_urls)
        completed_count = 0
        print(f"\nScraping {total_urls} store pages with up to {MAX_WORKERS} parallel workers...")

        for future in as_completed(future_to_url):
            completed_count += 1
            store_data = future.result()
            if store_data:
                all_stores_data.append(store_data)
                print(f"Progress: {completed_count}/{total_urls} | Extracted: {store_data.get('name')}")
            else:
                url = future_to_url[future]
                print(f"Progress: {completed_count}/{total_urls} | Failed to extract data for {url}")


    print("\n" + "="*30)

    all_stores_data.sort(key=lambda x: x.get('name') or '')

    end_time = time.time()
    duration = end_time - start_time
    
    print(f"Successfully scraped data for {len(all_stores_data)} out of {len(store_urls)} stores.")
    print(f"Total execution time: {duration:.2f} seconds.")
    
    try:
        with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
            json.dump(all_stores_data, f, indent=2, ensure_ascii=False)
        print(f"Data saved to {OUTPUT_FILE}")
    except IOError as e:
        print(f"Error writing to file {OUTPUT_FILE}: {e}")

if __name__ == "__main__":
    main()

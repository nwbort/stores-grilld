# FILE: scrape.py
import requests
import json
import time
import os
import threading
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from concurrent.futures import ThreadPoolExecutor, as_completed

BASE_URL = "https://grilld.com.au"
RESTAURANTS_LIST_URL = urljoin(BASE_URL, "/restaurants")
OUTPUT_FILE = "stores.json"
MAX_WORKERS = 10
DEBUG_DIR = "debug_html"

# --- START: Thread-safe debug flag ---
# A lock to ensure only one thread can modify the flag at a time.
_debug_lock = threading.Lock()
# The flag itself. Once set to True, no more debug files will be saved.
_debug_file_saved = False
# --- END: Thread-safe debug flag ---

def get_store_urls():
    """Fetches the main restaurants page and extracts all individual store URLs."""
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

def save_debug_html(url, content):
    """Saves the HTML content of the FIRST failed page for debugging."""
    global _debug_file_saved
    
    # Use a lock to check and set the flag atomically (thread-safe).
    with _debug_lock:
        if _debug_file_saved:
            return # A debug file has already been saved by another thread.
        
        try:
            os.makedirs(DEBUG_DIR, exist_ok=True)
            slug = url.strip('/').split('/')[-1]
            filename = os.path.join(DEBUG_DIR, f"debug_{slug}.html")
            with open(filename, 'wb') as f:
                f.write(content)
            print(f"  -> Saved debug HTML for first failure to {filename}")
            _debug_file_saved = True # Set the flag so no other thread will save a file.
        except Exception as e:
            print(f"  -> Failed to save debug file for {url}: {e}")

def scrape_store_page(url):
    """Fetches a store page and extracts data from the embedded __NUXT_DATA__ JSON blob."""
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
        save_debug_html(url, response.content)
        return None

    try:
        nuxt_data = json.loads(nuxt_data_script.string)
    except json.JSONDecodeError:
        print(f"  -> Error: Failed to parse JSON from __NUXT_DATA__ on {url}")
        save_debug_html(url, response.content)
        return None

    store_info = None
    if isinstance(nuxt_data, list):
        for item in nuxt_data:
            if isinstance(item, dict) and 'state' in item:
                state_obj = item.get("state", {})
                if isinstance(state_obj, dict):
                     restaurant_data = state_obj.get("restaurant", {}).get("restaurant")
                     if isinstance(restaurant_data, dict):
                         store_info = restaurant_data
                         break
            
    if not store_info:
        print(f"  -> Error: Could not find restaurant data object in __NUXT_DATA__ on {url}")
        save_debug_html(url, response.content)
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
    """Main function to orchestrate the scraping process."""
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

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
# Number of parallel threads to use for scraping store pages.
# Adjust this based on your network and the server's tolerance. 10 is a safe start.
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
    for item in nuxt_data:
        if isinstance(item, dict) and 'restaurantId' in item:
            store_info = item
            break
            
    if not store_info:
        print(f"  -> Error: Could not find store data object in __NUXT_DATA__ on {url}")
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
    # This print statement is now less descriptive of progress, but confirms extraction
    print(f"  -> Extracted: {data['name']}")
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
    
    # Use ThreadPoolExecutor to scrape pages in parallel
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Create a future for each URL. A future is a placeholder for a result that will exist later.
        future_to_url = {executor.submit(scrape_store_page, url): url for url in store_urls}
        
        total_urls = len(store_urls)
        print(f"\nScraping {total_urls} store pages with up to {MAX_WORKERS} parallel workers...")

        # As each future completes, process its result
        for i, future in enumerate(as_completed(future_to_url), 1):
            store_data = future.result()
            if store_data:
                all_stores_data.append(store_data)
            
            # Print progress
            print(f"Progress: {i}/{total_urls} pages processed.", end='\r')

    print("\n" + "="*30) # Newline after the progress indicator

    # Sort stores alphabetically by name for a consistent output file
    all_stores_data.sort(key=lambda x: x.get('name') or '')

    end_time = time.time()
    duration = end_time - start_time
    
    print(f"Successfully scraped data for {len(all_stores_data)} out of {len(store_urls)} stores.")
    print(f"Total execution time: {duration:.2f} seconds.")
    
    # Save the final list of store data to stores.json
    try:
        with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
            json.dump(all_stores_data, f, indent=2, ensure_ascii=False)
        print(f"Data saved to {OUTPUT_FILE}")
    except IOError as e:
        print(f"Error writing to file {OUTPUT_FILE}: {e}")

if __name__ == "__main__":
    main()

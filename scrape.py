# FILE: scrape.py
import requests
import json
import time
from datetime import datetime
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from concurrent.futures import ThreadPoolExecutor, as_completed

BASE_URL = "https://grilld.com.au"
RESTAURANTS_LIST_URL = urljoin(BASE_URL, "/restaurants")
OUTPUT_FILE = "stores.json"
MAX_WORKERS = 10

def get_store_urls():
    """Fetches the main restaurants page and extracts all individual store URLs."""
    print(f"Fetching store list from {RESTAURANTS_LIST_URL}...")
    try:
        response = requests.get(RESTAURANTS_LIST_URL, timeout=20)
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
    Fetches a store page and extracts data primarily from the ld+json script tag,
    supplemented with data from other parts of the page.
    """
    try:
        response = requests.get(url, timeout=20)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"  -> Error fetching {url}: {e}")
        return None

    soup = BeautifulSoup(response.content, "lxml")

    # --- 1. Primary Source: ld+json for core data (most reliable) ---
    ld_json_script = soup.find('script', type='application/ld+json')
    if not ld_json_script:
        print(f"  -> Error: ld+json script tag not found on {url}")
        return None
    try:
        ld_data = json.loads(ld_json_script.string)
    except json.JSONDecodeError:
        print(f"  -> Error: Failed to parse ld+json on {url}")
        return None

    # Combine address parts for a full address string
    address_obj = ld_data.get('address', {})
    address_parts = [
        address_obj.get('streetAddress'),
        address_obj.get('addressLocality'),
        address_obj.get('addressRegion')
    ]
    full_address = ', '.join(filter(None, [part.strip() if part else None for part in address_parts]))

    # Reformat opening hours to match the desired structure
    opening_hours_spec = ld_data.get('openingHoursSpecification', [])
    opening_hours = []
    day_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    for spec in opening_hours_spec:
        try:
            day = spec.get('dayOfWeek')
            opens_dt = datetime.strptime(spec['opens'], '%H:%M')
            closes_dt = datetime.strptime(spec['closes'], '%H:%M')
            opens_formatted = opens_dt.strftime('%-I%p').replace('AM', 'AM').replace('PM', 'PM')
            closes_formatted = closes_dt.strftime('%-I%p').replace('AM', 'AM').replace('PM', 'PM')
            desc = f"{opens_formatted} - {closes_formatted}"
            opening_hours.append({'name': day, 'description': desc, 'isClosed': False})
        except (ValueError, TypeError, KeyError):
            continue
    opening_hours.sort(key=lambda x: day_order.index(x['name']) if x['name'] in day_order else 99)

    data = {
        'name': ld_data.get('name'),
        'address': full_address,
        'phone': ld_data.get('telephone'),
        'opening_hours': opening_hours,
        'url': url,
        'description': None, 'services': [], 'latitude': None, 'longitude': None # Placeholders
    }

    # --- 2. Scrape visible HTML for services ---
    data['services'] = [chip.text.strip() for chip in soup.select('.restaurant-chips .chip-text')]

    # --- 3. Scrape __NUXT_DATA__ for geo-coords and description (less reliable) ---
    nuxt_data_script = soup.find('script', id='__NUXT_DATA__')
    if nuxt_data_script:
        try:
            nuxt_data = json.loads(nuxt_data_script.string)
            def dereference(data_list, ref):
                if isinstance(ref, int) and 0 <= ref < len(data_list):
                    return data_list[ref]
                return None
            
            state_ref_obj = next((item for item in nuxt_data if isinstance(item, dict) and 'state' in item), None)
            if state_ref_obj:
                state_obj = dereference(nuxt_data, state_ref_obj.get('state'))
                if isinstance(state_obj, dict):
                    restaurant_ref = state_obj.get('restaurant', {}).get('restaurant')
                    restaurant_obj = dereference(nuxt_data, restaurant_ref)
                    if isinstance(restaurant_obj, dict):
                        data['latitude'] = dereference(nuxt_data, restaurant_obj.get('latitude'))
                        data['longitude'] = dereference(nuxt_data, restaurant_obj.get('longitude'))
                        data['description'] = dereference(nuxt_data, restaurant_obj.get('description'))
        except (json.JSONDecodeError, IndexError, TypeError) as e:
            print(f"  -> Warning: Could not parse __NUXT_DATA__ for extra details on {url}. Error: {e}")
            
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
            url = future_to_url[future]
            store_data = future.result()
            if store_data:
                all_stores_data.append(store_data)
                print(f"Progress: {completed_count}/{total_urls} | Extracted: {store_data.get('name')}")
            else:
                print(f"Progress: {completed_count}/{total_urls} | Failed to extract data for {url}")

    print("\n" + "="*30)
    all_stores_data.sort(key=lambda x: (x.get('name') or '').lower())
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

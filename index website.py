import csv
import time
import threading
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed
headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36"}



# For optional JS rendering with Selenium
try:
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    SELENIUM_AVAILABLE = True
except ImportError:
    SELENIUM_AVAILABLE = False

# Global error log and lock for thread safety
error_log = {}
error_log_lock = threading.Lock()

def log_error(domain, error_message):
    """Logs error messages for a given domain in a thread-safe way."""
    with error_log_lock:
        if domain not in error_log:
            error_log[domain] = set()
        error_log[domain].add(error_message)

def load_input_csv(filename):
    """
    Reads an input CSV file containing URLs.
    The CSV must have a header row with a column named 'url'.
    Returns a list of URLs.
    """
    urls = []
    with open(filename, mode='r', newline='', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        for row in reader:
            urls.append(row['url'])
    return urls

def save_to_csv(data, filename, fieldnames):
    """Saves a list of dictionaries to a CSV file."""
    if not data:
        return
    with open(filename, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)

def is_valid_url(url):
    """Check if the URL has a valid scheme and network location."""
    parsed = urlparse(url)
    return bool(parsed.scheme) and bool(parsed.netloc)

def extract_links(base_url, soup):
    """Extract and return a set of absolute URLs found in the page."""
    links = set()
    for a_tag in soup.find_all('a', href=True):
        link = urljoin(base_url, a_tag['href'])
        if is_valid_url(link):
            links.add(link)
    return links

def get_page_title(soup):
    """Extract the <title> text from a BeautifulSoup object."""
    if soup:
        title_tag = soup.find('title')
        if title_tag:
            return title_tag.get_text().strip()
    return ""

def fetch_page_with_selenium(url, timeout=10):
    """Uses Selenium to render the page and return the page source."""
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    # You can add more options here (e.g. disable images, etc.) for speed.
    driver = webdriver.Chrome(options=options)
    driver.set_page_load_timeout(timeout)
    try:
        driver.get(url)
        time.sleep(1)  # slight delay to let JS load (adjust as needed)
        page_source = driver.page_source
    except Exception as e:
        log_error(urlparse(url).netloc, str(e))
        page_source = ""
    finally:
        driver.quit()
    return page_source

def search_product_on_page(url, product_keyword, timeout=5, use_selenium=False):
    """
    Fetch the page content and search for the product keyword ("lectern").
    If use_selenium is True and Selenium is available, the page is rendered using Selenium.
    Returns a tuple (soup, found):
      - soup: BeautifulSoup object (or None if error)
      - found: Boolean indicating if the keyword was found
    Logs errors for the domain if encountered.
    """
    try:
        if use_selenium and SELENIUM_AVAILABLE:
            page_source = fetch_page_with_selenium(url, timeout=timeout)
            if not page_source:
                return None, False
            soup = BeautifulSoup(page_source, 'html.parser')
        else:
            response = requests.get(url, headers=headers,timeout=timeout)
            if response.status_code != 200:
                domain = urlparse(url).netloc
                log_error(domain, f"Status code: {response.status_code}")
                return None, False
            soup = BeautifulSoup(response.text, 'html.parser')
        found = product_keyword.lower() in soup.get_text().lower()
        return soup, found
    except Exception as e:
        domain = urlparse(url).netloc
        log_error(domain, str(e))
        return None, False

def crawl_website(url, product_keyword, visited=None, max_depth=2, depth=0, max_workers=5, use_selenium=False):
    """
    Recursively crawl pages on the same domain starting from 'url'.
    Uses concurrency to speed up crawling.
    Returns a list of dicts: [{'url': <url>, 'title': <title>}, ...] for all pages where the keyword was found.
    """
    if visited is None:
        visited = set()
    if depth > max_depth or url in visited:
        return []
    visited.add(url)

    print(f"Crawling (depth={depth}): {url}")
    soup, found = search_product_on_page(url, product_keyword, timeout=5, use_selenium=use_selenium)
    found_pages = []

    if found:
        title = get_page_title(soup)
        found_pages.append({"url": url, "title": title})

    if soup and depth < max_depth:
        base_domain = urlparse(url).netloc
        next_links = [
            link for link in extract_links(url, soup)
            if urlparse(link).netloc == base_domain and link not in visited
        ]
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [
                executor.submit(crawl_website, link, product_keyword, visited, max_depth, depth + 1, max_workers, use_selenium)
                for link in next_links
            ]
            for future in as_completed(futures):
                found_pages.extend(future.result())

    return found_pages

if __name__ == "__main__":
    overall_start = time.time()

    product_keyword = "lectern"  # Product keyword to search for
    input_csv = r"C:\Users\kimiw\input_files1.csv"
    starting_urls = load_input_csv(input_csv)

    # Set this flag to True if you want to use Selenium for JS rendering.
    # Note: This will slow down the crawl considerably.
    use_selenium = False

    all_found_pages = []
    found_domains_set = set()

    for start_url in starting_urls:
        pages_with_keyword = crawl_website(start_url, product_keyword, max_depth=2, max_workers=5, use_selenium=use_selenium)
        all_found_pages.extend(pages_with_keyword)
        for page in pages_with_keyword:
            domain = urlparse(page["url"]).netloc
            found_domains_set.add(domain)

    save_to_csv(all_found_pages, r"C:\Users\kimiw\Downloads\scrapper\found_pages.csv", fieldnames=["url", "title"])
    print(f"\nSaved {len(all_found_pages)} pages containing '{product_keyword}' to 'found_pages.csv'.")

    found_domains_data = [{"domain": d} for d in found_domains_set]
    save_to_csv(found_domains_data, r"C:\Users\kimiw\Downloads\scrapper\found_domains.csv", fieldnames=["domain"])
    print(f"Saved {len(found_domains_set)} unique domains with '{product_keyword}' to 'found_domains.csv'.")

    input_domains = {urlparse(url).netloc for url in starting_urls}
    not_found_domains = input_domains - found_domains_set

    error_domains_data = []
    with error_log_lock:
        for domain, errors in error_log.items():
            error_domains_data.append({"domain": domain, "errors": "; ".join(errors)})

    save_to_csv(error_domains_data, "error_domains.csv", fieldnames=["domain", "errors"])
    print(f"Saved {len(error_domains_data)} domains with errors to 'error_domains.csv'.")

    count_found = len(found_domains_set)
    count_not_found = len(not_found_domains)
    print(f"\nSummary:")
    print(f" - Domains with '{product_keyword}' found: {count_found}")
    print(f" - Domains without '{product_keyword}' found: {count_not_found}")

    overall_end = time.time()
    elapsed_time = overall_end - overall_start
    print(f"\nTotal elapsed time: {elapsed_time:.2f} seconds")

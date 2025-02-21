import csv
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import time

 


def load_input_csv(filename):
    """
    Reads an input CSV file containing URLs.
    The CSV must have a header row with a column 'url'.
    Returns a list of URLs.
    """
    urls = []
    with open(filename, mode='r', newline='', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        for row in reader:
            # Make sure your CSV has a column named 'url'
            urls.append(row['url'])
    return urls

def save_to_csv(data, filename):
    """
    Saves a list of dictionaries (each with 'url' and 'title') to a CSV file.
    """
    if not data:
        return
    with open(filename, mode='w', newline='', encoding='utf-8') as file:
        fieldnames = data[0].keys()  # e.g., ['url', 'title']
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

def search_product_on_page(url, product_keyword):
    """
    Fetch the page content and search for the product keyword.
    Returns a tuple (soup, found):
      - soup: BeautifulSoup object (or None if error)
      - found: Boolean indicating if the keyword was found
    """
    try:
        response = requests.get(url, timeout=10)
        if response.status_code != 200:
            return None, False
        soup = BeautifulSoup(response.text, 'html.parser')
        found = product_keyword.lower() in soup.get_text().lower()
        return soup, found
    except Exception as e:
        print(f"Error fetching {url}: {e}")
        return None, False

def get_page_title(soup):
    """Extract the <title> text from a BeautifulSoup object."""
    if soup:
        title_tag = soup.find('title')
        if title_tag:
            return title_tag.get_text().strip()
    return ""

def crawl_website(url, product_keyword, visited=None, max_depth=2, depth=0):
    
    """
    Recursively crawl pages on the same domain starting from 'url'.
    - product_keyword: The text (e.g., "lecturns") to search for.
    - visited: A set to keep track of visited URLs (avoid re-crawling).
    - max_depth: Maximum recursion depth (to prevent infinite crawl).
    - depth: Current recursion depth.
    
    Returns a list of dictionaries: [{'url': <url>, 'title': <title>}...]
    for all pages where the keyword was found.
    """
    if visited is None:
        visited = set()
    if depth > max_depth:
        return []
    if url in visited:
        return []
    visited.add(url)

    print(f"Crawling (depth={depth}): {url}")
    soup, found = search_product_on_page(url, product_keyword)
    found_pages = []
    
    if found:
        title = get_page_title(soup)
        found_pages.append({"url": url, "title": title})
        
        print(f"\nfound website {url}   ------------------")

    # If we got a valid soup, continue crawling internal links
    if soup:
        for link in extract_links(url, soup):
            # Only follow links within the same domain
            if urlparse(url).netloc == urlparse(link).netloc:
                found_pages.extend(
                    crawl_website(link, product_keyword, visited, max_depth, depth + 1)
                )

    return found_pages

if __name__ == "__main__":
    # Your product keyword
    start_time = time.time()
    product_keyword = "lectern"

    # CSV file with a header row and column named 'url'
    input_csv =r"C:\Users\User\scrapper\input_urls.csv"
    starting_urls = load_input_csv(input_csv)

    all_found_pages = []
    for start_url in starting_urls:
        # Crawl each URL in the CSV
        pages_with_keyword = crawl_website(start_url, product_keyword, max_depth=2)
        all_found_pages.extend(pages_with_keyword)

    # Save all found pages to output CSV
    output_csv = r"C:\Users\User\scrapper\found_pages.csv"
    if all_found_pages:
        save_to_csv(all_found_pages, output_csv)
        print(f"\nSaved pages containing '{product_keyword}' to '{output_csv}'.")
        
    else:
        print(f"\nNo pages found containing '{product_keyword}'.")
        end_time = time.time()
        

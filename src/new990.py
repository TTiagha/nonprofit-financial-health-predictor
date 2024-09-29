import os
import requests
import datetime
from bs4 import BeautifulSoup
import json

def display_user_message():
    print("=" * 80)
    print("Checking for new Form 990 files...")
    print("This process will look for updates on the IRS website:")
    print("https://www.irs.gov/charities-non-profits/form-990-series-downloads")
    print("=" * 80)

# Function to fetch the IRS webpage content
def fetch_irs_page():
    url = "https://www.irs.gov/charities-non-profits/form-990-series-downloads"
    response = requests.get(url)
    response.raise_for_status()
    return response.text

# Function to extract links for a specific year using BeautifulSoup
def extract_links(page_content, year):
    soup = BeautifulSoup(page_content, 'html.parser')
    year_section = soup.find('h4', string=str(year))
    if year_section:
        links = []
        for a in year_section.find_next('ul').find_all('a'):
            href = a.get('href')
            if href and href.startswith('https://') and f'/{year}/' in href and href.endswith('.zip'):
                links.append(href)
        return links
    return []

# Function to read the last checked year
def read_last_checked_year():
    try:
        with open("last_year_checked.txt", "r") as f:
            return int(f.read().strip())
    except FileNotFoundError:
        return datetime.datetime.now().year

# Function to write the last checked year
def write_last_checked_year(year):
    with open("last_year_checked.txt", "w") as f:
        f.write(str(year))

# Function to update AVAILABLE_URLS in main.py
def update_available_urls(new_urls):
    main_py_path = 'src/main.py'
    with open(main_py_path, 'r') as f:
        content = f.read()

    start_index = content.find('AVAILABLE_URLS = {')
    if start_index == -1:
        print("AVAILABLE_URLS not found in main.py")
        return

    end_index = content.find('}', start_index)
    if end_index == -1:
        print("Error: Couldn't find the end of AVAILABLE_URLS dictionary")
        return

    available_urls_str = content[start_index:end_index+1]
    available_urls = eval(available_urls_str.split('=', 1)[1].strip())

    # Update AVAILABLE_URLS with new URLs, avoiding duplicates
    for year, urls in new_urls.items():
        if year not in available_urls:
            available_urls[year] = []
        for url in urls:
            if url not in available_urls[year]:
                available_urls[year].append(url)

    # Convert the updated dictionary to a formatted string
    updated_urls_str = json.dumps(available_urls, indent=4)

    # Replace the old AVAILABLE_URLS with the updated one
    updated_content = (
        content[:start_index] +
        f"AVAILABLE_URLS = {updated_urls_str}" +
        content[end_index+1:]
    )

    with open(main_py_path, 'w') as f:
        f.write(updated_content)

    print(f"Updated AVAILABLE_URLS in {main_py_path}")

# Main script logic
def check_for_updates():
    display_user_message()
    
    last_year_checked = read_last_checked_year()
    current_year = datetime.datetime.now().year
    next_year = current_year + 1

    page_content = fetch_irs_page()
    new_files_found = False
    new_urls = {}

    for year in range(last_year_checked + 1, next_year + 1):
        print(f"\nChecking for files for the year: {year}")
        links = extract_links(page_content, year)
        
        if links:
            print(f"New files for {year} found:")
            for link in links:
                print(link)
            new_files_found = True
            new_urls[str(year)] = links
            write_last_checked_year(year)
        else:
            print(f"No files for {year} are available yet.")
            break  # Stop checking if we find a year with no files

    if new_files_found and int(list(new_urls.keys())[0]) >= 2025:
        update_available_urls(new_urls)
    elif not new_files_found:
        print(f"\nNo new updates. The last year checked was {last_year_checked}.")

if __name__ == "__main__":
    check_for_updates()

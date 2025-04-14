import requests
from bs4 import BeautifulSoup
import pandas as pd
import logging
import schedule
import time
from datetime import datetime
import os

# Set up logging
logging.basicConfig(filename='scraper.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Function to fetch and parse the webpage
def fetch_jobs():
    url = 'https://vacancymail.co.zw/jobs/'
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,/;q=0.8'
    }
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        return soup
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching the webpage: {e}")
        return None

# Function to extract job data
def extract_job_data(soup):
    job_list = []
    try:
        job_posts = soup.find_all('div', class_='job-listing-details', limit=10)
        for job in job_posts:
            try:
                title_tag = job.find('h3', class_='job-listing-title')
                company_tag = job.find('h4', class_='job-listing-company')
                desc_tag = job.find('p', class_='job-listing-text')

                title = title_tag.text.strip() if title_tag else 'Not specified'
                company = company_tag.text.strip() if company_tag else 'Not specified'
                description = desc_tag.text.strip() if desc_tag else 'Not specified'

                job_list.append({
                    'Job Title': title,
                    'Company': company,
                    'Location': 'Not specified',
                    'Expiry Date': 'Not specified',
                    'Job Description': description,
                    'Scraped Date': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                })
            except Exception as e:
                logging.warning(f"Error parsing a job listing: {e}")
                continue
    except Exception as e:
        logging.error(f"Error extracting job data: {e}")
    return job_list

# Function to save data to CSV
def save_to_csv(job_list):
    df = pd.DataFrame(job_list)

    output_file = 'scraped_data.csv'
    file_exists = os.path.isfile(output_file)

    if file_exists:
        df.to_csv(output_file, mode='a', header=False, index=False)
    else:
        df.to_csv(output_file, index=False)

    logging.info(f"Data saved to {output_file} with {len(df)} entries")

# Main function to orchestrate the scraping
def scrape_jobs():
    logging.info("Scraping started")
    soup = fetch_jobs()
    if soup:
        job_list = extract_job_data(soup)
        if job_list:
            save_to_csv(job_list)
        else:
            logging.warning("No job data found")
    else:
        logging.warning("Failed to retrieve the webpage")
    logging.info("Scraping completed")

# Schedule the scraping task
def schedule_scraping():
    schedule.every().day.at("00:00").do(scrape_jobs)
    while True:
        schedule.run_pending()
        time.sleep(1)

# Main execution
if __name__== "_main_":
    scrape_jobs()  # Run immediately
    schedule_scraping()  # Then schedule future runs
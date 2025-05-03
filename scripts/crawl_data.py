import os
import time
import json
import logging
import csv
from urllib.parse import urlparse
import sys
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, ElementClickInterceptedException, TimeoutException
from bs4 import BeautifulSoup
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
# Configure logging
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
LOG_DIR = os.path.join(os.path.dirname(__file__), "..", "logs")
os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, "crawler.log"), 'a'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)
from utils.constants import INPUT_PATH, OUTPUT_PATH
from kafka_listening_staging import send_to_kafka, reset_kafka_topic
from utils.constants import KAFKA_BOOTSTRAP_SERVERS, TOPIC_PHONE_DATA

CONFIG = {
    'url_link': 'https://mobilecity.vn/dien-thoai',
    'output_dir': os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'data_crawled')),
    'links_csv': f'{OUTPUT_PATH}/links.csv',
    'output_csv':f'{OUTPUT_PATH}/raw_data.csv',
    'max_links': 3
}


os.makedirs(CONFIG['output_dir'], exist_ok=True)

def is_valid_url(url):
    """Check if the URL is valid."""
    parsed = urlparse(url)
    return bool(parsed.scheme and parsed.netloc)

def click_see_more_button(driver):
    """Click the 'See More' button if it exists."""
    try:
        see_more_button = driver.find_element(By.ID, "product_view_more")
        if see_more_button.is_displayed():
            see_more_button.click()
            time.sleep(2)  # Wait for products to load
            return True
    except (NoSuchElementException, ElementClickInterceptedException):
        return False
    return False

def collect_product_links(driver, url):
    """Collect product links by clicking 'See More' until all products are loaded."""
    logger.info("Starting to collect product links...")

    driver.get(url)
    links = []

    while True:
        soup = BeautifulSoup(driver.page_source, "html.parser")
        product_items = soup.find_all("div", class_="product-item-info")

        new_links = [item.find("a", href=True)['href'] for item in product_items if item.find("a", href=True)]
        links.extend(new_links)

        links = list(set(links))  # Remove duplicates

        if CONFIG['max_links'] and len(links) >= CONFIG['max_links']:
            links = links[:CONFIG['max_links']] 
            logger.info(f"Collected {len(links)} product links, stopping collection.")
            break
        if not click_see_more_button(driver):
            break
    logger.info(f"Collected {len(links)} product links.")
    return links

def scrape_product_data(driver, link):
    """Scrape product details from a given link."""
    logger.info(f"Scraping product data from: {link}")
    driver.get(link)
    soup = BeautifulSoup(driver.page_source, 'html.parser')

    # Collect product details
    try:
        product_title = soup.find("h1", class_="title").text.strip()
    except AttributeError:
        product_title = "Không có tên sản phẩm"

    try:
        brand = driver.find_element(By.XPATH, '//ul[@itemscope]/li[2]/a/span').text.strip()
    except NoSuchElementException:
        brand = "Không có thông tin hãng"

    try:
        warranty_text = soup.find('span', class_='warranty-content-default').get_text(strip=True)
    except AttributeError:
        warranty_text = "Không có thông tin bảo hành"

    try:
        specs_table = soup.find('div', class_='product-info-content').find('table')
        specs = {row.find_all('td')[0].text.strip(): row.find_all('td')[1].text.strip()
                 for row in specs_table.find_all('tr') if len(row.find_all('td')) == 2}
    except AttributeError:
        specs = {"Không có thông số kỹ thuật": "N/A"}

    try:
        rating = soup.find('div', class_='comment-vote__star-number').get_text(strip=True)
    except AttributeError:
        rating = "Không có đánh giá"

    try:
        total_reviews = soup.find('div', class_='comment-vote__star-total').get_text(strip=True).replace("đánh giá và hỏi đáp", "").strip()
    except AttributeError:
        total_reviews = "Không có thông tin đánh giá và hỏi đáp"

    # Collect color, storage, and price info
    color_storage_price = []
    color_items = driver.find_elements(By.CLASS_NAME, "color-item.attribute-item")
    storage_items = driver.find_elements(By.CLASS_NAME, "storage-item.attribute-item")

    def get_price():
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        try:
            price_new = soup.find("p", class_="price").text.strip()
        except AttributeError:
            price_new = "Không có giá mới"
        try:
            price_old = soup.find("p", class_="price-old").text.strip()
        except AttributeError:
            price_old = "Không có giá cũ"
        return price_new, price_old

    if not color_items and not storage_items:
        price_new, price_old = get_price()
        color_storage_price.append(["", "", price_new, price_old])
    elif not color_items:
        for storage in storage_items:
            storage.click()
            price_new, price_old = get_price()
            color_storage_price.append(["", storage.text.strip(), price_new, price_old])
    elif not storage_items:
        for color in color_items:
            color.click()
            price_new, price_old = get_price()
            color_storage_price.append([color.get_attribute("data-title"), "", price_new, price_old])
    else:
        for color in color_items:
            for storage in storage_items:
                color.click()
                storage.click()
                price_new, price_old = get_price()
                color_storage_price.append([color.get_attribute("data-title"), storage.text.strip(), price_new, price_old])

    return {
        "Tên sản phẩm": product_title,
        "Loại điện thoại": brand,
        "Màu sắc - Phiên bản bộ nhớ - Giá tương ứng": color_storage_price,
        "Thời gian bảo hành": warranty_text,
        "Thông số kỹ thuật": json.dumps(specs, ensure_ascii=False),
        "Đánh giá": rating,
        "Số lượt đánh giá và hỏi đáp": total_reviews,
        "Đường dẫn": link,
    }


def ensure_directory_exists(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)



def main():
    # Khởi tạo logger
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    logger.info(f"Starting crawler {CONFIG['max_links']} links...")

    # Cấu hình Selenium
    options = webdriver.ChromeOptions()
    options.add_argument('--incognito')
    options.add_argument("--headless=new")
    options.add_argument('--window-size=1920,1080')
    options.add_argument('--disable-gpu')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)

    # Khởi tạo ChromeDriver
    try:
        service = Service("/usr/local/bin/chromedriver")
        driver = webdriver.Chrome(service=service, options=options)
        logger.info("ChromeDriver initialized successfully.")
    except Exception as e:
        logger.error(f"Failed to initialize ChromeDriver: {e}")
        return

    try:
        try:
            logger.info("Collecting product links from website...")
            links = collect_product_links(driver, CONFIG['url_link'])

            # Đảm bảo thư mục tồn tại
            ensure_directory_exists(os.path.dirname(CONFIG['links_csv']))
            pd.DataFrame(links, columns=['links']).to_csv(CONFIG['links_csv'], index=False)
            logger.info(f"Saved {len(links)} links to {CONFIG['links_csv']}")
        except Exception as e:
            logger.error(f"Error collecting product links: {e}")
            return


        product_data = []
        ensure_directory_exists(os.path.dirname(CONFIG['output_csv']))

        for i, link in enumerate(links):
            if not is_valid_url(link):
                logger.warning(f"Invalid URL: {link}")
                continue
            try:
                data = scrape_product_data(driver, link)
                product_data.append(data)
                logger.info(f"Completed scraping product {i + 1}/{len(links)}")
            except Exception as e:
                logger.error(f"Error scraping {link}: {e}")


        if product_data:
            try:
                # reset_kafka_topic(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, topic=TOPIC_PHONE_DATA)
                send_to_kafka(data=product_data, bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, topic=TOPIC_PHONE_DATA)
                logger.info("Data sent to Kafka successfully.")
            except Exception as e:
                logger.error(f"Failed to send data to Kafka: {e}")

        if product_data:
            try:
                with open(CONFIG['output_csv'], 'w', newline='', encoding='utf-8-sig') as csvfile:
                    fieldnames = product_data[0].keys()
                    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                    writer.writeheader()
                    writer.writerows(product_data)
                    logger.info(f"Data saved to {CONFIG['output_csv']} successfully.")
            except Exception as e:
                logger.error(f"Failed to save data to CSV: {e}")
        else:
            logger.warning("No product data to save.")

    finally:
        driver.quit()
        logger.info("ChromeDriver closed.")

if __name__ == "__main__":
    main()
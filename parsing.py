import re

import requests
import logging
import pandas as pd
from bs4 import BeautifulSoup
import csv
from time import sleep
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from urllib.parse import urljoin

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Настройка опций для Selenium
chrome_options = Options()
chrome_options.add_argument('--headless')
chrome_options.add_argument('--no-sandbox')
chrome_options.add_argument('--disable-dev-shm-usage')
chrome_options.add_argument('--remote-debugging-port=9222')
chrome_options.add_argument('--user-data-dir=/tmp/user-data')
chrome_options.add_argument('--disable-gpu')

driver = webdriver.Chrome(options=chrome_options)


def fetch_page(url, headers):
    """Загружает страницу и возвращает HTML-контент."""
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        return response.content
    except requests.exceptions.RequestException as e:
        logging.error(f"Ошибка при запросе {url}: {e}")
        return None

def safe_get_text(element, tag, class_=None, default=""):
    found = element.find(tag, class_=class_)
    return found.get_text(strip=True) if found else default

def get_text_or_none(element):
    return element.get_text(strip=True) if element else 'N/A'


def save_to_csv(file_path, data, headers):
    """Сохраняет данные в CSV-файл."""
    if not data:
        logging.warning("Нет данных для сохранения.")
        return
    with open(file_path, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(headers)
        writer.writerows(data)


def save_to_csv_without_duplicates(file_path, data):
    if not data:
        logging.warning("Нет данных для сохранения.")
        return

    df = pd.DataFrame(data)
    df.drop_duplicates(inplace=True)  # Удаление дубликатов
    df.to_csv(file_path, index=False, encoding='utf-8')
    logging.info(f"Файл сохранен по пути: {file_path}")


def decode_spamspan(spamspan_element):
    email_parts = spamspan_element.find_all('span')
    return ''.join(part.get_text(strip=True) for part in email_parts).replace('[at]', '@').replace('[dot]', '.')


def load_links_from_csv(file_path):
    """Загружает ссылки из CSV файла"""
    links_df = pd.read_csv(file_path)
    return links_df['Link'].tolist() # Предполагается, что столбец называется Link

def safe_find(soup, selector, attribute=None, text_only=False, separator=' ', strip=True):
    try:
        element = soup.select_one(selector)
        if text_only:
            return element.get_text(separator=separator, strip=strip) if element else ''
        if attribute:
            return element[attribute] if element and attribute in element.attrs else ''
        return element.text.strip() if element else ''
    except AttributeError:
        return ''

def parse_csv_file(file_path):
    """Функция для парсинга конкретных данных по линкам из файла (данные могут быть изменены)"""

    FIELDS = {
        'title': ('h1.title', True),
        'call_type': ('.field-name-field-open-call-type ul', True),
        'industry': ('.field-name-field-opencall-industry ul', True),
        'category': ('.field-name-field-category-addapost ul', True),
        'theme': ('.field-name-field-open-call-theme ul', True),
        'country': ('.field-name-field-tags-news-country ul', True),
        'organisation': ('.field-name-field-organisation ul', True),
        'eligibility': ('.field-name-field-eligibility ul', True),
        'keywords': ('.field-name-field-tags-news ul', True),
        'description': ('.field-name-field-description', True, False),
        'prize_summary': ('.field-name-field-prize-summary', True, False),
        'prizes_details': ('.field-name-field-opencall-prizes', True, False),
        'event_date': ('.field-name-field-opencall-event-date', True, False),
        'deadline': ('.field-name-field-deadline-data', True, False),
        'entry_fee': ('.field-name-field-entry-fee ul', True),
        'fee_detail': ('.field-name-field-application-fee', True, False),
        'contact_links': ('.field-name-field-post-contact-links', True, False),
        'instagram': ('.field-name-field-opencall-instagram a', False, 'href'),
    }

    links = load_links_from_csv(file_path)
    data = []

    for link in links:
        try:
            response = requests.get(link, timeout=10)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')

            record = {}
            for field_name, (selector, text_only, *attr) in FIELDS.items():
                record[field_name] = safe_find(soup, selector, attribute=attr[0] if attr else None, text_only=text_only)
            data.append(record)

        except requests.exceptions.RequestException as e:
            print(f"Ошибка при обработке ссылки {link}: {e}")
            continue

    # Преобразование списка в DataFrame
    df = pd.DataFrame(data)
    output_file_path = 'artist_callforentry_12.csv'
    df.to_csv(output_file_path, index=False)


def main():
    """Основная функция для вызова всех парсеров."""
    try:
        # Задаем базовые URL и пути для сохранения данных
        tasks = [
            {
                "func": parse_artist_opportunities,
                "url": "https://www.artrabbit.com/artist-opportunities/",
                "output": "artist_opportunities.csv"
            },
            {
                "func": parse_transartists,
                "url": "https://www.transartists.org/en/call-artists?page=",
                "output": "transartists.csv"
            },
            {
                "func": parse_resartis_opportunities,
                "url": "https://resartis.org/open-calls/",
                "output": "resartis_opportunities.csv"
            },
            {
                "func": parse_curatorspace_opportunities,
                "url": "https://www.curatorspace.com/opportunities?page={page_num}",
                "output": "curatorspace.csv"
            },
            {
                "func": parse_artists_communities,
                "url": "https://artistcommunities.org/directory/open-calls",
                "output": "artist_communities.csv"
            },
        ]

        # Обрабатываем каждый парсер
        for task in tasks:
            logging.info(f"Starting {task['func'].__name__}...")
            task["func"](task["url"], task["output"])
            logging.info(f"Finished {task['func'].__name__}. Data saved to {task['output']}")

    except Exception as e:
        logging.error(f"An error occurred during execution: {e}")
    finally:
        # Закрываем драйвер Selenium (если он был использован)
        try:
            driver.quit()
        except Exception:
            pass


def parse_artist_opportunities(base_url, output_file):
    """Парсинг сайта https://www.artrabbit.com/artist-opportunities/"""
    html_content = fetch_page(base_url, headers={'User-Agent': 'Mozilla/5.0'})
    if not html_content:
        return []

    soup = BeautifulSoup(html_content, 'html.parser')
    artopp_elements = soup.find_all('div', class_='artopp')

    data = []
    for artopp_element in artopp_elements:
        row = {
            'Data-d': artopp_element.get('data-d', ''),
            'Data-a': artopp_element.get('data-a', ''),
            'Heading': safe_get_text(artopp_element, 'h3', 'b_categorical-heading mod--artopps'),
            'Alert': safe_get_text(artopp_element, 'p', 'b_ending-alert mod--just-opened'),
            'Title': safe_get_text(artopp_element, 'h2'),
            'Date Updated': safe_get_text(artopp_element, 'p', 'b_date'),
            'Body': safe_get_text(artopp_element, 'div', 'm_body-copy'),
            'URL': artopp_element.find('a', class_='b_submit mod--next').get('href', '') if artopp_element.find('a', class_='b_submit mod--next') else ''
        }
        data.append(row)

    save_to_csv_without_duplicates(output_file, data)
    logging.info("Saved Artist opportunities to %s", output_file)


def parse_transartists(base_url, output_file):
    """Парсинг сайта https://www.transartists.org/en/call-artists?page="""
    all_data = []

    for page_number in range(9):
        url = f"{base_url}{page_number}"
        html_content = fetch_page(url, headers={'User-Agent': 'Mozilla/5.0'})
        if not html_content:
            continue

        soup = BeautifulSoup(html_content, 'html.parser')
        rows = soup.find_all('tr')

        for row in rows:
            date = safe_get_text(row, 'td', class_='views-field views-field-created')
            content_td = row.find('td', class_='views-field views-field-field-your-ad')

            if content_td:
                title = safe_get_text(content_td, 'h2')
                description = ' '.join(p.get_text(strip=True) for p in content_td.find_all('p'))
                email = decode_spamspan(content_td.find('a', class_='spamspan')) if content_td.find('a', class_='spamspan') else ''
                website = content_td.find('a', href=lambda href: href and "http" in href).get('href') if content_td.find('a', href=lambda href: href and "http" in href) else ''

                all_data.append({
                    'Date': date,
                    'Title': title,
                    'Description': description,
                    'Email': email,
                    'Website': website
                })

    save_to_csv_without_duplicates(output_file, all_data)
    logging.info("Saved Transartist opportunities to %s", output_file)



def parse_resartis_opportunities(base_url, output_file):
    """Парсинг сайта https://resartis.org/open-calls/"""
    driver.get(base_url)

    try:
        # Ждём, пока элемент с классом 'grid__item postcard' появится
        WebDriverWait(driver, 10).until(
            EC.presence_of_all_elements_located((By.CLASS_NAME, 'grid__item'))
        )
        html = driver.page_source
        soup = BeautifulSoup(html, 'html.parser')

        items = soup.find_all('div', class_='grid__item postcard')
        logging.info(f"Found {len(items)} elements with class 'grid__item postcard'.")
    except Exception as e:
        logging.error(f"Error: {e}")
    finally:
        driver.quit()

    data = []
    for item in items:
        try:
            link_tag = item.find('a', href=True)
            link = link_tag['href'] if link_tag else None
            title = item.find('h2', class_='card__title').get_text(strip=True) if item.find('h2',
                                                                                            class_='card__title') \
                else "No Title"

            if not link:
                logging.warning(f"No link found for item: {title}")
                continue

            logging.info(f"Processing: {title}")

            detail_html = fetch_page(link, headers=None)
            if not detail_html:
                logging.warning(f"Failed to fetch details for {title}.")
                continue
            detail_soup = BeautifulSoup(detail_html, 'html.parser')

            # Извлечение других данных
            description_tag = detail_soup.find('div', class_='entry-content')
            description = description_tag.get_text(strip=True) if description_tag else "No description"

            deadline_tag = item.find('dt')
            deadline = deadline_tag.get_text(strip=True).replace('Deadline: ', '') if deadline_tag else "No deadline"
            country = deadline.split('Country: ')[1] if 'Country: ' in deadline else ''
            deadline = deadline.split('Country: ')[0] if 'Country: ' in deadline else deadline
            duration_tag = detail_soup.find('h5', text='Duration of residency')
            duration = duration_tag.find_next('span').get_text(strip=True) if duration_tag else "No duration"

            accommodation_tag = detail_soup.find('h5', text='Accommodation')
            accommodation = accommodation_tag.find_next('span').get_text(
                strip=True) if accommodation_tag else "No accommodation"

            disciplines_tag = detail_soup.find('h5', text='Disciplines, work equipment and assistance')
            disciplines = disciplines_tag.find_next('span').get_text(
                strip=True) if disciplines_tag else "No disciplines"

            studio_tag = detail_soup.find('h5', text='Studio / Workspace')
            studio = studio_tag.find_next('span').get_text(strip=True) if studio_tag else "No studio"

            fees_tag = detail_soup.find('h5', text='Fees and support')
            fees = fees_tag.find_next('span').get_text(strip=True) if fees_tag else "No fees"

            expectations_tag = detail_soup.find('h5', text='Expectations towards the artist')
            expectations = expectations_tag.find_next('span').get_text(
                strip=True) if expectations_tag else "No expectations"

            application_info_tag = detail_soup.find('h5', text='Application information')
            application_info = application_info_tag.find_next('span').get_text(
                strip=True) if application_info_tag else "No application info"

            application_deadline_tag = detail_soup.find('h5', text='Application deadline')
            application_deadline = application_deadline_tag.find_next('span').get_text(
                strip=True) if application_deadline_tag else "No application deadline"

            residency_starts_tag = detail_soup.find('h5', text='Residency starts')
            residency_starts = residency_starts_tag.find_next('span').get_text(
                strip=True) if residency_starts_tag else "No residency starts"

            residency_ends_tag = detail_soup.find('h5', text='Residency ends')
            residency_ends = residency_ends_tag.find_next('span').get_text(
                strip=True) if residency_ends_tag else "No residency ends"

            location_tag = detail_soup.find('h5', text='Location')
            location = location_tag.find_next('span').get_text(strip=True) if location_tag else "No location"

            more_info_tag = detail_soup.find('h5', text='Link to more information')
            more_info_link = more_info_tag.find_next('a', href=True)['href'] if more_info_tag else "No more info link"

            data.append(
                [title, description, deadline, country, duration, accommodation, disciplines, studio, fees, application_info,
                application_deadline, residency_starts, residency_ends, location, more_info_link])
            sleep(1)  # Задержка между запросами
        except Exception as e:
            logging.error(f"Error processing item: {e}")
            continue

    save_to_csv(output_file, data, ['Title', 'Description', 'Deadline', 'Country', 'Duration', 'Accommodation',
                                    'Disciplines', 'Studio', 'Fees', 'Application_info', 'Application_Deadline', 'Residency starts',
                                    'Residency ends', 'Location', 'More_info_link'
                                    ])
    logging.info(f"Saved ResArtis data to {output_file}")


def parse_curatorspace_opportunities(base_url, output_file):
    """Парсинг сайта https://www.curatorspace.com/opportunities"""
    data = []
    for page_num in range(1, 8):
        url = base_url.format(page_num=page_num)
        logging.info(f"Processing page {page_num}: {url}")
        html = fetch_page(url, headers={'User-Agent': 'Mozilla/5.0'})
        if not html:
            continue

        soup = BeautifulSoup(html, 'html.parser')
        opportunities = soup.find_all('div', class_='media-body')

        for opp in opportunities:
            try:
                title = safe_get_text(opp, 'h4', 'media-heading')
                deadline = safe_get_text(opp, 'strong').replace('Deadline: ', '')
                location_info = safe_get_text(opp, 'p', 'details')
                short_description = safe_get_text(opp, 'p', 'description')
                link = "https://www.curatorspace.com" + opp.find('a', class_='btn-sm btn btn-info')['href']

                data.append([title, deadline, location_info, short_description, link])
                sleep(1)  # Задержка
            except Exception as e:
                logging.error(f"Error processing opportunity: {e}")
                continue

    save_to_csv(output_file, data, ['Title', 'Deadline', 'Location Info', 'Short Description', 'Link'])
    logging.info(f"Saved CuratorSpace data to {output_file}")


def parse_artists_communities(base_url, output_file):
    """Парсинг сайта https://artistcommunities.org/directory/open-calls"""

    data = []
    html = fetch_page(base_url, headers={'User-Agent': 'Mozilla/5.0'})
    if not html:
        logging.error("Failed to fetch the main page.")
        return

    soup = BeautifulSoup(html, 'html.parser')
    links = soup.select('td.views-field-label a')

    for link in links:
        try:
            # Формируем полный URL для страницы деталей
            details_url = urljoin(base_url, link['href'])
            logging.info(f"Processing page {details_url}")

            detail_html = fetch_page(details_url, headers={'User-Agent': 'Mozilla/5.0'})
            if not detail_html:
                logging.warning(f"Failed to fetch details for {details_url}")
                continue

            details_soup = BeautifulSoup(detail_html, 'html.parser')
            title = get_text_or_none(details_soup.select_one('h1'))

            content = details_soup.select_one('.node__content')
            if not content:
                logging.warning(f"No content found for {details_url}")
                continue

            # Извлечение данных с проверкой наличия элементов
            associated_residency = get_text_or_none(
                content.select_one('.field--name-field-associated-residency .field__item a'))
            organization = get_text_or_none(
                content.select_one('.field-pseudo-field--pseudo-group_node\:organization-link-list .field__item a'))
            description = get_text_or_none(
                content.select_one('.field--name-field-oc-residency-description .field__item'))

            deadline = get_text_or_none(content.select_one('.field--name-field-deadline .datetime'))
            application_url = get_text_or_none(content.select_one('.field--name-field-application-url .field__item a'))

            residency_length = get_text_or_none(
                content.select_one('.field--label-inline.field-pseudo-field--pseudo-residency-length .field__item'))
            languages = get_text_or_none(content.select_one('.field--name-field-languages .field__item'))
            avg_num_artists = get_text_or_none(content.select_one('.field--name-field-average-artists .field__item'))
            collaborative_residency = get_text_or_none(
                content.select_one('.field--name-field-collaborative-residency .field__item'))
            disciplines = ', '.join(
                [item.get_text(strip=True) for item in content.select('.field--name-field-discipline .field__item')])

            companions = get_text_or_none(content.select_one('.field--name-field-companions .field__item'))
            country_of_residence = get_text_or_none(
                content.select_one('.field--name-field-country-of-residence .field__item'))
            family_friendly = get_text_or_none(content.select_one('.field--name-field-family-friendly .field__item'))
            stage_of_career = get_text_or_none(content.select_one('.field--name-field-stage-of-career .field__item'))
            additional_expectations = get_text_or_none(
                content.select_one('.field--name-field-additional-expectations .field__item'))

            accessible_housing = get_text_or_none(
                content.select_one('.field--name-field-accessible-housing .field__item'))
            meals_provided = ', '.join([item.get_text(strip=True) for item in
                                        content.select('.field--name-field-meals-provided .field__item')])
            studios_equipment = ', '.join([item.get_text(strip=True) for item in
                                           content.select('.field--name-field-studios-special-equipment .field__item')])
            studios_accessibility = get_text_or_none(
                content.select_one('.field--name-field-studios-accessibility .field__item'))
            type_of_housing = get_text_or_none(content.select_one('.field--name-field-type-of-housing .field__item'))

            additional_eligibility = get_text_or_none(
                content.select_one('.field--name-field-additional-eligibility .field__item'))
            num_artists_accepted = get_text_or_none(
                content.select_one('.field--name-field-number-of-artists-accepted .field__item'))
            total_applicant_pool = get_text_or_none(
                content.select_one('.field--name-field-applicant-pool .field__item'))

            artist_stipend = get_text_or_none(content.select_one('.field--name-field-artist-stipend .field__item'))
            travel_stipend = get_text_or_none(content.select_one('.field--name-field-travel-stipend .field__item'))
            residency_fees = get_text_or_none(content.select_one('.field--name-field-residency-fees .field__item'))
            grant_scholarship_support = get_text_or_none(
                content.select_one('.field--name-field-grant-scholarship .field__item'))
            application_fee = get_text_or_none(content.select_one('.field--name-field-application-fee .field__item'))
            application_type = get_text_or_none(content.select_one('.field--name-field-application-type .field__item'))

            data.append(
                [title, associated_residency, organization, description, deadline, application_url,
                 residency_length, languages, avg_num_artists, collaborative_residency, disciplines,
                 companions, country_of_residence, family_friendly, stage_of_career, additional_expectations,
                 accessible_housing, meals_provided, studios_equipment, studios_accessibility, type_of_housing,
                 additional_eligibility, num_artists_accepted, total_applicant_pool, artist_stipend,
                 travel_stipend, residency_fees, grant_scholarship_support, application_fee, application_type]
            )
            sleep(1)
        except Exception as e:
            logging.error(f"Error processing opportunity: {e}")
            continue

    save_to_csv(output_file, data, [
        'Title', 'Associated Residency Program', 'Organization', 'Description',
        'Deadline', 'Application URL', 'Residency Length', 'Languages',
        'Average Number of Artists', 'Collaborative Residency', 'Disciplines',
        'Companions', 'Country of Residence', 'Family Friendly', 'Stage of Career',
        'Additional Expectations', 'Accessible Housing', 'Meals Provided',
        'Studios/Special Equipment', 'Studios/Facilities Accessibility',
        'Type of Housing', 'Additional Eligibility Information',
        'Number of Artists Accepted', 'Total Applicant Pool', 'Artist Stipend',
        'Travel Stipend', 'Residency Fees', 'Grant/Scholarship Support',
        'Application Fee', 'Application Type'
    ])
    logging.info('Saved Artistcommunities data to CSV')


if __name__ == "__main__":
    # main()
    parse_csv_file('curatorspace_calls.csv')

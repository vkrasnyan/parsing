import openai
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

openai.api_key = 'KEY'


def fetch_page(url, headers):
    """Загружает страницу и возвращает HTML-контент."""
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        return response.content
    except requests.exceptions.RequestException as e:
        logging.error(f"Ошибка при запросе {url}: {e}")
        return None


def extract_data(
        soup,
        tag=None,
        class_=None,
        text=None,
        attribute=None,
        find_next=False,
        default="No data",
        element=None
):
    """
    Универсальная функция для извлечения данных из HTML.

    :param soup: BeautifulSoup объект или HTML-элемент.
    :param tag: Тег для поиска.
    :param class_: Класс элемента.
    :param text: Текст элемента.
    :param attribute: Атрибут для извлечения (например, href).
    :param find_next: Найти следующий элемент (например, span).
    :param default: Значение по умолчанию, если элемент не найден.
    :param element: Конкретный HTML-элемент (если уже найден ранее).
    :return: Извлеченные данные или значение по умолчанию.
    """
    try:
        # Если передан элемент, работаем с ним, иначе используем soup
        search_area = element if element else soup
        found = search_area.find(tag, class_=class_, text=text)

        if find_next and found:
            found = found.find_next('span')

        if attribute and found:
            return found[attribute]

        return found.get_text(strip=True) if found else default
    except Exception:
        return default


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
    """Сохраняет данные в файл в формате csv без повторений"""
    if not data:
        logging.warning("Нет данных для сохранения.")
        return

    df = pd.DataFrame(data)
    df.drop_duplicates(inplace=True)  # Удаление дубликатов
    df.to_csv(file_path, index=False, encoding='utf-8')
    logging.info(f"Файл сохранен по пути: {file_path}")


def decode_spamspan(spamspan_element):
    """Обрабатывает адреса электронной почты"""
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

def get_text_or_none(element):
    return element.get_text(strip=True) if element else 'N/A'

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

def ask_openai(question, prompt_prefix=""):
    """Задает вопрос OpenAI и возвращает ответ."""
    prompt = f"{prompt_prefix}\n\nQuestion: {question}\nAnswer:"
    try:
        response = openai.chat.completions.create(
            model="gpt-4",
            messages=[
                {"role": "system", "content": "You are a helpful assistant."},
                {"role": "user", "content": prompt},
            ],
            max_tokens=4000,
            temperature=1
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        print(f"Ошибка при обращении к OpenAI: {e}")
        return "Error"


def send_post_request(row):
    """Отправляет POST-запрос на указанный URL с данными из строки файла."""
    url = "https://beta.mirr.art/api/open_calls/"
    headers = {
        "Authorization": "KEY",
        "Accept": "application/json"
    }

    data = {
        "city_country": row['City_Country'],
        "open_call_title": row['Open_Call_Title'],
        "deadline_date": row['Deadline_Date'],
        "event_date": row['Event_Date'],
        "application_from_link": row['Application_Form_Link'],
        "selection_criteria": row['Selection_Criteria'],
        "faq": row['FAQ'],
        "fee": row['Fee'],
        "application_guide": row['Application_Guide'],
        "open_call_description": f"Open call in {row['City_Country']} titled {row['Open_Call_Title']}."
    }

    try:
        response = requests.post(url, headers=headers, json=data)
        if response.status_code == 200:
            logging.info(f"Успешно отправлены данные для Open Call: {row['Open_Call_Title']}")
        else:
            logging.error(f"Ошибка при отправке данных для Open Call: {row['Open_Call_Title']}. Статус код: {response.status_code}")
    except Exception as e:
        logging.error(f"Ошибка при отправке POST-запроса: {e}")

def process_csv_and_send_requests(file_path):
    """Читает CSV файл, обрабатывает данные и отправляет POST-запросы по каждой строке."""
    try:
        df = pd.read_csv(file_path)
        print(f"Файл {file_path} успешно загружен.")
    except Exception as e:
        print(f"Ошибка при загрузке файла {file_path}: {e}")
        return

    for _, row in df.iterrows():
        # Задать вопросы OpenAI и обработать строки
        data = " ".join([f"{col}: {str(value)}" for col, value in row.items()])

        city_country = ask_openai(f"Верни на английском языке ТОЛЬКО страну, если указано. Данные: {data}")
        opencall_title = ask_openai(f"Верни на английском языке ТОЛЬКО название опен-колла. Данные: {data}")
        deadline_date = ask_openai(f"Верни на английском языке ТОЛЬКО дату дедлайна в формате YYYY-MM-DD. Данные: {data}")
        event_date = ask_openai(f"Верни на английском языке ТОЛЬКО дату мероприятия. Данные: {data}")
        application_form_link = ask_openai(f"Верни на английском языке ТОЛЬКО ссылку на форму заявки. Данные: {data}")
        selection_criteria = ask_openai(f"Верни на английском языке ТОЛЬКО критерии отбора. Данные: {data}")
        fee = ask_openai(f"Верни на английском языке ТОЛЬКО стоимость участия. Данные: {data}")
        faq = ask_openai(f"Составь FAQ для опен-колла. Данные: {data}")
        application_guide = ask_openai(f"Составь подробный план подачи заявки. Данные: {data}")

        # Сохранить данные в словарь
        processed_data = {
            'City_Country': city_country,
            'Open_Call_Title': opencall_title,
            'Deadline_Date': deadline_date,
            'Event_Date': event_date,
            'Application_Form_Link': application_form_link,
            'Selection_Criteria': selection_criteria,
            'FAQ': faq,
            'Fee': fee,
            'Application_Guide': application_guide
        }

        # Отправка данных на удаленный сервер
        send_post_request(processed_data)

def save_results(results, output_file):
    """Сохраняет результаты в CSV файл."""
    try:
        df_results = pd.DataFrame(results)
        df_results.to_csv(output_file, index=False, encoding='utf-8-sig')
        print(f"Результаты успешно сохранены в {output_file}")
    except Exception as e:
        print(f"Ошибка при сохранении файла: {e}")

def main_process(file_path):
    """Основной процесс обработки CSV и отправки данных."""
    try:
        results = process_csv_and_send_requests(file_path)
        if results:
            save_results(results, '/content/drive/MyDrive/open_calls_ready_2/results.csv')
        else:
            print("Нет данных для сохранения.")
    except Exception as e:
        print(f"Ошибка в процессе выполнения: {e}")


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
            'Heading': extract_data(artopp_element, tag='h3', class_='b_categorical-heading mod--artopps'),
            'Alert': extract_data(artopp_element, tag='p', class_='b_ending-alert mod--just-opened'),
            'Title': extract_data(artopp_element, tag='h2'),
            'Date Updated': extract_data(artopp_element, tag='p', class_='b_date'),
            'Body': extract_data(artopp_element, tag='div', class_='m_body-copy'),
            'URL': extract_data(artopp_element, tag='a', class_='b_submit mod--next', attribute='href', default='')
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
            date = extract_data(row, tag='td', class_='views-field views-field-created')
            content_td = row.find('td', class_='views-field views-field-field-your-ad')

            if content_td:
                title = extract_data(content_td, tag='h2')
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
        WebDriverWait(driver, 10).until(
            EC.presence_of_all_elements_located((By.CLASS_NAME, 'grid__item'))
        )
        html = driver.page_source
        soup = BeautifulSoup(html, 'html.parser')
        items = soup.find_all('div', class_='grid__item postcard')
        logging.info(f"Found {len(items)} elements with class 'grid__item postcard'.")
    except Exception as e:
        logging.error(f"Error: {e}")
        return
    finally:
        driver.quit()

    data = []
    for item in items:
        try:
            link = extract_data(item, tag='a', attribute='href', default=None)
            title = extract_data(item, tag='h2', class_='card__title', default="No title")

            if not link:
                logging.warning(f"No link found for item: {title}")
                continue

            logging.info(f"Fetching details for: {title}")

            # Загружаем HTML-страницу детали
            detail_html = fetch_page(link, headers=None)
            if not detail_html:
                logging.warning(f"Failed to fetch details for {title}.")
                continue

            detail_soup = BeautifulSoup(detail_html, 'html.parser')

            # Извлекаем необходимые данные
            data.append({
                'title': title,
                'description': extract_data(detail_soup, tag='div', class_='entry-content', default="No description"),
                'duration': extract_data(detail_soup, tag='h5', text='Duration of residency', find_next=True),
                'accommodation': extract_data(detail_soup, tag='h5', text='Accommodation', find_next=True),
                'disciplines': extract_data(detail_soup, tag='h5', text='Disciplines, work equipment and assistance',
                                            find_next=True),
                'studio': extract_data(detail_soup, tag='h5', text='Studio / Workspace', find_next=True),
                'fees': extract_data(detail_soup, tag='h5', text='Fees and support', find_next=True),
                'expectations': extract_data(detail_soup, tag='h5', text='Expectations towards the artist',
                                             find_next=True),
                'application_info': extract_data(detail_soup, tag='h5', text='Application information', find_next=True),
                'application_deadline': extract_data(detail_soup, tag='h5', text='Application deadline',
                                                     find_next=True),
                'residency_starts': extract_data(detail_soup, tag='h5', text='Residency starts', find_next=True),
                'residency_ends': extract_data(detail_soup, tag='h5', text='Residency ends', find_next=True),
                'location': extract_data(detail_soup, tag='h5', text='Location', find_next=True),
                'more_info_link': extract_data(detail_soup, tag='h5', text='Link to more information', find_next=False,
                                               attribute='href'),
            })

            logging.info(f"Successfully processed: {title}")
            sleep(1)  # Задержка между запросами
        except Exception as e:
            logging.error(f"Error processing item {title if 'title' in locals() else 'Unknown'}: {e}")
            continue


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
                title = extract_data(opp, tag='h4', class_='media-heading')
                deadline = extract_data(opp, tag='strong').replace('Deadline: ', '')
                location_info = extract_data(opp, tag='p', class_='details')
                short_description = extract_data(opp, tag='p', class_='description')
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
    main()
    # parse_csv_file('/content/drive/MyDrive/curatorspace_calls.csv')
    # добавляем больше файлов для дополнительного парсинга
    # main_process(
    #    '/content/drive/MyDrive/open_calls_ready_2',
    # )
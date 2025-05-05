import time
from concurrent.futures import ThreadPoolExecutor
import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse
from itertools import chain
import re
import csv
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
from gspread_formatting import CellFormat, NumberFormat, format_cell_range
from datetime import datetime
from gspread.cell import Cell
from gspread.utils import rowcol_to_a1

from oauth2client.service_account import ServiceAccountCredentials
from gspread_formatting import format_cell_range, CellFormat, NumberFormat, TextFormat
import gspread
from datetime import datetime

import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime


from datetime import datetime
import gspread
from oauth2client.service_account import ServiceAccountCredentials

def save_to_google_sheet_with_prices_over_time(data, sheet_name="Scraping Output1005", creds_file="credentials.json"):
    # إعداد الاتصال بجوجل شيت
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(creds_file, scope)
    client = gspread.authorize(creds)

    # فتح أو إنشاء الشيت
    try:
        sheet = client.open(sheet_name).sheet1
    except gspread.SpreadsheetNotFound:
        sheet = client.create(sheet_name).sheet1

    existing_data = sheet.get_all_values()
    if not existing_data:
        existing_data = [[]]

    fixed_headers = [
        "unique_url", "match_url", "date", "competition", "Match", "stadium",
        "company", "url", "info", "nights", "type"
    ]

    today = datetime.today().strftime('%d-%m-%Y')
    # today = datetime.today().strftime('02-%m-%Y')

    headers = existing_data[0]
    rows = existing_data[1:]

    # تأكد من وجود الأعمدة المطلوبة
    for header in fixed_headers:
        if header not in headers:
            headers.append(header)

    if today not in headers:
        headers.append(today)
    today_index = headers.index(today)

    # تحديث رؤوس الجدول
    sheet.update([headers], 'A1')

    # إعداد الخلايا التي سيتم تحديثها مرة واحدة
    cells_to_update = []
    updated_rows = []

    unique_index = headers.index("unique_url")
    url_to_row_map = {row[unique_index]: row for row in rows if len(row) > unique_index}

    for match in data:
        unique_url = str(match.get("unique_url", "")).strip()
        if not unique_url:
            continue

        try:
            price = int(match.get("price", ""))
        except (ValueError, TypeError):
            price = ""

        url = match.get("url", "")
        if not url:
            continue

        if unique_url in url_to_row_map:
            row_number = rows.index(url_to_row_map[unique_url]) + 2  # لأن الرأس في الصف 1
            col_number = today_index + 1  # الأعمدة تبدأ من 1
            cells_to_update.append(Cell(row=row_number, col=col_number, value=price))
        else:
            # صف جديد بالكامل
            new_row = []
            for h in headers:
                if h == "unique_url":
                    new_row.append(unique_url)
                elif h == today:
                    new_row.append(price)
                elif h in ["price", "type"]:
                    try:
                        new_row.append(int(match.get(h, "")))
                    except:
                        new_row.append("")
                elif h == "date":
                    date_obj = match.get("date")
                    new_row.append(str(date_obj).strip() if date_obj else "")
                else:
                    new_row.append(str(match.get(h, "")).strip())
            updated_rows.append(new_row)

    # تحديث الخلايا القديمة دفعة واحدة
    if cells_to_update:
        sheet.update_cells(cells_to_update)

    # إضافة الصفوف الجديدة مرة واحدة
    if updated_rows:
        sheet.append_rows(updated_rows)

        # تنسيق الأعمدة الرقمية من type حتى آخر عمود
        try:
            type_index = headers.index("type")
            last_index = len(headers) - 1

            def column_letter(index):
                result = ''
                while index >= 0:
                    result = chr(index % 26 + 65) + result
                    index = index // 26 - 1
                return result

            col_start = column_letter(type_index)
            col_end = column_letter(last_index)
            col_range = f"{col_start}2:{col_end}"

            number_format = CellFormat(
                numberFormat=NumberFormat(type='NUMBER', pattern='0'),
            )
            format_cell_range(sheet, col_range, number_format)
            print(f"🎯 Applied number format to columns: {col_range}")
        except Exception as e:
            print(f"⚠️ Error applying number format: {e}")

    try:
        client.open(sheet_name).share('mostafaemadss21@gmail.com', perm_type='user', role='writer')
    except Exception as e:
        print(f"⚠️ Could not share the sheet: {e}")

    sheet_url = f"https://docs.google.com/spreadsheets/d/{sheet.spreadsheet.id}"
    print(f"🔗 Google Sheet URL: {sheet_url}")
    print("✅ Prices updated in Google Sheet.")



###################################################################################################################
#tools_helper
#remove duplicates from list of dict
def remove_dict_duplicates_keep_order(lst):
    seen = set()
    unique = []
    for d in lst:
        t = tuple(sorted(d.items()))
        if t not in seen:
            seen.add(t)
            unique.append(d)
    return unique

#add go in link
def convert_url(original_url):
    parsed = urlparse(original_url)
    path_parts = parsed.path.strip("/").split("/")
    if len(path_parts) >= 2:
        new_path = f"/go/{path_parts[0]}/{path_parts[1]}/"
        new_url = f"{parsed.scheme}://{parsed.netloc}{new_path}"
        response = None

        for i in range(5):
            try:
                response = requests.get(new_url, allow_redirects=False, timeout=10)
                break
            except :
                time.sleep(1)

        if not response:
            print(f"❌ Failed to connect to: {new_url}")
            return None, new_url

        print(response.headers)
        url_target = response.headers.get('location')
        if "?utm_source=" in str(url_target):
            url_target = url_target.split("?utm_source=")[0]
            print(url_target)
        else:
            print("No ?utm_source= found.")
        return url_target,new_url
    return None, None

#clean_date
month_map = {
    "Jan": "Jan", "Januari": "Jan", "Jänner": "Jan", "Ene": "Jan", "Janvier": "Jan",
    "Feb": "Feb", "Februari": "Feb", "Februar": "Feb", "Febrero": "Feb", "Février": "Feb",
    "Mar": "Mar", "Maret": "Mar", "März": "Mar", "Marzo": "Mar", "Mars": "Mar",
    "Apr": "Apr", "April": "Apr", "Avril": "Apr", "Abril": "Apr",
    "May": "May", "Mei": "May", "Mai": "May", "Mayo": "May",
    "Jun": "Jun", "Juni": "Jun", "Juin": "Jun", "Junio": "Jun",
    "Jul": "Jul", "Juli": "Jul", "Juillet": "Jul", "Julio": "Jul",
    "Aug": "Aug", "Agustus": "Aug", "Aout": "Aug", "Ago": "Aug",
    "Sep": "Sep", "September": "Sep", "Septembre": "Sep", "Septiembre": "Sep",
    "Oct": "Oct", "Oktober": "Oct", "Octobre": "Oct", "Octubre": "Oct", "Okt": "Oct",
    "Nov": "Nov", "November": "Nov", "Novembre": "Nov", "Noviembre": "Nov",
    "Dec": "Dec", "Desember": "Dec", "Dezember": "Dec", "Décembre": "Dec", "Diciembre": "Dec", "Des": "Dec"
}

def clean_date_field(date_str):
    """
    تنظيف وتحويل التاريخ من أي تنسيق معروف إلى dd-mm-yyyy
    """
    # تأكد إن القيمة نص
    date_str = str(date_str).replace("'", "").strip()

    # إزالة الرموز غير المرئية أو الأحرف غير ASCII
    date_str = re.sub(r"[^\x00-\x7F]+", "", date_str)

    # تجاهل الساعات أو التواريخ الزمنية
    if "AM" in date_str or "PM" in date_str or ":" in date_str:
        return ""

    # تطبيع أسماء الشهور غير الإنجليزية
    for non_eng, eng in month_map.items():
        if non_eng in date_str:
            date_str = date_str.replace(non_eng, eng)

    # محاولة التعرف على عدة تنسيقات تاريخ
    date_formats = [
        "%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y", "%d.%m.%Y",
        "%d %b %Y", "%d %B %Y",
        "%Y/%m/%d", "%Y.%m.%d"
    ]


    for fmt in date_formats:
        try:
            dt = datetime.strptime(date_str, fmt)
            # print(f"✅ Parsed Date: {dt.date()}")  # طباعة التواريخ التي تم معالجتها بنجاح

            return dt.date()
        except ValueError:
            continue


    print(f"⚠️ Couldn't parse date: '{date_str}'")
    return date_str









def conver_link_to_json_link(url):
    if url.startswith("https://www.voetbalticket.com/"):
        relative_path = url.replace("https://www.voetbalticket.com/", "")
        json_url = f"https://www.voetbalticket.com/matchjson/{relative_path}all"
        return json_url


def clean_price(price):
    if not price:
        return None
    try:
        # Remove all symbols and keep only digits
        cleaned = price.replace("€", "").replace(",", "").replace(".", "").replace("-", "").replace(" ", "").strip()
        return int(cleaned)
    except Exception as e:
        print("❌ Error cleaning price:", price, e)
        return None
###################################################################################################################





#get data of all team in country and studium
def get_match_data(team_url):
    headers = {"User-Agent": "Mozilla/5.0"}
    response = None

    for i in range(5):
        try:
            response = requests.get(team_url, headers=headers, timeout=10)
            if response.status_code == 200:
                break
            else:
                print(f"⚠️ Attempt {i+1}: status code {response.status_code}")
        except Exception as e:
            print(f"❌ Attempt {i+1} failed for {team_url}: {e}")
        time.sleep(1)

    if not response or response.status_code != 200:
        print(f"❌ Failed to load team page after retries: {team_url}")
        return []

    soup = BeautifulSoup(response.text, 'html.parser')

    matches_data = []

    match_cards = soup.select('a.list-group-item')
    for match in match_cards:
        try:
            url = match.get('href')
            home_team = match.select('span[itemprop="name"]')[0].text.strip()
            away_team = match.select('span[itemprop="name"]')[1].text.strip()
            stadium = match.select_one('[itemprop="location"] span[itemprop="name"]').text.strip()
            competition = match.select('div[itemprop="location"] ~ div span[itemprop="name"]')[-1].text.strip()
            date = match.select_one('.dmonth').text.strip()
            price = match.select_one('[itemprop="price"]').text.strip()

            matches_data.append({
                "url": url,
                "date": date,
                "home_team": home_team,
                "away_team": away_team,
                "stadium": stadium,
                "competition": competition,
                "price": price
            })
        except Exception as e:
            print("Error while parsing match:", e)

    return matches_data
###################################################################################################################
# get info for matchs in feauture
def get_data_teams():
    import time
    teams = []
    url = "https://www.voetbalticket.com/teams/"
    headers = {"User-Agent": "Mozilla/5.0"}

    response = None
    for i in range(5):
        try:
            response = requests.get(url, headers=headers, timeout=10)
            if response.status_code == 200:
                break
            else:
                print(f"⚠️ Attempt {i+1}: Status code {response.status_code}")
        except Exception as e:
            print(f"❌ Attempt {i+1} failed: {e}")
        time.sleep(1)

    if not response or response.status_code != 200:
        print(f"❌ Failed to retrieve team page after retries: {url}")
        return []

    soup = BeautifulSoup(response.text, "html.parser")
    team_links = soup.select('.team-container a[href]')

    for team in team_links:
        href = team.get('href')
        if href:
            teams.append(href.strip())

    print(f"✅ Found {len(teams)} team URLs.")
    return teams
###################################################################################################################
#chcek price is valid
def check_voetbalticketshop(soup):
    price_tag = soup.find("h5", class_="text-primary")
    print(price_tag)

    if price_tag:
        price = price_tag.get_text(strip=True)
        print("✅price:", price)
        return price
    else:
        print("❌Price tag not found.")

def check_voetbalreizenxl(soup):
    price_tag = soup.find("span", class_="price fw-500")
    print(price_tag)
    if price_tag:
        price = price_tag.get_text(strip=True)
        print("price", price)
        return price
    else:
        print("❌Price tag not found.")



def check_find_price(link_company_new,target_company):
    headers = {
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36',
    }
    response = None
    for i in range(5):

        try:
            response = requests.get(link_company_new, headers=headers, timeout=15)
            if response.status_code == 200:
                break
            else:
                print(f"⚠️ Attempt {i + 1}: Status {response.status_code} for {link_company_new}")
        except Exception as e:
            print(f"❌ Attempt {i + 1} failed for {link_company_new}: {e}")
            time.sleep(1)


    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html.parser')
        if target_company=="Voetbalticketshop.nl":
            price=check_voetbalticketshop(soup)
            return price

        elif target_company=="Voetbalreizenxl.nl":
            price= check_voetbalreizenxl(soup)
            return price


###################################################################################################################



def get_all_data(team_page_url):
    def fetch_and_return(url):
        print("Fetching:", url)
        data = get_match_data(url)
        print(data)

        return data

    with ThreadPoolExecutor(max_workers=10) as executor:
        results = list(executor.map(fetch_and_return, team_page_url))

    flat_list = list(chain.from_iterable(results))
    unique_list = remove_dict_duplicates_keep_order(flat_list)

    return unique_list

###################################################################################################################
# # get json data
def get_data_json(all_data):
    all_data_json = []

    def fetch_json(link_ticket):
        url = link_ticket['url']
        json_url = conver_link_to_json_link(url)
        print("Fetching JSON:", json_url)
        try:
            response = requests.get(json_url)
            if response.status_code == 200:
                json_data = response.json()

                if isinstance(json_data, list):
                    for item in json_data:
                        if isinstance(item, dict):
                            item.update({
                                "ulr_match": url,
                                "date": link_ticket.get("date"),
                                "home_team": link_ticket.get("home_team"),
                                "away_team": link_ticket.get("away_team"),
                                "stadium": link_ticket.get("stadium"),
                                "competition": link_ticket.get("competition"),
                            })
                    return json_data
        except Exception as e:
            print("Error fetching:", json_url, e)
        return None

    with ThreadPoolExecutor(max_workers=10) as executor:
        results = list(executor.map(fetch_json, all_data))


    all_data_json = [res for res in results if res is not None]
    flat_list = list(chain.from_iterable(all_data_json))

    return flat_list

###################################################################################################################

#check_domain_price
def check_price_domain_price(data_json_ticket):
    updated_data = []
    target_companies = ["voetbalreizenxl.nl", "voetbalticketshop.nl"]


    def process_price(match):
        company = match.get('company', '')
        match['price'] = clean_price(match['price'])
        if any(target.lower() in company.lower() for target in target_companies):
            print("⛔️ check - matched:", match['company'])
            link_company = match['url']
            link_company_new, id = convert_url(link_company)
            print("➡️ checking price for:", link_company_new)
            print(match['price'])
            new_price = check_find_price(link_company_new, match['company'])
            match['price'] = new_price
            match['price'] = clean_price(new_price)

        processed_match = {
            'company': match.get('company', ''),
            'info': match.get('info', ''),
            'price': match.get('price', ''),
            'nights': match.get('nights', ''),
            'url': convert_url(match.get('url', ''))[0] or '',
            'unique_url': convert_url(match.get('url', ''))[1] or '',
            'type': match.get('type', ''),
            'match_url': match.get('ulr_match', ''),
            'date': match.get('date', ''),
            'Match': f"{match.get('home_team', '').strip()} - {match.get('away_team', '').strip()}",
            'stadium': match.get('stadium', ''),
            'competition': match.get('competition', ''),
        }
        print(processed_match)
        return processed_match

    with ThreadPoolExecutor(max_workers=10) as executor:
        updated_data = list(executor.map(process_price, data_json_ticket))



    save_to_google_sheet_with_prices_over_time(updated_data, sheet_name="Scraping Output1002")












def main():

    team_page_url= get_data_teams()
    all_data=get_all_data(team_page_url)
    data_json_ticket=get_data_json(all_data)
    check_price_domain_price(data_json_ticket)



if __name__ == "__main__":
    main()
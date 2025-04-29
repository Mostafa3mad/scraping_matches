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


from oauth2client.service_account import ServiceAccountCredentials
from gspread_formatting import format_cell_range, CellFormat, NumberFormat, TextFormat
import gspread
from datetime import datetime

def save_to_google_sheet_with_prices_over_time(data, sheet_name="Scraping Output1002", creds_file="credentials.json"):
    # إعداد الاتصال بجوجل شيت
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(creds_file, scope)
    client = gspread.authorize(creds)

    try:
        sheet = client.open(sheet_name).sheet1
    except gspread.SpreadsheetNotFound:
        sheet = client.create(sheet_name).sheet1

    existing_data = sheet.get_all_values()

    # رؤوس الأعمدة الرسمية
    fixed_headers = [
        "match_url", "date", "competition", "Match", "stadium",
        "company", "url", "info", "nights", "type",
    ]

    today = datetime.today().strftime('%d-%m-%Y')
    # today = datetime.today().strftime('30-04-2025')

    if len(existing_data) < 2 or fixed_headers != existing_data[0][:len(fixed_headers)]:
        sheet.clear()
        sheet.update([fixed_headers], 'A1')
        existing_data = sheet.get_all_values()

    headers = existing_data[0]
    rows = existing_data[1:]

    if "url" not in headers or "type" not in headers:
        print("❌ Error: 'url' or 'type' column not found in sheet headers.")
        print("📌 Current Headers:", headers)
        return

    if today not in headers:
        headers.append(today)
        sheet.update([headers], 'A1')

    url_index = headers.index("url")
    type_index = headers.index("type")
    today_index = headers.index(today)

    # بناء مفتاح فريد للصفوف الحالية
    row_map = {}
    for i, row in enumerate(rows):
        if len(row) > max(url_index, type_index):
            key = f"{row[url_index]}__{row[type_index]}"
            row_map[key] = (i + 2, row)  # الصف يبدأ من A2

    updates = []
    new_rows = []

    for match in data:
        url = match.get("url", "")
        ticket_type = match.get("type", "")
        price = match.get("price", "")

        if not url or not ticket_type:
            continue

        key = f"{url}__{ticket_type}"

        # صف موجود: نحدث العمود الجديد
        if key in row_map:
            row_num, existing_row = row_map[key]
            while len(existing_row) < len(headers):
                existing_row.append("")
            cell = gspread.utils.rowcol_to_a1(row_num, today_index + 1)
            updates.append((cell, price))
        else:
            # صف جديد بالكامل
            new_row = []
            for h in headers:
                if h == today:
                    new_row.append(price)
                elif h == "date":
                    cleaned_date = clean_date_field(match.get(h, ""))
                    print(cleaned_date)
                    if re.match(r"\d{2}-\d{2}-\d{4}", cleaned_date):
                        new_row.append(cleaned_date)
                    else:
                        new_row.append("")
                else:
                    new_row.append(match.get(h, ""))
            while len(new_row) < len(headers):
                new_row.append("")
            new_rows.append(new_row)

    if updates:
        cell_list = sheet.range(f"{updates[0][0]}:{updates[-1][0]}")
        for i, cell in enumerate(cell_list):
            cell.value = updates[i][1]
        sheet.update_cells(cell_list)

    if new_rows:
        sheet.append_rows(new_rows)

    # try:
    #     date_col_index = headers.index("date")
    #     col_letter = chr(65 + date_col_index)
    #     format_cell_range(sheet, f"{col_letter}2:{col_letter}", CellFormat(
    #         numberFormat=NumberFormat(type='DATE', pattern='dd-mm-yyyy'),
    #         horizontalAlignment='CENTER',
    #         textFormat=TextFormat(bold=False)
    #     ))
    #     print(f"📅 Column '{col_letter}' formatted as Date.")
    # except Exception as e:
    #     print(f"⚠️ Couldn't apply date format: {e}")

    # مشاركة الشيت مع إيميلك
    client.open(sheet_name).share('mostafaemadss21@gmail.com', perm_type='user', role='writer')

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
        response = requests.get(new_url, allow_redirects=False)
        print(response.headers)
        url_target = response.headers.get('location')
        if "?utm_source=" in str(url_target):
            url_target = url_target.split("?utm_source=")[0]
            print(url_target)
        else:
            print("No ?utm_source= found.")
        return url_target
    return None

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
            return dt.strftime("%d-%m-%Y")
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
    response = requests.get(team_url, headers=headers)

    if response.status_code != 200:
        print("Failed to load team page")
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
    teams=[]
    url = "https://www.voetbalticket.com/teams/"
    headers = {
        "User-Agent": "Mozilla/5.0"
    }
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        soup = BeautifulSoup(response.text, "html.parser")
        team_links = soup.select('.team-container a[href]')
        for team in team_links:
            teams.append(team.get('href'))




        return teams


    else:
        print("failed to retrieve the page")
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

    # response = requests.get(link_company_new, headers=headers, allow_redirects=False)
    # Location=response.headers.get('Location')
    # print(Location)
    response = requests.get(link_company_new, headers=headers)
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html.parser')
        # print(soup)
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
            link_company_new = convert_url(link_company)
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
            'url': convert_url(match.get('url', '')) or '',
            'type': match.get('type', ''),
            'match_url': match.get('ulr_match', ''),
            'date': clean_date_field(match.get('date', '')),
            'Match': f"{match.get('home_team', '').strip()} - {match.get('away_team', '').strip()}",
            'stadium': match.get('stadium', ''),
            'competition': match.get('competition', ''),
        }
        # print(processed_match["date"])
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
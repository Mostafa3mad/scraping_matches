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

import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime


def save_to_google_sheet_with_prices_over_time(data, sheet_name="Scraping Output1005", creds_file="credentials.json"):
    # إعداد الاتصال بجوجل شيت
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(creds_file, scope)
    client = gspread.authorize(creds)

    try:
        sheet = client.open(sheet_name).sheet1
    except gspread.SpreadsheetNotFound:
        sheet = client.create(sheet_name).sheet1

    existing_data = sheet.get_all_values()

    fixed_headers = [
        "id", "match_url", "date", "competition", "Match", "stadium",
        "company", "url", "info", "nights", "type",
    ]

    today = datetime.today().strftime('%d-%m-%Y')
    # today = datetime.today().strftime('01-05-2025')

    # التأكد من أن الشيت يحتوي على الرؤوس الصحيحة
    headers = existing_data[0]  # Get headers from first row
    rows = existing_data[1:]  # All rows excluding the header

    # إذا كانت الرؤوس متوافقة، لا نقوم بمسح الشيت
    if fixed_headers != headers[:len(fixed_headers)]:
        print("❌ Error: Headers in the sheet are not as expected.")
        print("📌 Current Headers:", headers)

        # إضافة الأعمدة المفقودة
        for header in fixed_headers:
            if header not in headers:
                headers.append(header)

        sheet.update([headers], 'A1')  # تحديث الشيت بالرؤوس الجديدة

    # التأكد من وجود عمودي 'id' و 'url'
    if "id" not in headers or "url" not in headers:
        print("❌ Error: 'id' or 'url' column not found in sheet headers.")
        return

    id_index = headers.index("id")  # تحديد الفهرس الخاص بـ 'id'
    url_index = headers.index("url")  # تحديد الفهرس الخاص بـ 'url'

    # إضافة عمود تاريخ اليوم إذا لم يكن موجودًا
    if today not in headers:
        headers.append(today)  # إضافة العمود الخاص بتاريخ اليوم في نهاية الرؤوس
        sheet.update([headers], 'A1')  # تحديث الشيت بالرؤوس الجديدة

    # تحديد الفهرس الخاص بالعمود `today` بعد إضافته
    today_index = headers.index(today)

    # تخزين الصفوف في قاموس باستخدام (id) كمفتاح
    id_to_row = {row[id_index]: row for row in rows if len(row) > id_index}
    updated_rows = []

    for match in data:

        try:
            match_id = int(match.get("id", ""))
        except (ValueError, TypeError):
            continue  # تجاهل الصف إذا الـ id مش رقم

        try:
            price = int(match.get("price", 0))
        except (ValueError, TypeError):
            price = 0  # أو continue لو عايز تتجاهل الصف

        url = match.get("url", "")  # الحصول على الـ url

        if not match_id or not url:
            continue  # تخطي البيانات إذا لم يكن يوجد id أو url

        row = []
        for h in headers:
            if h == today:
                row.append(price)
            elif h == "id":
                try:
                    row.append(int(match.get("id", 0)))
                except:
                    row.append("")
            elif h == "date":
                date_obj = clean_date_field(match.get("date", ""))
                if date_obj:
                    row.append(date_obj.strftime('%d-%m-%Y'))  # تحويل التاريخ إلى نص قبل إضافته
                else:
                    row.append("")
            elif h == "type":
                try:
                    row.append(int(match.get("type", 0)))
                except:
                    row.append("")
            else:
                row.append(match.get(h, ""))

        # التحقق من تطابق 'id' في الصفوف الحالية
        if match_id in id_to_row:
            existing_row = id_to_row[match_id]

            # التأكد من أن الصف يحتوي على العدد الصحيح من الأعمدة
            while len(existing_row) < len(headers):
                existing_row.append("")  # إضافة الخلايا الفارغة لضمان التطابق مع الهيكل

            try:
                existing_row[today_index] = int(price)
            except:
                existing_row[today_index] = ""

            updated_rows.append(existing_row)
        else:
            # إذا لم يتم العثور على الـ id معًا، نقوم بإضافة صف جديد
            while len(row) < len(headers):
                row.append("")  # إضافة الخلايا الفارغة لضمان التطابق مع الهيكل
            updated_rows.append(row)

    # إذا كان هناك أي بيانات محدثة، نقوم بتحديث الشيت
    if updated_rows:
        sheet.update(updated_rows, 'A2')

    # مشاركة الشيت مع بريدك الإلكتروني
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
        id= path_parts[0]+path_parts[1]
        print(id)
        new_url = f"{parsed.scheme}://{parsed.netloc}{new_path}"
        response = requests.get(new_url, allow_redirects=False)
        print(response.headers)
        url_target = response.headers.get('location')
        if "?utm_source=" in str(url_target):
            url_target = url_target.split("?utm_source=")[0]
            print(url_target)
        else:
            print("No ?utm_source= found.")
        return url_target,id
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
            'id': convert_url(match.get('url', ''))[1] or '',
            'type': match.get('type', ''),
            'match_url': match.get('ulr_match', ''),
            'date': clean_date_field(match.get('date', '')),
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
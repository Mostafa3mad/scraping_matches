# 🏟️ Football Ticket Scraper

A fast, multi-threaded Python scraper to extract comprehensive ticket package details from [voetbalticket.com](https://www.voetbalticket.com). Perfect for sports travel analysts, agencies, and fans who want to track ticket prices, availability, and extra services like flights and hotels.

---

## 🚀 Features

- ⚡ **High Performance:** Built with `ThreadPoolExecutor` to fetch data in parallel.
- 🧠 **Smart Parsing:** Extracts key match and offer details from structured and JSON data.
- 📦 **Complete Package Info:** Collects fields like teams, stadium, date, ticket type, price, flight/hotel info, and more.
- 📑 **Clean Output:** Final data is returned as a clean, flat list or exported to CSV.
- 🔎 **Domain-Specific Price Checks:** Can verify real-time prices from selected reseller websites.
- 🔧 **Easy to Extend:** Add more checks, filters, or sources with minimal effort.

---

## 🧪 Sample Output Fields

| Field | Description |
|-------|-------------|
| `url_match` | Match event page |
| `flightincluded` | Flight included flag |
| `url` | Booking page link |
| `flight` | Flight availability (0 or 1) |
| `info` | Ticket type (e.g., E-Tickets) |
| `home_team` | Home team name |
| `away_team` | Away team name |
| `stadium` | Stadium name |
| `competition` | League/competition name |
| `date` | Match date |
| `nights` | Hotel nights included |
| `price` | Package price |
| `company` | Travel company name |
| `companyimg` | Company logo identifier |
| `readmore` | Call to action (e.g., Book now) |
| `buytype`, `type` | Type of ticket bundle |
| `matchticketincluded` | Is match ticket included? |

---

## 📊 Sample CSV Row

```csv
url_match,flightincluded,url,flight,info,home_team,matchticketincluded,competition,nights,date,price,buytype,companyimg,company,readmore,stadium,away_team,type
https://www.voetbalticket.com/arsenal/paris-saint-germain/champions-league/,Vliegticket inbegrepen,https://www.voetbalticket.com/1065421/1/number1-voetbalreizen/arsenal/paris-saint-germain/,0,E-Tickets,Arsenal,Wedstrijdticket inbegrepen,Champions League,,29 Apr 2025,1 079 €,-1,Number1_Voetbalreizen,Number1 Voetbalreizen,Lees meer/Boek,Emirates Stadium,Paris Saint Germain,1
```

---

## 🛠️ Installation

Install the required libraries with:

```bash
pip install -r requirements.txt
```

### `requirements.txt`
```
requests
beautifulsoup4
```

---

## ▶️ How to Run

```bash
python scraping_voetbalticket.py
```

1. Edit the script to load your list of match URLs (or scrape them).
2. Run the script to extract and display/save the results.
3. Optionally, verify selected prices from target domains.

---

## 🤝 Author & Contact

Crafted with love by **Mostafa Emad** 💡

- [🔗 LinkedIn](https://www.linkedin.com/in/mostafa--emad?originalSubdomain=eg)
- [🧑‍💻 Upwork](https://www.upwork.com/freelancers/~0179f2b4933834b31f?mp_source=share)
- [🐦 Twitter / X](https://x.com/mostafa___emad)

---

## 🌐 Use Cases

- Sports analytics platforms
- Travel and ticket aggregation sites
- Monitoring flight and hotel-inclusive packages
- Dynamic competitor price tracking

---

## 🧰 To-Do / Improvements

- [ ] Add headless browser scraping for dynamic JS-rendered prices
- [ ] Add retry mechanism for failed fetches
- [ ] Improve data deduplication and validation
- [ ] Add optional JSON and Excel output formats

---


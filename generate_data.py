"""
Adım 1B: Sentetik kaynak sistem verisi üretici.

Amaç: Borusan grubundaki farklı sistemlerden gelen veriyi taklit etmek.
Her kaynak sistem KENDİ müşteri kaydını tutar (master data yok) ve
bilerek bozuk veri içerir. Silver katmanının değeri bu bozuklukları
temizleyip müşteriyi tek kayda indirmesinden görünecek.

Tüm veri SENTETİKTİR. Gerçek kişi/araç/şirket verisi değildir.
Çalıştır:  python generate_data.py
Çıktı:     data/raw/*.csv  (Bronze katmanın girdisi)
"""
import random
import string
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

SEED = 42
random.seed(SEED)

OUT = Path(__file__).parent / "data" / "raw"
OUT.mkdir(parents=True, exist_ok=True)

N_CUSTOMERS = 2000
TODAY = date(2026, 10, 9)

# ----------------------------------------------------------------------
# Referans listeler
# ----------------------------------------------------------------------
FIRST = ["Ahmet", "Mehmet", "Ayşe", "Fatma", "Mustafa", "Zeynep", "Emre", "Elif",
         "Can", "Deniz", "Burak", "Selin", "Cem", "Esra", "Kemal", "Ceren",
         "Onur", "Gizem", "Murat", "Büşra", "Ozan", "İpek", "Barış", "Yasemin"]
LAST = ["Yılmaz", "Kaya", "Demir", "Şahin", "Çelik", "Yıldız", "Öztürk", "Aydın",
        "Arslan", "Doğan", "Kılıç", "Aslan", "Çetin", "Koç", "Kurt", "Özkan",
        "Şimşek", "Polat", "Korkmaz", "Güneş"]

# (marka, model, olası yakıtlar ve ağırlıkları)
MODELS = {
    "BMW": [("3 Serisi", {"Benzin": 4, "Dizel": 3, "Hibrit": 2, "Elektrik": 1}),
            ("X1", {"Benzin": 4, "Dizel": 3, "Hibrit": 2, "Elektrik": 2}),
            ("iX3", {"Elektrik": 1}),
            ("5 Serisi", {"Benzin": 3, "Dizel": 3, "Hibrit": 3, "Elektrik": 2})],
    "MINI": [("Cooper", {"Benzin": 1, "Elektrik": 9}),
             ("Countryman", {"Benzin": 1, "Elektrik": 9})],
    "Range Rover": [("Evoque", {"Benzin": 3, "Dizel": 3, "Hibrit": 2}),
                    ("Sport", {"Benzin": 3, "Dizel": 3, "Hibrit": 3})],
    "Defender": [("110", {"Benzin": 3, "Dizel": 4, "Hibrit": 2})],
    "Discovery": [("Sport", {"Benzin": 3, "Dizel": 4, "Hibrit": 1})],
    "Jaguar": [("F-Pace", {"Benzin": 3, "Dizel": 3, "Hibrit": 2}),
               ("I-Pace", {"Elektrik": 1})],
}
BRAND_WEIGHTS = {"BMW": 50, "MINI": 22, "Range Rover": 10, "Defender": 7,
                 "Discovery": 5, "Jaguar": 6}

CITIES = ["İstanbul", "Ankara", "İzmir", "Bursa", "Antalya", "Adana", "Konya", "Kocaeli"]
DEALERS = []
for i, city in enumerate(CITIES):
    DEALERS.append({"dealer_code": f"B{i+1:02d}", "dealer_name": f"Borusan Oto {city}",
                    "city": city, "point_type": "Bayi"})
for i, city in enumerate(CITIES + ["İstanbul", "İzmir"]):
    DEALERS.append({"dealer_code": f"S{i+1:02d}", "dealer_name": f"Yetkili Servis {city} {i+1}",
                    "city": city, "point_type": "Servis"})
SALE_DEALERS = [d["dealer_code"] for d in DEALERS if d["point_type"] == "Bayi"]
SERVICE_POINTS = [d["dealer_code"] for d in DEALERS if d["point_type"] == "Servis"]


# ----------------------------------------------------------------------
# Yardımcılar
# ----------------------------------------------------------------------
def wchoice(weights: dict):
    return random.choices(list(weights), weights=list(weights.values()))[0]


def make_vin():
    chars = string.ascii_uppercase.replace("I", "").replace("O", "").replace("Q", "") + string.digits
    return "WBA" + "".join(random.choices(chars, k=14))  # 17 karakter


def make_phone():
    return f"5{random.randint(30, 59)}{random.randint(1000000, 9999999)}"


def rand_date(start: date, end: date) -> date:
    return start + timedelta(days=random.randint(0, (end - start).days))


# --- kirletme fonksiyonları (kaynak sistemlerin gerçekçi hataları) ------
TR_MAP = str.maketrans("ĞÜŞİÖÇğüşıöç", "GUSIOCgusioc")


def dirty_name(name: str) -> str:
    r = random.random()
    if r < 0.55:
        return name
    if r < 0.70:
        return name.upper()
    if r < 0.80:
        return name.lower()
    if r < 0.90:
        return name.translate(TR_MAP)          # Türkçe karakter kaybı
    if r < 0.96:
        return "  " + name.replace(" ", "  ") + " "   # fazla boşluk
    first, last = name.split(" ", 1)
    return f"{last} {first}"                   # soyad-ad ters


def dirty_phone(p: str) -> str:
    r = random.random()
    if r < 0.40:
        return p
    if r < 0.60:
        return "0" + p
    if r < 0.75:
        return f"+90{p}"
    if r < 0.90:
        return f"0 ({p[:3]}) {p[3:6]} {p[6:8]} {p[8:]}"
    return ""                                   # eksik


def dirty_date(d: date) -> str:
    r = random.random()
    if r < 0.70:
        return d.strftime("%Y-%m-%d")
    if r < 0.92:
        return d.strftime("%d.%m.%Y")
    return d.strftime("%d/%m/%Y")


def dirty_vin(v: str) -> str:
    r = random.random()
    if r < 0.93:
        return v
    if r < 0.96:
        return ""                               # boş VIN
    if r < 0.98:
        return v[:-1]                           # 16 karakter
    return v.lower()


def dirty_brand(b: str) -> str:
    r = random.random()
    if r < 0.90:
        return b
    if r < 0.95:
        return b.upper() + " "
    return b.lower()


# ----------------------------------------------------------------------
# Gerçek (gizli) müşteri ve araç evreni
# ----------------------------------------------------------------------
customers = []
used = set()
for cid in range(1, N_CUSTOMERS + 1):
    while True:
        name = f"{random.choice(FIRST)} {random.choice(LAST)}"
        phone = make_phone()
        if (name, phone) not in used:
            used.add((name, phone))
            break
    customers.append({"true_id": cid, "name": name, "phone": phone,
                      "city": random.choice(CITIES)})

vehicles = []
for c in customers:
    n_veh = wchoice({1: 70, 2: 25, 3: 5})
    for _ in range(n_veh):
        brand = wchoice(BRAND_WEIGHTS)
        model, fuels = random.choice(MODELS[brand])
        fuel = wchoice(fuels)
        sale_date = rand_date(date(2019, 1, 1), date(2026, 9, 1))
        vehicles.append({"vin": make_vin(), "brand": brand, "model": model, "fuel": fuel,
                         "sale_date": sale_date, "owner": c})

# ----------------------------------------------------------------------
# Kaynak 1: DMS satış sistemi (Borusan Oto)
# ----------------------------------------------------------------------
sales = []
for i, v in enumerate(vehicles, 1):
    base = {"BMW": 2_800_000, "MINI": 2_100_000, "Range Rover": 7_500_000,
            "Defender": 6_500_000, "Discovery": 5_800_000, "Jaguar": 4_500_000}[v["brand"]]
    price = int(base * random.uniform(0.85, 1.4))
    sales.append({
        "sale_id": f"S{i:06d}",
        "musteri_adi": dirty_name(v["owner"]["name"]),
        "telefon": dirty_phone(v["owner"]["phone"]),
        "sasi_no": dirty_vin(v["vin"]),
        "marka": dirty_brand(v["brand"]),
        "model": v["model"],
        "yakit_tipi": v["fuel"],
        "satis_tarihi": dirty_date(v["sale_date"]),
        "bayi_kodu": random.choice(SALE_DEALERS),
        "satis_fiyati_tl": price,
    })
df_sales = pd.DataFrame(sales)
# mükerrer satırlar (~1.5%)
df_sales = pd.concat([df_sales, df_sales.sample(frac=0.015, random_state=SEED)],
                     ignore_index=True)

# ----------------------------------------------------------------------
# Kaynak 2: Borusan Otomotiv Premium (kiralama/finans/kasko)
# ----------------------------------------------------------------------
contracts = []
cid_seq = 1
for v in vehicles:
    if random.random() < 0.55:  # araçların yarısından fazlasında bir sözleşme var
        ctype = wchoice({"Kiralama": 35, "Finans": 40, "Kasko": 25})
        start = v["sale_date"] + timedelta(days=random.randint(0, 20))
        months = random.choice([12, 24, 36, 48])
        end = start + timedelta(days=30 * months)
        if random.random() < 0.02:           # hata: bitiş < başlangıç
            end = start - timedelta(days=random.randint(1, 60))
        contracts.append({
            "sozlesme_no": f"PRM{cid_seq:06d}",
            "musteri_unvani": dirty_name(v["owner"]["name"]),
            "iletisim_tel": dirty_phone(v["owner"]["phone"]),
            "vin": dirty_vin(v["vin"]),
            "sozlesme_tipi": ctype,
            "baslangic": dirty_date(start),
            "bitis": dirty_date(end),
            "aylik_tutar_tl": int(random.uniform(18_000, 95_000)) if ctype != "Kasko"
                              else int(random.uniform(3_000, 15_000)),
        })
        cid_seq += 1
df_contracts = pd.DataFrame(contracts)

# ----------------------------------------------------------------------
# Kaynak 3: Servis sistemi
# ----------------------------------------------------------------------
services = []
sid = 1
for v in vehicles:
    years = max((TODAY - v["sale_date"]).days / 365, 0.1)
    n_visits = int(years * random.uniform(0.3, 1.6))
    for _ in range(n_visits):
        sdate = rand_date(v["sale_date"], TODAY)
        # EV'lerin servis maliyeti ortalamada daha düşük (varsayım)
        mean_cost = 7_500 if v["fuel"] == "Elektrik" else 12_500
        cost = max(500, int(random.gauss(mean_cost, mean_cost * 0.4)))
        services.append({
            "servis_id": f"SRV{sid:07d}",
            "sasi": dirty_vin(v["vin"]),
            "sahip_adi": dirty_name(v["owner"]["name"]),
            "servis_tarihi": dirty_date(sdate),
            "servis_noktasi": random.choice(SERVICE_POINTS),
            "km": int(max(500, (TODAY - v["sale_date"]).days * random.uniform(20, 55))),
            "tutar_tl": cost,
            "is_tipi": wchoice({"Periyodik Bakım": 55, "Arıza": 30, "Kaporta": 10, "Lastik": 5}),
        })
        sid += 1
df_services = pd.DataFrame(services)

# ----------------------------------------------------------------------
# Kaynak 4: Borusan Next (takas / ikinci el)
# ----------------------------------------------------------------------
trades = []
tid = 1
for v in vehicles:
    age_years = (TODAY - v["sale_date"]).days / 365
    if age_years > 2 and random.random() < 0.18:
        tdate = rand_date(v["sale_date"] + timedelta(days=700), TODAY)
        deprec = 0.55 if v["fuel"] == "Elektrik" else 0.70   # varsayım: EV daha hızlı değer kaybeder
        trades.append({
            "islem_no": f"NXT{tid:06d}",
            "vin_no": dirty_vin(v["vin"]),
            "musteri": dirty_name(v["owner"]["name"]),
            "islem_tarihi": dirty_date(tdate),
            "islem_tipi": wchoice({"Takas": 55, "Nakit Alım": 30, "İhale": 15}),
            "teklif_tl": int(random.uniform(1_000_000, 3_500_000) * deprec),
        })
        tid += 1
df_trades = pd.DataFrame(trades)

df_dealers = pd.DataFrame(DEALERS)

# ----------------------------------------------------------------------
# Yaz
# ----------------------------------------------------------------------
files = {
    "dms_satis.csv": df_sales,
    "premium_sozlesme.csv": df_contracts,
    "servis_kayit.csv": df_services,
    "next_islem.csv": df_trades,
    "bayi_servis_noktalari.csv": df_dealers,
}
for fname, df in files.items():
    df.to_csv(OUT / fname, index=False, encoding="utf-8")
    print(f"{fname:28s} {len(df):>7,} satır")

# Doğrulama için gizli gerçek eşleşme (Silver'ın doğruluğunu ölçmekte kullanılacak)
truth = pd.DataFrame([{"vin": v["vin"], "true_customer_id": v["owner"]["true_id"],
                       "true_name": v["owner"]["name"]} for v in vehicles])
(OUT.parent / "ground_truth").mkdir(exist_ok=True)
truth.to_csv(OUT.parent / "ground_truth" / "vin_to_customer.csv", index=False)
print(f"\nGerçek müşteri sayısı: {N_CUSTOMERS:,}  |  gerçek araç sayısı: {len(vehicles):,}")

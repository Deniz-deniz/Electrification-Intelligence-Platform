# 7 Dakikalık Yönetici & Teknik Demo Akışı (Demo Script)

> **Proje:** Electrification Intelligence Platform (Borusan Otomotiv Grubu)  
> **Konsept:** Sentetik veri üzerinde uçtan uca çalışan prototip (local implementation; maps to Fabric, not tested on Fabric)  
> **Hedef Kitle:** Veri Mühendisliği Liderleri, Satış & Satış Sonrası Direktörleri, Mülakat Paneli  
> **Süre:** 7 Dakika  
> **Dil:** Türkçe  
> **Temel İlke:** %100 Çevrimdışı, Sıfır Dış Bağımlılık, Canlı Terminal ve Web Arayüzü

---

## 🎯 SİSTEMİN GERÇEK ÇIKTILARINDAN ÜRETİLEN KRİTİK METRİKLER

Sunum ve demo sırasında aşağıdaki metrikleri canlı olarak doğrulamak için `python -m pipeline.key_numbers` komutunu çalıştırabilirsiniz:

1. **Toplanan Ham Veri Hacmi (Bronze):**  
   $$\mathbf{13.247\text{ Satır}}$$  
   *(DMS Satış: 2.732, Premium Sözleşme: 1.445, Servis: 8.690, Next İkinci El: 362, Bayi Ağı: 18)*
2. **Karantinaya Alınan Ret Kayıtları (Silver Quarantine):**  
   $$\mathbf{738\text{ Tekil Satır}}\quad (\mathbf{741\text{ Kural İhlal Olayı}},\ \text{Ret Oranı: }\mathbf{\%5,57})$$  
   *(Not: Aradaki 3 olaylık fark, `dms_satis` tablosundaki mükerrer `sale_id` olup aynı zamanda eksik/hatalı VIN içeren 3 satırdan kaynaklanır; telefon bir ret kuralı değildir).*
3. **Müşteri Eşleme (Entity Resolution) Doğruluğu (Sentetik Zemin Gerçeği):**  
   $$\text{Precision: }\mathbf{\%100,00}\quad\mid\quad\text{Recall: }\mathbf{\%82,68}\quad\mid\quad\text{F1: }\mathbf{\%90,52}\quad\mid\quad\text{Küme Saflığı: }\mathbf{\%100,00}$$  
   *(Kesin kural: "Asla sadece isimle birleştirme". Karantinadaki kayıtlar değerlendirmeye dahil değildir. Muhafazakar eşleme sebebiyle recall %82,68'dir, yani bazı müşteriler bölünmüş olabilir; bu nedenle 1.057 aday sayısı bir üst sınırdır).*
4. **Veri Kalitesi ve Referans Bütünlüğü (Dürüst Mühendislik):**  
   - **Bronze Satış Geçerliliği:** %97,33  
   - **Karantina Ret Oranı:** %5,57 (738 tekil satır / 741 olay)  
   - **Yetim Kayıt Oranları (Satışta Bulunamayan):** Servis %5,52 (454 satır), Sözleşme %5,00 (68 satır), Takas %6,09 (21 satır)  
   *(Önemli Not: Silver geçerliliği %100'dür ancak bu kurgusal bir sonuçtur — sadece geçerli satırlar Silver katmanına alındığı için tanım gereği %100'dür ve bir kalite başarısı olarak sunulmamalıdır).*
5. **Önceliklendirilen EV Dönüşüm Fırsat Havuzu:**  
   $$\mathbf{1.057\text{ Nitelikli Aday}}\quad (\text{80-100: }\mathbf{7},\ \text{60-79: }\mathbf{231},\ \text{40-59: }\mathbf{398},\ \text{0-39: }\mathbf{421})$$  
   *(Yakıt tipi bilinmeyen ve doğrulanması gereken müşteri kuyruğu: **102 müşteri**).*

---

## ⏱️ Dakika Dakika Canlı Demo Akış Planı

```
[0:00 - 1:00]  Bölüm 1: Giriş & Kurumsal Problem (3 Slayt / Sözel)
[1:00 - 2:30]  Bölüm 2: Terminal & Medallion Boru Hattı Çalıştırması
[2:30 - 4:30]  Bölüm 3: Web Arayüzü — Fırsat Radarı & Müşteri 360 Çekmecesi
[4:30 - 5:30]  Bölüm 4: Web Arayüzü — Yönetim Kokpiti & Inline SVG Grafikler
[5:30 - 6:30]  Bölüm 5: Web Arayüzü — Veri Sağlığı & Karantina & Eşleme Kalitesi
[6:30 - 7:00]  Bölüm 6: Veri Kataloğu, Power BI Semantik Modeli & Kapanış
```

---

### [0:00 - 1:00] Bölüm 1: Problem Tanımı ve Giriş

**Konuşma Metni:**
> *"Merhaba. Bugün sizlere Borusan Otomotiv Grubu ekosistemini modelleyen **Electrification Intelligence Platform** projesini canlı olarak sunacağım.*
>
> *Borusan Oto satış ve servis ağını, Borusan Otomotiv Premium kiralama ve finansman operasyonlarını, Borusan Next ise ikinci el takas operasyonlarını yönetiyor. Ancak bu sistemler arasında **ortak bir müşteri kimliği bulunmuyor**.*
>
> *DMS satışlarında telefon var ama isimler ters girilmiş; servis kayıtlarında 8.600 işlem var ama telefon kolonu hiç yok; Premium sözleşmelerinde ise tarih formatları tutarsız.*
>
> *Amacımız: Bu verileri güvenilir bir Medallion Lakehouse mimarisinde birleştirmek, sıfır veri kaybıyla temizlemek ve satış danışmanlarımızın önüne elektrikli araca geçişe en hazır **1.057 nitelikli müşteriyi** çıkaran bir Fırsat Radarı sunmak."*

---

### [1:00 - 2:30] Bölüm 2: Terminalde Boru Hattının Çalıştırılması

**Komutlar (Terminalde çalıştırın):**
```bash
# 1. Uçtan uca göl boru hattını çalıştırın (5 katman, idempotent)
python -m pipeline.run_all

# 2. Üretilen çıktılardan kesin metrikleri ekrana yazdırın
python -m pipeline.key_numbers
```

**Ekranda Gösterilecekler ve Vurgulanacak Noktalar:**
1. **Katman 1 (Bronze):** 5 operasyonel kaynaktan **13.247 satır** raw parquet olarak OneLake uyumlu yapıya alındı.
2. **Katman 2A (Silver Clean & Reconciliation):**
   - Ekranda çıkan **Reconciliation Tablosunu** gösterin:
   - *"Bakınız, hiçbir satır sessizce düşürülmedi. `Rows In = Rows Out + Rows Rejected` denklemi her tabloda `YES` veriyor. Toplam **738 tekil satır / 741 kural ihlali** `rejected_records` karantina masasına taşındı. Aradaki 3 olay farkı, hem mükerrer `sale_id` hem de hatalı/eksik VIN içeren 3 satırdan kaynaklanmaktadır; telefon bir ret kuralı değildir."*
3. **Katman 2B (Silver MDM Entity Resolution):**
   - *"Burada deterministik graph matching çalışıyor. 'Asla sadece isimle birleştirme' kuralımız sayesinde Precision **%100,00**, yani sıfır hatalı birleştirme elde ettik."*
4. **Katman 3 (Gold Aggregations):** 1.057 EV fırsatı, bayi EV penetrasyonu ve servis benchmarkları hesaplandı.
5. **Katman 4 & 5 (Catalog & Power BI):** 20 tablo kataloglandı ve Power BI için UTF-8 BOM CSV dışa aktarımı tamamlandı.
6. **Key Numbers Çıktısı:** `python -m pipeline.key_numbers` çıktısını göstererek tüm sayıların diskteki dosyalardan doğrudan ve canlı olarak doğrulandığını vurgulayın.

---

### [2:30 - 4:30] Bölüm 3: Fırsat Radarı Demosu (Web UI)

**Eylem:** Tarayıcıda `http://localhost:8000/app/` adresini açın (veya sunucuyu `uvicorn api.main:app --port 8000` ile başlatın).

**Tıklama ve Gösterim Sırası:**
1. **Header Alanı:**
   - Üstteki **`⚠️ SENTETİK VERİ`** ve **`🔒 KVKK UYUMLU / SIFIR PII`** rozetlerini işaret edin.
   - *"Uygulama hiçbir CDN veya dış API'ye bağımlı olmadan, tamamen yerel ve KVKK uyumlu maskelenmiş verilerle çalışmaktadır."*
2. **Skor Bantları (Clickable Filter Chips):**
   - **`80 – 100 Puan (En Yüksek Öncelik) 7`** çipine tıklayın.
   - Tablonun anında sadece en kritik 7 adayı filtrelediğini gösterin.
   - Ardından **`Tüm Skorlar 1.057`** çipine basarak filtreyi sıfırlayın.
3. **Yakıt Doğrulama Rozeti:**
   - `has_unknown_vehicle` uyarısı olan satırdaki **`⚠️ Yakıt bilgisi doğrulanmalı`** rozetini gösterin:
   - *"Bu müşterinin bilinen dizel aracının yanında servis kayıtlarında yakıtı eksik bir aracı daha var; danışmana açık uyarı veriyoruz."*
4. **Satır Tıklaması ve Yan Çekmece ("Bu Müşteriyle Neden İletişime Geçilmeli?"):**
   - En üstteki müşteriye (`C001064`, Skor: 80) tıklayın. Sağdan çekmece açılır.
   - **Satış Danışmanı Aksiyon Özeti** kutusunu okuyun:
     > *"Sözleşmesi 170 gün önce bitmiş, araç 4.5 yaşında, 16 aydır servise girmemiş ve BMW portföyünde doğrudan elektrikli i4/i5 alternatifi mevcut. Danışmana önerilen aksiyon: Özel test sürüşü ve Borusan Next takas ön teklifi."*
   - Çekmecedeki kapatma tuşuna basın.
5. **İkincil Panel (Doğrulama Bekleyen Müşteriler):**
   - Sayfayı aşağı kaydırın, turuncu çerçeveli paneli gösterin:
   - *"Yakıt tipi tamamen bilinmeyen **102 müşteriyi** fırsat listesinden ayırdık; yakıtı teyit edilmeden bu müşterileri EV adayı olarak adlandıramayız."*

---

### [4:30 - 5:30] Bölüm 4: Yönetim Paneli (Management Cockpit)

**Eylem:** Üst menüden **`📊 Yönetim Paneli`** sekmesine tıklayın.

**Tıklama ve Gösterim Sırası:**
1. **KPI Kartları:** Ortalama Skor 44,8, Genel EV Satış Payı %23,4, Yıllık Servis Tasarrufu ~4.170 TL.
2. **Metodolojik Not Kutusu:**
   - Sarı uyarı kutusunu gösterin:
   - *"Burada açıkça belirtiyoruz: Bu servis ve takas kıyaslamaları sentetik modelleme varsayımıdır; gerçek saha bulgusu olarak lanse edilmemiştir."*
3. **Inline SVG Grafikler:**
   - **Bayi Bazında EV Payı:** Antalya (%48), İstanbul (%47) sütunlarının üzerine gelin. Harici kütüphane olmadan saf SVG ile çizildiğini vurgulayın.
   - **Servis Maliyeti Karşılaştırması:** BMW, MINI ve Jaguar için elektrikli araçların yıllık bakım masrafının içten yanmalıya göre yaklaşık %40 daha ucuz olduğunu gösterin.

---

### [5:30 - 6:30] Bölüm 5: Veri Sağlığı & Yönetişim (Data Health)

**Eylem:** Üst menüden **`🛡️ Veri Sağlığı`** sekmesine tıklayın.

**Tıklama ve Gösterim Sırası:**
1. **Başlık KPI'ları:**
   - *"Bakınız, Silver geçerliliğini %100 diye vitrine koymadık; çünkü sadece geçerli satırlar Silver katmanına alındığı için tanım gereği %100'dür ve bir kalite başarısı değildir. Dürüst mühendislik gereği **Bronze Satış Geçerliliği %97,33**, **Karantina Ret Oranı %5,57** ve **Yetim Oranları (Servis %5,52, Sözleşme %5,00, Takas %6,09)** metriklerini öne çıkardık."*
2. **Karantina Analizi (Olaylar vs. Tekil Satırlar):**
   - **741 Olay vs. 738 Tekil Satır** kutusunu gösterin:
   - *"Tekil satır sayısı 738, toplam ihlal olayı ise 741'dir. Aradaki 3 olay farkı, `dms_satis` tablosunda hem mükerrer `sale_id` hem de eksik/geçersiz VIN içeren 3 satırdan kaynaklanmaktadır (telefon bir ret kuralı değildir)."*
3. **Referans Bütünlüğü Tablosu:**
   - Servis kayıtlarındaki 454 yetim satırın (%5,52), satış kaydı karantinaya alınan 117 araçtan kaynaklandığını açıklayın.
4. **Müşteri Eşleme Kutusu:**
   - Kırmızı uyarı notunu okuyun:
   > *"Karantinadaki kayıtlar değerlendirmeye dahil değildir; recall %82,68, yani bazı müşteriler birden fazla kayda bölünmüş olabilir. Bu sebeple 1.057 sayısı bir üst sınırdır. Ancak precision %100'dür."*

---

### [6:30 - 7:00] Bölüm 6: Veri Kataloğu, Power BI & Kapanış

**Eylem:** Üst menüden **`📖 Veri Kataloğu & Metrikler`** sekmesine tıklayın.

**Tıklama ve Gösterim Sırası:**
1. **Canlı Arama:** Arama kutusuna `opportunity` yazın; hem `ev_opportunities` tablosu hem de `opportunity_score` formülü listelenir.
2. **Formül Şeffaflığı:** Formül kutusundaki açık puanlama matematiğini gösterin.
3. **Power BI Entegrasyonu:**
   - *"Tüm Gold tabloları ve maskelenmiş boyutlar `data/export/powerbi/` dizinine **UTF-8 with BOM** CSV olarak aktarılmıştır. Power BI Desktop'a doğrudan aktarılmaya hazırdır."*
4. **Kapanış Cümlesi:**
   > *"Özetle: Dağınık 5 kaynaktan başlayan süreci, sıfır veri kaybı, matematiksel doğrulanmış müşteri kimliği ve satış danışmanına doğrudan aksiyon aldıran modern bir web kokpitiyle tamamladık. Dinlediğiniz için teşekkür ederim, sorularınızı yanıtlamaktan memnuniyet duyarım."*

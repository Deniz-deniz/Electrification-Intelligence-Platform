# Electrification Intelligence Platform — 10 Slaytlık Yönetici Sunum Taslağı

> **Proje:** Electrification Intelligence Platform (Borusan Otomotiv Grubu)  
> **Konsept:** Sentetik veri üzerinde uçtan uca çalışan prototip (local implementation; maps to Fabric, not tested on Fabric)  
> **Format:** 10 Slaytlık Kurumsal & Teknik Sunum  
> **Dil:** Türkçe  
> **Hedef Kitle:** CDO, CIO, Veri Mühendisliği Direktörleri, Satış Liderleri

---

## 📑 Slayt Dökümü ve İçerik Rehberi

---

### SLAYT 1: Kurumsal Problem & İş İhtiyacı
* **Başlık:** Çok İştirakli Otomotiv Dağıtımında Veri Parçalanmışlığı ve Elektrifikasyon Baskısı
* **Alt Başlık:** Borusan Oto, Borusan Otomotiv Premium ve Borusan Next Arasında Entegre Müşteri Görünürlüğü İhtiyacı
* **Temel Maddeler:**
  - **Silo Veri Yapıları:** Yetkili satıcı/servis ağı (DMS), filo kiralama/finansman (Premium) ve ikinci el takas (Next) operasyonlarının bağımsız veri tabanları kullanması.
  - **Ortak Müşteri Anahtarının Eksikliği:** Sistemler arasında paylaşılan bir tekil müşteri kimliği (Master Customer ID) bulunmaması.
  - **Operasyonel Veri Gürültüsü:** İsim-soyisim tersliği, yazım hataları, eksik telefonlar (servis tablosunda telefon kolonu yok) ve mantıksız sözleşme tarihleri.
  - **Elektrifikasyon Dönüşüm Hedefleri:** Grup markalarında (BMW, MINI, Jaguar) artan elektrikli araç hedeflerine karşın satış danışmanlarının sözleşmesi biten müşterileri önceden tespit edememesi.
* **Sunucu Notu:** *"Veri ambarı öncesinde en büyük operasyonel maliyet, bir müşterinin birden fazla iştirakle temas etmesine rağmen şirket içinde yabancı gibi muamele görmesidir."*

---

### SLAYT 2: Çözüm Vizyonu & Değer Önerisi
* **Başlık:** Electrification Intelligence Platform: Bütünleşik Müşteri 360 & Fırsat Radarı
* **Alt Başlık:** Kural Tabanlı Varlık Çözümleme ve Tahmine Dayalı Dönüşüm Motoru
* **Temel Maddeler:**
  - **Tekil Gerçeklik Kaynağı (Single Source of Truth):** 4 farklı kaynaktan beslenen, Müşteri ve Şasi (VIN) odaklı birleşik veri katmanı.
  - **Akıllı Fırsat Radarı (Opportunity Radar):** Sözleşme bitişine kalan gün, araç yaşı, bakım masrafı ve elektrikli model eşleşmesine göre önceliklendirilen dinamik aday listesi.
  - **Sıfır Sessiz Kayıp Garantisi:** Hatalı kayıtların çöpe atılmayıp şeffaf neden kodlarıyla karantinaya alındığı kurumsal yönetişim masası.
  - **KVKK & Sıfır-PII Standartı:** Tüm sunum, API ve arayüz katmanlarında %100 maskelenmiş kişisel veriler.
* **Sunucu Notu:** *"Danışmanın önüne yüzlerce sayfalık rapor değil, 'Bu hafta araman gereken en sıcak 7 müşteri ve neden araman gerektiğinin gerekçesi' konulmaktadır."*

---

### SLAYT 3: Sistem Mimarisi & Microsoft Fabric Eşleşmesi
* **Başlık:** Medallion Lakehouse Mimarisi: Yerel Gerçekleştirmeden Buluta 1:1 Eşleme
* **Alt Başlık:** Raw $\rightarrow$ Bronze $\rightarrow$ Silver $\rightarrow$ Gold Katman Akışı
* **Temel Maddeler:**
  - **Bronze Katmanı (OneLake Raw):** Kaynak CSV'lerin değişmeden, alım zaman damgası ve satır karması (hash) ile Parquet olarak saklanması.
  - **Silver Katmanı (Cleansed Delta):** Format standardizasyonu, `rejected_records` karantina masası ve kural tabanlı Varlık Çözümleme (MDM).
  - **Gold Katmanı (Direct Lake Semantic Marts):** `ev_opportunities`, `ev_share_by_dealer`, `service_cost_comparison` analitik modelleri.
  - **Teslim Katmanı:** FastAPI REST API, 4 sekmeli çevrimdışı Web Kokpiti ve UTF-8 BOM Power BI dışa aktarımı.
* **Görsel / Şema:** Medallion mimari veri akış diyagramı (Bkz. `docs/fabric_mapping.md`).
* **Önemli Not:** *Yerel gerçekleştirme (Python, PyArrow, FastAPI); Microsoft Fabric ile kavramsal olarak eşleşir, Fabric üzerinde test edilmemiştir.*
* **Sunucu Notu:** *"Yerel ortamda Python, PyArrow ve FastAPI ile sıfır bulut maliyetiyle inşa edilen mimari, Microsoft Fabric Lakehouse bileşenleriyle kavramsal olarak 1:1 örtüşmektedir; henüz canlı Fabric üzerinde test edilmemiştir."*

---

### SLAYT 4: Alan Modeli & Ana Veri Yönetimi (MDM)
* **Başlık:** Müşteri ve Araç Boyutlarının Yıldız Şema (Star-Schema) Tasarımı
* **Alt Başlık:** Şasi Numarası (VIN) Merkezli Çoklu İlişki Ağı
* **Temel Maddeler:**
  - **`dim_customers` (Altın Müşteri Kaydı):** Benzersiz `customer_id`, maskeli isim/telefon ve kaynak sistem listesi.
  - **`dim_vehicles` (Araç Boyutu):** Şasi no (`vin`), marka, model, yakıt tipi ve araç sahibi müşteri anahtarı.
  - **`dim_dealers` (Yetkili Ağ Boyutu):** Bayi kodu, bayi adı, şehir ve hizmet tipi.
  - **Olgu Tabloları (Facts):** Satış işlemleri, finans/kiralama sözleşmeleri, servis iş emirleri ve takas ekspertizleri.
* **Sunucu Notu:** *"Araç şasi numarası (VIN), mülkiyet zincirini ve servis sadakatini iştirakler arasında bağlayan en güçlü fiziksel çıpadır."*

---

### SLAYT 5: Veri Kalitesi & Karantina Yönetişimi
* **Başlık:** Sıfır Sessiz Kayıp İlkesi: Şeffaf Karantina ve Referans Bütünlüğü
* **Alt Başlık:** Hatalı Veriyi Silmek Yerine Neden Kodlarıyla İzole Etmek
* **Temel Maddeler:**
  - **Dürüst Veri Kalitesi Ölçümü:**
    - **Bronze Satış Geçerliliği:** %97,33 (Ham verideki kural denetimi).
    - **Karantina Ret Oranı:** %5,57 (13.247 satırdan 738 tekil satır / 741 ihlal olayı karantinaya alındı).
    - **Çoklu İhlal Satırları:** Aradaki 3 olay farkı, `dms_satis` tablosunda hem mükerrer `sale_id` hem de eksik/geçersiz VIN içeren 3 satırdan kaynaklanır (telefon bir ret kuralı değildir).
    - **Referans Bütünlüğü (Yetim Oranları):** Servis %5,52 (454 satır), Sözleşme %5,00 (68 satır), Takas %6,09 (21 satır). Bu yetim kayıtlar, satış kaydı karantinaya alınan 117 araçtan kaynaklanmaktadır.
    - **Silver Geçerliliği:** %100'dür ancak bu kurgusal bir sonuçtur (sadece geçerli satırlar Silver'a alındığı için tanım gereği %100'dür ve bir kalite başarısı değildir).
  - **Kesin Mutabakat (Reconciliation):** $\text{Rows In} = \text{Rows Out} + \text{Distinct Rows Rejected}$ dengesi tüm tablolarda %100 sağlandı.
* **Sunucu Notu:** *"Gerçek mühendislik kalitesi, veriyi zorla temizleyip hatayı gizlemek değil; hatanın kök nedenini kaynak sisteme geri besleyecek karantina izi bırakmaktır."*

---

### SLAYT 6: Varlık Çözümleme (Entity Resolution) Sonuçları
* **Başlık:** Muhafazakar Müşteri Eşleme: "Asla Sadece İsimle Birleştirme"
* **Alt Başlık:** Sentetik Zemin Gerçeği (Ground Truth) Doğrulama Skorları
* **Temel Maddeler:**
  - **Eşleme Kuralı:** Paylaşılan geçerli VIN (Güçlü bağ), Paylaşılan geçerli telefon + benzer isim (Güçlü bağ), İsim benzerliği + ortak araç/telefon (Zayıf bağ). Sadece isim benzerliğiyle birleştirme yasaktır.
  - **Eşleme Başarı Metrikleri:**
    - **Pairwise Precision: %100,00** (Sıfır Yanlış Birleştirme / Zero False Positive).
    - **Pairwise Recall: %82,68** (Karantinadaki araçlar hariç).
    - **Harmonik Ortalama (F1-Skoru): %90,52**
    - **Küme Saflığı (Cluster Purity): %100,00**
  - **Dürüstlük Notu:** Karantinadaki kayıtlar değerlendirmeye dahil değildir. Muhafazakar eşleme sebebiyle recall %82,68'dir; yani bazı müşteriler bölünmüş olabilir. Bu sebeple 1.057 EV fırsatı bir **üst sınırdır**.
* **Sunucu Notu:** *"Türkiye'de 'Ahmet Yılmaz' gibi yaygın isimleri sadece isme bakarak birleştirirseniz, satış danışmanınız bir müşterinin aracını tamamen yabancı birine teklif eder. %100 Precision tercihimizin nedeni budur."*

---

### SLAYT 7: Teslim Katmanı: REST API & Veri Kataloğu
* **Başlık:** Kurumsal Tüketim Katmanı: FastAPI, Purview Uyumluluğu ve Power BI
* **Alt Başlık:** RFC 8259 Standart JSON ve UTF-8 BOM Destekli Semantik Dışa Aktarım
* **Temel Maddeler:**
  - **REST API:** `/health`, `/catalog/tables`, `/catalog/metrics`, `/gold/opportunities`, `/quality` uç noktaları.
  - **JSON Güvenliği:** Pandas `NaN`/`NaT` değerleri RFC 8259 uyumlu `null` tipine çevrilerek istemci çökmeleri engellendi.
  - **Purview Uyumlu Veri Kataloğu:** 20 tablonun şeması, sahipliği ve veri soybağı (lineage) diskten otomatik taranır (`catalog.json`).
  - **Power BI Dışa Aktarımı:** Gold ve maskeli boyut tabloları Windows uyumlu `utf-8-sig` CSV olarak hazırlanmıştır.
* **Sunucu Notu:** *"Veri gölünde kalan veri değer üretmez. Platform, veriyi hem operasyonel web uygulamasına hem kurumsal Power BI raporlarına anında servis eder."*

---

### SLAYT 8: Fırsat Radarı & Web Arayüzü Demosu
* **Başlık:** Opportunity Radar: Danışman ve Yönetim İçin Karar Destek Kokpiti
* **Alt Başlık:** Sıfır Dış Bağımlılık (No CDN / No LLM), %100 Çevrimdışı Tek Sayfa Uygulaması (SPA)
* **Temel Maddeler:**
  - **Sekme 1 (Fırsat Radarı):** 1.057 aday, tıklanabilir skor çipleri (`0-39`, `40-59`, `60-79`, `80-100`), marka/bayi filtreleri.
  - **Açılır Çekmece:** *"Bu Müşteriyle Neden İletişime Geçilmeli?"* başlığı altında kural tabanlı, deterministik Türkçe satış tavsiyesi.
  - **Yakıt Doğrulama Kuyruğu:** Yakıtı bilinmeyen 102 müşterinin operasyonel doğrulama paneli.
  - **Sekme 2 (Yönetim):** Saf SVG ile çizilmiş bayi EV payı ve servis tasarrufu benchmark grafikleri.
* **Sunucu Notu:** *(Burada 7 dakikalık canlı web arayüzü demosu gerçekleştirilir; Bkz. `docs/demo_script.md`).*

---

### SLAYT 9: İş Değeri & Finansal Getiri Modeli (ROI Modeli)
* **Başlık:** Ölçülebilir İş Değeri ve Gelir Potansiyeli Hesaplama Modeli
* **Alt Başlık:** Parametrik ve Şeffaf Varsayımlara Dayalı Dönüşüm Formülü

> [!IMPORTANT]
> **Metodolojik & Dürüstlük İlkesi:** *Bu çalışmada uydurma/hayali ciro rakamları kullanılmamıştır. Modeldeki EV servis maliyeti ve takas tutma oranları sentetik veri üretecinin varsayımlarıdır, gerçek saha bulgusu değildir. İş değeri, her kurumun kendi birim ekonomisine göre uyarlayabileceği aşağıdaki parametrik formülle hesaplanır:*

#### Gelir Potansiyeli Formülü:
$$\text{Toplam Ek Gelir Potansiyeli} = \Big[ \big( N_{\text{opp}} \times C_{\text{rate}} \big) \times \big( M_{\text{EV}} + \omega_{\text{trade}} \cdot M_{\text{Next}} \big) \Big] + \Big( N_{\text{exp90}} \times R_{\text{churn}} \times LTV \Big)$$

#### Model Parametreleri ve Doldurulabilir Alanlar:
| Parametre | Modeldeki Değeri | Açıklama / Kurumsal Varsayım | Değer Girişi |
|---|---|---|---|
| **$N_{\text{opp}}$** | **1.057 Müşteri** | Fırsat Radarı havuzundaki toplam nitelikli ICE sahibi sayısı. | *[Sabit: 1.057]* |
| **$N_{\text{exp90}}$** | **~238 Müşteri** | Sözleşmesi 90 gün içinde bitecek / yeni bitmiş yüksek riskli müşteri havuzu. | *[Sabit: Veriden]* |
| **$C_{\text{rate}}$** | *Varsayım (%X)* | Proaktif temas sonrası elektrikli araca dönüşüm oranı. *(Örn. %8 - %15)* | `[   ]%` |
| **$M_{\text{EV}}$** | *Varsayım (TL)* | Satılan yeni elektrikli araç başına ortalama brüt kâr katkısı. | `[   ] TL` |
| **$\omega_{\text{trade}}$**| *Varsayım (%Y)* | Takas desteğiyle Borusan Next'e kazandırılan araç oranı. *(Örn. %40)* | `[   ]%` |
| **$M_{\text{Next}}$** | *Varsayım (TL)* | Alınan takas aracının Borusan Next satışından elde edilen marj. | `[   ] TL` |
| **$R_{\text{churn}}$** | *Varsayım (%Z)* | Proaktif teklifle rakip markaya müşteri kaybının (churn) önlenme oranı. | `[   ]%` |
| **$LTV$** | *Varsayım (TL)* | Müşterinin grup ekosisteminde kalmasıyla korunan 3 yıllık yaşam boyu değeri. | `[   ] TL` |

* **Sunucu Notu:** *"Platformun sunduğu 1.057 adayın sadece %10'u bile dönüştürülse, 105 yeni EV satışı ve en az 40 ikinci el takas girişi kurumsal ölçekte somut kâr katkısı yaratacaktır."*

---

### SLAYT 10: Gelecek Yol Haritası (Strategic Roadmap)
* **Başlık:** Faz 2 Vizyonu: Sentetik Veri Üzerinde Uçtan Uca Çalışan Prototipten Kurumsal Bulut Dağıtımına Geçiş
* **Alt Başlık:** Yerel Prototip Sınırlarından Microsoft Fabric ve Azure Altyapısına Dağıtım Adımları
* **Temel Maddeler:**
  1. **Gerçek Zamanlı CDC Bağlantıları:** SAP, Turkuaz ve Salesforce sistemlerinden Microsoft Fabric Data Factory ile anlık veri akışı.
  2. **Telematik & Şarj Ağı Entegrasyonu:** Araç IoT telematikleri ve kamu şarj ağları (Trugo, Eşarj) verileriyle günlük sürüş profili skorlaması.
  3. **Olay Güdümlü Mobil Uyarılar:** Sözleşmesi 90 günün altına inen VIP müşteriler için satış danışmanının Teams / WhatsApp hesabına anlık bildirim düşmesi.
  4. **Güvenli LLM Semantik Katmanı:** Beyaz listeli API/SQL fonksiyonları üzerinden yöneticilere doğal Türkçe dille soru sorma imkanı sunan halüsinasyonsuz AI katmanı.
  5. **Çoklu Marka Genişlemesi:** Portföye eklenebilecek yeni distribütörlük markalarının tak-çalıştır şema adaptörleriyle sisteme dahil edilmesi.
* **Sunucu Notu:** *"Mimari, bugünün verisini temizlemekle kalmayıp yarının bağlantılı araç ve yapay zeka vizyonunu taşıyacak modülerlikte tasarlanmıştır."*

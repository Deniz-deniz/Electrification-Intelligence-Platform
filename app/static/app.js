/**
 * Electrification Intelligence Platform - Vanilla SPA Controller
 * 100% Offline, Zero external CDNs, Pure JavaScript.
 */

document.addEventListener("DOMContentLoaded", () => {
  // Global State
  const state = {
    currentTab: "radar",
    radar: {
      allOpportunities: [],
      filteredOpportunities: [],
      selectedBand: "all",
      selectedBrand: "",
      selectedDealer: "",
      minScore: 0,
      searchQuery: "",
      sortBy: "score_desc",
      page: 1,
      pageSize: 25,
      scoreBands: { "0-39": 421, "40-59": 398, "60-79": 231, "80-100": 7 },
      totalLeads: 1057,
    },
    verificationQueue: {
      items: [],
      total: 102,
      page: 1,
      pageSize: 10,
    },
    management: {
      loaded: false,
      evShareData: [],
      serviceCostData: [],
      tradeInData: [],
    },
    dataHealth: {
      loaded: false,
      qualityData: null,
      refIntegrityData: [],
      dqScoresData: [],
    },
    catalog: {
      loaded: false,
      tables: [],
      metrics: [],
      activeLayerFilter: "all",
      searchQuery: "",
    },
  };

  // ==============================================================================
  // 1. Tab Navigation & Routing
  // ==============================================================================
  const navTabs = document.querySelectorAll(".nav-tab");
  const tabContents = document.querySelectorAll(".tab-content");

  const autoOpenDrawer = window.location.hash.includes("drawer") || window.location.search.includes("drawer");

  function switchTab(tabId) {
    state.currentTab = tabId;
    navTabs.forEach((btn) => {
      btn.classList.toggle("active", btn.dataset.tab === tabId);
    });
    tabContents.forEach((sec) => {
      sec.classList.toggle("active", sec.id === `tab-${tabId}`);
    });
    if (!autoOpenDrawer) {
      window.location.hash = tabId;
    }

    // Trigger on-demand loads
    if (tabId === "management" && !state.management.loaded) {
      loadManagementTab();
    } else if (tabId === "data-health" && !state.dataHealth.loaded) {
      loadDataHealthTab();
    } else if (tabId === "catalog" && !state.catalog.loaded) {
      loadCatalogTab();
    }
  }

  navTabs.forEach((btn) => {
    btn.addEventListener("click", () => switchTab(btn.dataset.tab));
  });

  // Handle URL hash on init
  const initialHash = window.location.hash.replace("#", "");
  if (initialHash.includes("drawer") || initialHash === "radar-drawer") {
    switchTab("radar");
  } else if (["radar", "management", "data-health", "catalog"].includes(initialHash)) {
    switchTab(initialHash);
  }

  // ==============================================================================
  // 2. Tab 1: Fırsat Radarı (Opportunity Radar)
  // ==============================================================================
  const tbodyRadar = document.getElementById("opportunities-tbody");
  const radarTotalCountEl = document.getElementById("radar-total-count");
  const radarShownCountEl = document.getElementById("radar-shown-count");
  const activeFilterBadgeEl = document.getElementById("active-filter-badge");

  const filterBrand = document.getElementById("filter-brand");
  const filterDealer = document.getElementById("filter-dealer");
  const filterMinScore = document.getElementById("filter-min-score");
  const minScoreVal = document.getElementById("min-score-val");
  const filterSearch = document.getElementById("filter-search");
  const btnResetFilters = document.getElementById("btn-reset-filters");
  const sortSelect = document.getElementById("sort-select");

  const btnRadarPrev = document.getElementById("btn-radar-prev");
  const btnRadarNext = document.getElementById("btn-radar-next");
  const radarPageInfo = document.getElementById("radar-page-info");

  // Score Chips
  const chipBtns = document.querySelectorAll(".score-chips-container .chip");
  chipBtns.forEach((chip) => {
    chip.addEventListener("click", () => {
      chipBtns.forEach((c) => c.classList.remove("active"));
      chip.classList.add("active");
      state.radar.selectedBand = chip.dataset.band;
      state.radar.page = 1;
      applyRadarFilters();
    });
  });

  // Filter Listeners
  filterBrand.addEventListener("change", (e) => {
    state.radar.selectedBrand = e.target.value;
    state.radar.page = 1;
    applyRadarFilters();
  });

  filterDealer.addEventListener("change", (e) => {
    state.radar.selectedDealer = e.target.value;
    state.radar.page = 1;
    applyRadarFilters();
  });

  filterMinScore.addEventListener("input", (e) => {
    state.radar.minScore = parseInt(e.target.value, 10);
    minScoreVal.textContent = state.radar.minScore;
    state.radar.page = 1;
    applyRadarFilters();
  });

  filterSearch.addEventListener("input", (e) => {
    state.radar.searchQuery = e.target.value.trim().toLowerCase();
    state.radar.page = 1;
    applyRadarFilters();
  });

  sortSelect.addEventListener("change", (e) => {
    state.radar.sortBy = e.target.value;
    state.radar.page = 1;
    applyRadarFilters();
  });

  btnResetFilters.addEventListener("click", () => {
    filterBrand.value = "";
    filterDealer.value = "";
    filterMinScore.value = "0";
    minScoreVal.textContent = "0";
    filterSearch.value = "";
    sortSelect.value = "score_desc";
    state.radar.selectedBand = "all";
    state.radar.selectedBrand = "";
    state.radar.selectedDealer = "";
    state.radar.minScore = 0;
    state.radar.searchQuery = "";
    state.radar.sortBy = "score_desc";
    state.radar.page = 1;

    chipBtns.forEach((c) => c.classList.toggle("active", c.dataset.band === "all"));
    applyRadarFilters();
  });

  // Pagination Listeners
  btnRadarPrev.addEventListener("click", () => {
    if (state.radar.page > 1) {
      state.radar.page--;
      renderRadarTable();
    }
  });

  btnRadarNext.addEventListener("click", () => {
    const maxPage = Math.ceil(state.radar.filteredOpportunities.length / state.radar.pageSize) || 1;
    if (state.radar.page < maxPage) {
      state.radar.page++;
      renderRadarTable();
    }
  });

  // Fetch opportunities from API
  async function loadOpportunities() {
    try {
      const res = await fetch("/gold/opportunities?limit=1500");
      if (!res.ok) throw new Error("API hatası: " + res.status);
      const data = await res.json();

      state.radar.allOpportunities = data.opportunities || [];
      state.radar.totalLeads = data.total_leads || 1057;
      state.radar.scoreBands = data.score_bands || state.radar.scoreBands;

      radarTotalCountEl.textContent = state.radar.totalLeads.toLocaleString("tr-TR");

      // Update chip counts
      document.getElementById("chip-count-all").textContent = state.radar.totalLeads.toLocaleString("tr-TR");
      document.getElementById("chip-count-80-100").textContent = state.radar.scoreBands["80-100"] || 7;
      document.getElementById("chip-count-60-79").textContent = state.radar.scoreBands["60-79"] || 231;
      document.getElementById("chip-count-40-59").textContent = state.radar.scoreBands["40-59"] || 398;
      document.getElementById("chip-count-0-39").textContent = state.radar.scoreBands["0-39"] || 421;

      applyRadarFilters();

      if (autoOpenDrawer && state.radar.allOpportunities.length > 0) {
        openSideDrawer(state.radar.allOpportunities[0].customer_id);
      }
    } catch (err) {
      tbodyRadar.innerHTML = `<tr><td colspan="9" class="text-center text-danger py-4">Veri yüklenemedi: ${err.message}</td></tr>`;
    }
  }

  function applyRadarFilters() {
    let list = [...state.radar.allOpportunities];

    // Score band filter
    if (state.radar.selectedBand !== "all") {
      if (state.radar.selectedBand === "80-100") {
        list = list.filter((item) => item.opportunity_score >= 80);
      } else if (state.radar.selectedBand === "60-79") {
        list = list.filter((item) => item.opportunity_score >= 60 && item.opportunity_score < 80);
      } else if (state.radar.selectedBand === "40-59") {
        list = list.filter((item) => item.opportunity_score >= 40 && item.opportunity_score < 60);
      } else if (state.radar.selectedBand === "0-39") {
        list = list.filter((item) => item.opportunity_score < 40);
      }
    }

    // Brand filter
    if (state.radar.selectedBrand) {
      list = list.filter((item) => item.brand && item.brand.toLowerCase() === state.radar.selectedBrand.toLowerCase());
    }

    // Dealer filter
    if (state.radar.selectedDealer) {
      list = list.filter((item) => item.dealer_name && item.dealer_name.toLowerCase().includes(state.radar.selectedDealer.toLowerCase()));
    }

    // Min Score
    if (state.radar.minScore > 0) {
      list = list.filter((item) => item.opportunity_score >= state.radar.minScore);
    }

    // Search Query
    if (state.radar.searchQuery) {
      const q = state.radar.searchQuery;
      list = list.filter((item) => {
        const id = (item.customer_id || "").toLowerCase();
        const name = (item.name_masked || "").toLowerCase();
        const vin = (item.vin || "").toLowerCase();
        const model = (item.model || "").toLowerCase();
        return id.includes(q) || name.includes(q) || vin.includes(q) || model.includes(q);
      });
    }

    // Sorting
    list.sort((a, b) => {
      switch (state.radar.sortBy) {
        case "score_desc":
          return b.opportunity_score - a.opportunity_score;
        case "score_asc":
          return a.opportunity_score - b.opportunity_score;
        case "days_asc":
          return (a.days_to_contract_end ?? 9999) - (b.days_to_contract_end ?? 9999);
        case "age_desc":
          return (b.vehicle_age_years ?? 0) - (a.vehicle_age_years ?? 0);
        default:
          return b.opportunity_score - a.opportunity_score;
      }
    });

    state.radar.filteredOpportunities = list;
    radarShownCountEl.textContent = list.length.toLocaleString("tr-TR");

    // Active filter badge
    if (state.radar.selectedBand !== "all" || state.radar.selectedBrand || state.radar.minScore > 0 || state.radar.searchQuery || state.radar.selectedDealer) {
      activeFilterBadgeEl.style.display = "inline-flex";
      activeFilterBadgeEl.textContent = `Aktif Filtre: ${list.length} / ${state.radar.totalLeads}`;
    } else {
      activeFilterBadgeEl.style.display = "none";
    }

    renderRadarTable();
  }

  function renderRadarTable() {
    const list = state.radar.filteredOpportunities;
    const page = state.radar.page;
    const size = state.radar.pageSize;
    const totalPages = Math.ceil(list.length / size) || 1;

    btnRadarPrev.disabled = page <= 1;
    btnRadarNext.disabled = page >= totalPages;
    radarPageInfo.textContent = `Sayfa ${page} / ${totalPages} (${list.length} kayıt)`;

    const start = (page - 1) * size;
    const pageItems = list.slice(start, start + size);

    if (pageItems.length === 0) {
      tbodyRadar.innerHTML = `<tr><td colspan="9" class="text-center py-4 text-muted">Filtrelere uygun fırsat kaydı bulunamadı.</td></tr>`;
      return;
    }

    let html = "";
    pageItems.forEach((lead) => {
      // Score Tier
      let tierClass = "score-tier-4";
      if (lead.opportunity_score >= 80) tierClass = "score-tier-1";
      else if (lead.opportunity_score >= 60) tierClass = "score-tier-2";
      else if (lead.opportunity_score >= 40) tierClass = "score-tier-3";

      // Days Left formatting
      let daysHtml = "-";
      if (lead.days_to_contract_end !== null && lead.days_to_contract_end !== undefined) {
        const days = Math.round(lead.days_to_contract_end);
        if (days < 0) {
          daysHtml = `<span class="badge badge-warning" title="Sözleşme ${Math.abs(days)} gün önce sona erdi">${days} gün</span>`;
        } else if (days <= 90) {
          daysHtml = `<span class="badge badge-danger" title="Acil: 90 gün içinde bitiyor">${days} gün</span>`;
        } else if (days <= 180) {
          daysHtml = `<span class="badge badge-warning" title="180 gün içinde bitiyor">${days} gün</span>`;
        } else {
          daysHtml = `<span class="text-muted">${days} gün</span>`;
        }
      }

      // Reasons Badges (show first 2, +N more)
      const reasons = lead.score_reasons || [];
      let reasonsHtml = "";
      reasons.slice(0, 2).forEach((r) => {
        reasonsHtml += `<span class="reason-tag" title="${r}">${r}</span>`;
      });
      if (reasons.length > 2) {
        reasonsHtml += `<span class="reason-tag" style="background-color:#e2e8f0;font-weight:600;" title="${reasons.join(' | ')}">+${reasons.length - 2}</span>`;
      }

      // Unknown Vehicle Badge
      let unknownVehicleBadge = "";
      if (lead.has_unknown_vehicle === true) {
        unknownVehicleBadge = `<div class="badge-unknown-fuel" title="Bu müşterinin yakıt tipi henüz bilinmeyen başka bir aracı mevcuttur.">⚠️ Yakıt bilgisi doğrulanmalı</div>`;
      }

      html += `
        <tr data-customer-id="${lead.customer_id}">
          <td>
            <div class="cust-cell">
              <span class="cust-masked-name">${lead.name_masked || "İsimsiz"}</span>
              <span class="cust-id-code">${lead.customer_id}</span>
            </div>
          </td>
          <td>
            <strong>${lead.brand || ""}</strong> ${lead.model || ""}
            ${unknownVehicleBadge}
          </td>
          <td><span class="badge badge-outline">${lead.fuel_type || "Bilinmiyor"}</span></td>
          <td>${lead.contract_type || "-"}</td>
          <td>${lead.contract_end_date || "-"}</td>
          <td>${daysHtml}</td>
          <td>
            <div class="score-badge-pill ${tierClass}" title="Fırsat Skoru: ${lead.opportunity_score} / 100">
              ${lead.opportunity_score}
            </div>
          </td>
          <td>
            <div class="reasons-tags-container">
              ${reasonsHtml}
            </div>
          </td>
          <td style="text-align: center;">
            <button class="btn btn-xs btn-outline btn-inspect" data-customer-id="${lead.customer_id}">
              İncele
            </button>
          </td>
        </tr>
      `;
    });

    tbodyRadar.innerHTML = html;

    // Attach row click listeners for side drawer
    tbodyRadar.querySelectorAll("tr").forEach((row) => {
      row.addEventListener("click", () => {
        const custId = row.dataset.customerId;
        openSideDrawer(custId);
      });
    });
  }

  // ==============================================================================
  // 3. Side Drawer: "Bu Müşteriyle Neden İletişime Geçilmeli?"
  // ==============================================================================
  const sideDrawer = document.getElementById("side-drawer");
  const drawerBackdrop = document.getElementById("drawer-backdrop");
  const btnCloseDrawer = document.getElementById("btn-close-drawer");

  const drawerCustName = document.getElementById("drawer-cust-name");
  const drawerCustId = document.getElementById("drawer-cust-id");
  const drawerCustPhone = document.getElementById("drawer-cust-phone");
  const drawerCustVehicle = document.getElementById("drawer-cust-vehicle");
  const drawerScoreCircle = document.getElementById("drawer-score-circle");
  const drawerPitchContent = document.getElementById("drawer-pitch-content");
  const drawerReasonsList = document.getElementById("drawer-reasons-list");
  const drawerDetailsGrid = document.getElementById("drawer-details-grid");

  function openSideDrawer(customerId) {
    const lead = state.radar.allOpportunities.find((x) => x.customer_id === customerId);
    if (!lead) return;

    drawerCustName.textContent = lead.name_masked || "Maskeli Müşteri";
    drawerCustId.textContent = lead.customer_id;
    drawerCustPhone.textContent = lead.phone_masked || "Belirtilmemiş";
    drawerCustVehicle.textContent = `${lead.brand} ${lead.model} (${lead.fuel_type})`;
    drawerScoreCircle.textContent = lead.opportunity_score;

    // Deterministic Rule-Based Pitch in Turkish
    let pitchHtml = "";
    const days = lead.days_to_contract_end !== null ? Math.round(lead.days_to_contract_end) : null;

    pitchHtml += `<p><strong>Sözleşme & Yenileme Zamanı:</strong> `;
    if (days !== null && days <= 90 && days >= 0) {
      pitchHtml += `Müşterinin ${lead.contract_type || "Finans/Kiralama"} sözleşmesi ${lead.contract_end_date} tarihinde (<strong>${days} gün içinde</strong>) sona ermektedir. Sözleşme bitmeden yeni bir elektrikli modele geçiş teklifi sunmak için en kritik penceredir.</p>`;
    } else if (days !== null && days < 0 && days >= -180) {
      pitchHtml += `Müşterinin ${lead.contract_type || "Finans/Kiralama"} sözleşmesi <strong>${Math.abs(days)} gün önce</strong> sona ermiştir. Müşteri halen araç yenileme kararı aşamasında olup yüksek dönüşüm potansiyeline sahiptir.</p>`;
    } else {
      pitchHtml += `Sözleşme durumu takip edilmekle birlikte araç döngüsü yenileme periyodundadır.</p>`;
    }

    if (lead.vehicle_age_years) {
      pitchHtml += `<p><strong>Araç Yaşı & Kilometre:</strong> Mevcut ${lead.brand} ${lead.model} aracı <strong>${lead.vehicle_age_years.toFixed(1)} yaşında</strong> olup kurumsal filo ve bireysel değiştirme ortalamasının üzerindedir.</p>`;
    }

    if (lead.months_since_last_service) {
      pitchHtml += `<p><strong>Servis ve Bakım Baskısı:</strong> Müşteri son <strong>${lead.months_since_last_service.toFixed(1)} aydır</strong> servise girmemiştir. Bakım geçmişindeki harcama yükü, sıfır motor yağı/filtre bakım masrafı sunan elektrikli araçlar için güçlü bir tasarruf argümanıdır.</p>`;
    }

    pitchHtml += `<p><strong>Model Alternatifi:</strong> ${lead.brand} portföyünde müşterinin segmentine birebir karşılık gelen tam elektrikli modeller (örn. ${getElectricModelRecommendation(lead.brand)}) hazır stokta bulunmaktadır.</p>`;

    pitchHtml += `<p style="margin-top:8px;padding-top:8px;border-top:1px dashed #bfdbfe;"><strong>Önerilen Aksiyon:</strong> Müşteriye özel elektrikli test sürüş randevusu oluşturulmalı ve mevcut aracına Borusan Next bünyesinde cazip takas ön değerlemesi sunulmalıdır.</p>`;

    drawerPitchContent.innerHTML = pitchHtml;

    // Reasons List
    const reasons = lead.score_reasons || [];
    let reasonsHtml = "";
    reasons.forEach((r) => {
      reasonsHtml += `<li>${r}</li>`;
    });
    drawerReasonsList.innerHTML = reasonsHtml;

    // Details Grid
    drawerDetailsGrid.innerHTML = `
      <div class="detail-item">
        <div class="detail-label">Şasi No (VIN)</div>
        <div class="detail-val">${lead.vin}</div>
      </div>
      <div class="detail-item">
        <div class="detail-label">Yakıt Tipi</div>
        <div class="detail-val">${lead.fuel_type}</div>
      </div>
      <div class="detail-item">
        <div class="detail-label">Araç Yaşı</div>
        <div class="detail-val">${lead.vehicle_age_years ? lead.vehicle_age_years.toFixed(2) + " Yıl" : "-"}</div>
      </div>
      <div class="detail-item">
        <div class="detail-label">Sözleşme Tipi</div>
        <div class="detail-val">${lead.contract_type || "-"}</div>
      </div>
      <div class="detail-item">
        <div class="detail-label">Sözleşme Bitiş</div>
        <div class="detail-val">${lead.contract_end_date || "-"} (${days !== null ? days + " gün" : "-"})</div>
      </div>
      <div class="detail-item">
        <div class="detail-label">Son Servis</div>
        <div class="detail-val">${lead.last_service_date || "-"} (${lead.months_since_last_service ? lead.months_since_last_service.toFixed(1) + " ay önce" : "-"})</div>
      </div>
      <div class="detail-item" style="grid-column: 1 / -1;">
        <div class="detail-label">Yetkili Satıcı / Bayi</div>
        <div class="detail-val">${lead.dealer_name || "Borusan Oto Yetkili Ağı"}</div>
      </div>
    `;

    sideDrawer.classList.add("active");
    drawerBackdrop.classList.add("active");
  }

  function closeSideDrawer() {
    sideDrawer.classList.remove("active");
    drawerBackdrop.classList.remove("active");
  }

  btnCloseDrawer.addEventListener("click", closeSideDrawer);
  drawerBackdrop.addEventListener("click", closeSideDrawer);

  function getElectricModelRecommendation(brand) {
    switch (brand) {
      case "BMW":
        return "BMW i4, BMW i5, BMW iX1";
      case "MINI":
        return "MINI Cooper SE, MINI Countryman E";
      case "Jaguar":
        return "Jaguar I-PACE";
      case "Range Rover":
      case "Defender":
      case "Discovery":
        return "Range Rover Electric / Plug-in Hybrid alternatifleri";
      default:
        return "Borusan Otomotiv EV Serisi";
    }
  }

  // ==============================================================================
  // 4. Secondary Panel: Fuel Type Verification Queue
  // ==============================================================================
  const tbodyVerif = document.getElementById("verification-tbody");
  const btnVerifPrev = document.getElementById("btn-verif-prev");
  const btnVerifNext = document.getElementById("btn-verif-next");
  const verifPageInfo = document.getElementById("verif-page-info");

  async function loadVerificationQueue() {
    try {
      const res = await fetch("/gold/fuel-verification-queue?limit=150");
      if (!res.ok) throw new Error("API hatası: " + res.status);
      const data = await res.json();
      state.verificationQueue.items = data.items || [];
      state.verificationQueue.total = data.total_count || 102;

      renderVerificationQueue();
    } catch (err) {
      tbodyVerif.innerHTML = `<tr><td colspan="6" class="text-center text-danger py-3">Kuyruk yüklenemedi: ${err.message}</td></tr>`;
    }
  }

  function renderVerificationQueue() {
    const list = state.verificationQueue.items;
    const page = state.verificationQueue.page;
    const size = state.verificationQueue.pageSize;
    const totalPages = Math.ceil(list.length / size) || 1;

    btnVerifPrev.disabled = page <= 1;
    btnVerifNext.disabled = page >= totalPages;

    const start = (page - 1) * size;
    const pageItems = list.slice(start, start + size);
    verifPageInfo.textContent = `${start + 1} - ${Math.min(start + size, list.length)} / ${list.length}`;

    let html = "";
    pageItems.forEach((row) => {
      html += `
        <tr>
          <td><code>${row.customer_id}</code></td>
          <td><strong>${row.name_masked || "İsimsiz"}</strong></td>
          <td>${row.phone_masked || "-"}</td>
          <td><code style="font-size:0.75rem;">${row.vin}</code></td>
          <td>${row.brand} ${row.model}</td>
          <td><span class="badge badge-warning" title="${row.reason}">Yakıt tipi UNKNOWN (Doğrulama Gerekli)</span></td>
        </tr>
      `;
    });
    tbodyVerif.innerHTML = html;
  }

  btnVerifPrev.addEventListener("click", () => {
    if (state.verificationQueue.page > 1) {
      state.verificationQueue.page--;
      renderVerificationQueue();
    }
  });

  btnVerifNext.addEventListener("click", () => {
    const totalPages = Math.ceil(state.verificationQueue.items.length / state.verificationQueue.pageSize) || 1;
    if (state.verificationQueue.page < totalPages) {
      state.verificationQueue.page++;
      renderVerificationQueue();
    }
  });

  // ==============================================================================
  // 5. Tab 2: Yönetim Paneli (Management Cockpit & Inline SVG Charts)
  // ==============================================================================
  async function loadManagementTab() {
    state.management.loaded = true;
    try {
      const [resDealer, resCost, resTrade] = await Promise.all([
        fetch("/gold/ev-share-by-dealer?year=2024"),
        fetch("/gold/service-cost-comparison"),
        fetch("/gold/trade-in-value-ratio"),
      ]);

      const dealerData = await resDealer.json();
      const costData = await resCost.json();
      const tradeData = await resTrade.json();

      state.management.evShareData = dealerData.items || dealerData.dealers || [];
      state.management.serviceCostData = costData.comparison || [];
      state.management.tradeInData = tradeData.retention_ratios || [];

      renderDealerEvChart();
      renderServiceCostChart();
      renderTradeInTable();
    } catch (err) {
      console.error("Management veri hatası:", err);
    }
  }

  // Pure Inline SVG Chart: EV Share by Dealer
  function renderDealerEvChart() {
    const container = document.getElementById("chart-dealer-ev-share");
    const rawData = state.management.evShareData;

    // Aggregate by dealer (combining brands) to show total dealer EV share %
    const dealerMap = {};
    rawData.forEach((item) => {
      const dName = (item.dealer_name || item.dealer_code).replace("Borusan Oto ", "");
      if (!dealerMap[dName]) {
        dealerMap[dName] = { name: dName, total: 0, ev: 0 };
      }
      dealerMap[dName].total += item.total_sales || 0;
      dealerMap[dName].ev += item.ev_sales || 0;
    });

    const list = Object.values(dealerMap).map((d) => ({
      name: d.name,
      pct: d.total > 0 ? (d.ev / d.total) * 100 : 0,
      evSales: d.ev,
      totalSales: d.total,
    }));

    // Fallback if empty
    if (list.length === 0) {
      container.innerHTML = `<div class="chart-loader">Grafik verisi bulunamadı.</div>`;
      return;
    }

    list.sort((a, b) => b.pct - a.pct);

    const width = 500;
    const height = 280;
    const padding = { top: 20, right: 30, bottom: 60, left: 45 };
    const chartW = width - padding.left - padding.right;
    const chartH = height - padding.top - padding.bottom;
    const barWidth = Math.min(36, chartW / list.length - 12);

    let svg = `<svg viewBox="0 0 ${width} ${height}" class="svg-chart">`;
    // Gradients & Defs
    svg += `
      <defs>
        <linearGradient id="barGrad" x1="0" y1="0" x2="0" y2="1">
          <stop offset="0%" stop-color="#2563eb" />
          <stop offset="100%" stop-color="#1d4ed8" />
        </linearGradient>
      </defs>
    `;

    // Grid lines
    [0, 25, 50, 75, 100].forEach((tick) => {
      const y = padding.top + chartH - (tick / 100) * chartH;
      svg += `
        <line x1="${padding.left}" y1="${y}" x2="${width - padding.right}" y2="${y}" stroke="#e2e8f0" stroke-dasharray="3 3" />
        <text x="${padding.left - 8}" y="${y + 4}" font-size="10" fill="#94a3b8" text-anchor="end">%${tick}</text>
      `;
    });

    // Bars
    list.forEach((d, idx) => {
      const x = padding.left + (idx * chartW) / list.length + (chartW / list.length - barWidth) / 2;
      const barH = (d.pct / 100) * chartH;
      const y = padding.top + chartH - barH;

      svg += `
        <rect class="chart-bar" x="${x}" y="${y}" width="${barWidth}" height="${barH}" rx="4" fill="url(#barGrad)">
          <title>${d.name}: %${d.pct.toFixed(1)} (${d.evSales}/${d.totalSales} EV)</title>
        </rect>
        <text x="${x + barWidth / 2}" y="${y - 6}" font-size="10" font-weight="700" fill="#1e3a8a" text-anchor="middle">
          %${d.pct.toFixed(0)}
        </text>
        <text x="${x + barWidth / 2}" y="${height - padding.bottom + 16}" font-size="9" fill="#475569" text-anchor="end" transform="rotate(-35, ${x + barWidth / 2}, ${height - padding.bottom + 16})">
          ${d.name}
        </text>
      `;
    });

    svg += `</svg>`;
    container.innerHTML = svg;
  }

  // Pure Inline SVG Chart: Service Cost Comparison
  function renderServiceCostChart() {
    const container = document.getElementById("chart-service-cost");
    const rawData = state.management.serviceCostData;

    // Filter brands having both Electric and ICE
    const brands = ["BMW", "MINI", "Jaguar"];
    const cohorts = brands.map((b) => {
      const evRow = rawData.find((x) => x.brand === b && x.powertrain_type === "Elektrik");
      const iceRow = rawData.find((x) => x.brand === b && x.powertrain_type.includes("İçten Yanmalı"));
      return {
        brand: b,
        evCost: evRow ? evRow.avg_annual_cost_per_vehicle_tl : 0,
        iceCost: iceRow ? iceRow.avg_annual_cost_per_vehicle_tl : 0,
      };
    });

    const width = 500;
    const height = 280;
    const padding = { top: 30, right: 30, bottom: 50, left: 60 };
    const chartW = width - padding.left - padding.right;
    const chartH = height - padding.top - padding.bottom;
    const maxVal = 12000;

    let svg = `<svg viewBox="0 0 ${width} ${height}" class="svg-chart">`;
    svg += `
      <defs>
        <linearGradient id="evGrad" x1="0" y1="0" x2="0" y2="1">
          <stop offset="0%" stop-color="#10b981" />
          <stop offset="100%" stop-color="#059669" />
        </linearGradient>
        <linearGradient id="iceGrad" x1="0" y1="0" x2="0" y2="1">
          <stop offset="0%" stop-color="#f59e0b" />
          <stop offset="100%" stop-color="#d97706" />
        </linearGradient>
      </defs>
    `;

    // Legend
    svg += `
      <g transform="translate(${width - 230}, 10)">
        <rect x="0" y="0" width="12" height="12" rx="2" fill="url(#evGrad)" />
        <text x="18" y="10" font-size="10" fill="#334155" font-weight="600">Elektrik</text>
        <rect x="80" y="0" width="12" height="12" rx="2" fill="url(#iceGrad)" />
        <text x="98" y="10" font-size="10" fill="#334155" font-weight="600">İçten Yanmalı</text>
      </g>
    `;

    // Y Axis Ticks
    [0, 3000, 6000, 9000, 12000].forEach((tick) => {
      const y = padding.top + chartH - (tick / maxVal) * chartH;
      svg += `
        <line x1="${padding.left}" y1="${y}" x2="${width - padding.right}" y2="${y}" stroke="#e2e8f0" stroke-dasharray="3 3" />
        <text x="${padding.left - 8}" y="${y + 4}" font-size="9" fill="#94a3b8" text-anchor="end">${(tick / 1000).toFixed(0)}k TL</text>
      `;
    });

    const groupWidth = chartW / cohorts.length;
    const barWidth = 32;

    cohorts.forEach((c, idx) => {
      const groupX = padding.left + idx * groupWidth;
      const xEv = groupX + (groupWidth - barWidth * 2 - 8) / 2;
      const xIce = xEv + barWidth + 8;

      const hEv = (c.evCost / maxVal) * chartH;
      const yEv = padding.top + chartH - hEv;

      const hIce = (c.iceCost / maxVal) * chartH;
      const yIce = padding.top + chartH - hIce;

      // EV Bar
      svg += `
        <rect class="chart-bar" x="${xEv}" y="${yEv}" width="${barWidth}" height="${hEv}" rx="4" fill="url(#evGrad)">
          <title>${c.brand} Elektrik: ${Math.round(c.evCost).toLocaleString("tr-TR")} TL / yıl</title>
        </rect>
        <text x="${xEv + barWidth / 2}" y="${yEv - 5}" font-size="9" font-weight="700" fill="#065f46" text-anchor="middle">
          ${(c.evCost / 1000).toFixed(1)}k
        </text>
      `;

      // ICE Bar
      svg += `
        <rect class="chart-bar" x="${xIce}" y="${yIce}" width="${barWidth}" height="${hIce}" rx="4" fill="url(#iceGrad)">
          <title>${c.brand} İçten Yanmalı: ${Math.round(c.iceCost).toLocaleString("tr-TR")} TL / yıl</title>
        </rect>
        <text x="${xIce + barWidth / 2}" y="${yIce - 5}" font-size="9" font-weight="700" fill="#92400e" text-anchor="middle">
          ${(c.iceCost / 1000).toFixed(1)}k
        </text>
      `;

      // Group Label
      svg += `
        <text x="${groupX + groupWidth / 2}" y="${height - padding.bottom + 20}" font-size="11" font-weight="700" fill="#1e293b" text-anchor="middle">
          ${c.brand}
        </text>
      `;
    });

    svg += `</svg>`;
    container.innerHTML = svg;
  }

  function renderTradeInTable() {
    const tbody = document.getElementById("trade-in-tbody");
    const list = state.management.tradeInData;

    if (!list || list.length === 0) {
      tbody.innerHTML = `<tr><td colspan="6" class="text-center py-3">Veri bulunamadı.</td></tr>`;
      return;
    }

    let html = "";
    list.forEach((row) => {
      html += `
        <tr>
          <td><strong>${row.brand}</strong></td>
          <td><span class="badge ${row.powertrain_type === 'Elektrik' ? 'badge-success' : 'badge-outline'}">${row.powertrain_type}</span></td>
          <td>${row.trade_in_transactions_count || row.trade_ins_count || "-"}</td>
          <td>${row.avg_original_sale_price_tl ? Math.round(row.avg_original_sale_price_tl).toLocaleString("tr-TR") + " TL" : "-"}</td>
          <td>${row.avg_trade_in_offer_tl ? Math.round(row.avg_trade_in_offer_tl).toLocaleString("tr-TR") + " TL" : "-"}</td>
          <td><strong>%${(row.retention_pct || 0).toFixed(2)}</strong></td>
        </tr>
      `;
    });
    tbody.innerHTML = html;
  }

  // ==============================================================================
  // 6. Tab 3: Veri Sağlığı (Data Health)
  // ==============================================================================
  async function loadDataHealthTab() {
    state.dataHealth.loaded = true;
    try {
      const [resQuality, resIntegrity, resScores] = await Promise.all([
        fetch("/quality"),
        fetch("/gold/referential-integrity"),
        fetch("/gold/data-quality-scores"),
      ]);

      const qData = await resQuality.json();
      const refData = await resIntegrity.json();
      const scoresData = await resScores.json();

      state.dataHealth.qualityData = qData;
      state.dataHealth.refIntegrityData = refData.checks || [];
      state.dataHealth.dqScoresData = scoresData.scores || [];

      renderReferentialIntegrityTable();
      renderTableDqScoresTable();
    } catch (err) {
      console.error("Data Health veri hatası:", err);
    }
  }

  function renderReferentialIntegrityTable() {
    const tbody = document.getElementById("ref-integrity-tbody");
    const list = state.dataHealth.refIntegrityData;

    if (!list || list.length === 0) {
      tbody.innerHTML = `<tr><td colspan="8" class="text-center py-3">Veri bulunamadı.</td></tr>`;
      return;
    }

    let html = "";
    list.forEach((row) => {
      html += `
        <tr>
          <td><code>${row.fact_table}</code></td>
          <td><span class="badge badge-outline">${row.foreign_key}</span></td>
          <td><code>${row.parent_table}</code></td>
          <td>${(row.total_fact_rows || 0).toLocaleString("tr-TR")}</td>
          <td><strong class="text-danger">${(row.orphan_unmatched_sales_rows || 0).toLocaleString("tr-TR")}</strong></td>
          <td><span class="badge badge-warning">%${(row.orphan_unmatched_sales_pct || 0).toFixed(2)}</span></td>
          <td>${row.orphan_distinct_vins || 0} VIN</td>
          <td style="font-size:0.78rem; color:#475569;">${row.root_cause_explanation || "Quarantine isolation"}</td>
        </tr>
      `;
    });
    tbody.innerHTML = html;
  }

  function renderTableDqScoresTable() {
    const tbody = document.getElementById("table-dq-scores-tbody");
    const list = state.dataHealth.dqScoresData;

    if (!list || list.length === 0) {
      tbody.innerHTML = `<tr><td colspan="8" class="text-center py-3">Skorlar bulunamadı.</td></tr>`;
      return;
    }

    let html = "";
    list.forEach((row) => {
      let statusBadge = `<span class="badge badge-success">Mükemmel</span>`;
      if (row.overall_dq_score < 90) statusBadge = `<span class="badge badge-warning">İyileştirme Gerekli</span>`;
      else if (row.overall_dq_score < 98) statusBadge = `<span class="badge badge-info">İyi</span>`;

      html += `
        <tr>
          <td><strong>${row.table_name}</strong></td>
          <td><span class="badge badge-outline">${row.layer.toUpperCase()}</span></td>
          <td>${(row.row_count || 0).toLocaleString("tr-TR")}</td>
          <td>%${(row.completeness_pct || 0).toFixed(1)}</td>
          <td>%${(row.validity_pct || 0).toFixed(1)}</td>
          <td>%${(row.uniqueness_pct || 0).toFixed(1)}</td>
          <td><strong>%${(row.overall_dq_score || 0).toFixed(2)}</strong></td>
          <td>${statusBadge}</td>
        </tr>
      `;
    });
    tbody.innerHTML = html;
  }

  // ==============================================================================
  // 7. Tab 4: Veri Kataloğu & Metrikler (Catalog & Glossary)
  // ==============================================================================
  const catalogSearchInput = document.getElementById("catalog-search-input");
  const btnCatalogSearch = document.getElementById("btn-catalog-search");
  const layerPillBtns = document.querySelectorAll("#catalog-layer-pills .pill-btn");

  layerPillBtns.forEach((btn) => {
    btn.addEventListener("click", () => {
      layerPillBtns.forEach((b) => b.classList.remove("active"));
      btn.classList.add("active");
      state.catalog.activeLayerFilter = btn.dataset.layer;
      renderCatalogView();
    });
  });

  catalogSearchInput.addEventListener("input", (e) => {
    state.catalog.searchQuery = e.target.value.trim().toLowerCase();
    renderCatalogView();
  });

  btnCatalogSearch.addEventListener("click", () => {
    state.catalog.searchQuery = catalogSearchInput.value.trim().toLowerCase();
    renderCatalogView();
  });

  async function loadCatalogTab() {
    state.catalog.loaded = true;
    try {
      const [resTables, resMetrics] = await Promise.all([
        fetch("/catalog/tables"),
        fetch("/catalog/metrics"),
      ]);

      const tData = await resTables.json();
      const mData = await resMetrics.json();

      state.catalog.tables = tData.tables || [];
      state.catalog.metrics = mData.metrics || [];

      renderCatalogView();
    } catch (err) {
      console.error("Catalog veri hatası:", err);
    }
  }

  function renderCatalogView() {
    renderMetricsGlossary();
    renderTablesAccordion();
  }

  function renderMetricsGlossary() {
    const container = document.getElementById("metrics-cards-container");
    const q = state.catalog.searchQuery;
    let list = state.catalog.metrics;

    if (state.catalog.activeLayerFilter !== "all" && state.catalog.activeLayerFilter !== "metrics") {
      document.getElementById("metrics-glossary-section").style.display = "none";
      return;
    } else {
      document.getElementById("metrics-glossary-section").style.display = "block";
    }

    if (q) {
      list = list.filter((m) => {
        return (
          m.name.toLowerCase().includes(q) ||
          (m.business_definition && m.business_definition.toLowerCase().includes(q)) ||
          (m.formula && m.formula.toLowerCase().includes(q)) ||
          (m.owner && m.owner.toLowerCase().includes(q))
        );
      });
    }

    if (list.length === 0) {
      container.innerHTML = `<div class="text-muted py-3">Aramaya uygun metrik bulunamadı.</div>`;
      return;
    }

    let html = "";
    list.forEach((m) => {
      // Synthetic data note for matching metrics or trade in
      let syntheticNote = "";
      if (
        m.name.includes("customer_match") ||
        m.name.includes("retention") ||
        m.name.includes("precision") ||
        m.name.includes("recall") ||
        m.name.includes("f1")
      ) {
        syntheticNote = `
          <div class="badge-warning" style="padding:4px 8px;border-radius:4px;font-size:0.75rem;margin-top:6px;">
            ⚠️ Sentetik zemin gerçeği (ground truth) üzerinden ölçülmüştür; karantinadaki kayıtlar dahil değildir.
          </div>
        `;
      }

      html += `
        <div class="metric-card">
          <div class="metric-header">
            <div class="metric-title">📊 ${m.name}</div>
            <span class="badge badge-outline">${m.owner || "Data Team"}</span>
          </div>
          <div class="metric-desc">${m.business_definition || ""}</div>
          <div class="metric-formula-box">
            <strong>Hesaplama Formülü:</strong><br>${m.formula || "-"}
          </div>
          ${syntheticNote}
          <div class="metric-meta">
            <span><strong>Kaynak:</strong> ${(m.source_tables || []).join(", ")}</span>
            <span><strong>Yenilenme:</strong> ${m.refresh_frequency || "Daily"}</span>
          </div>
        </div>
      `;
    });
    container.innerHTML = html;
  }

  function renderTablesAccordion() {
    const container = document.getElementById("tables-accordion-container");
    const q = state.catalog.searchQuery;
    let list = state.catalog.tables;

    if (state.catalog.activeLayerFilter === "metrics") {
      document.getElementById("tables-catalog-section").style.display = "none";
      return;
    } else {
      document.getElementById("tables-catalog-section").style.display = "block";
    }

    if (state.catalog.activeLayerFilter !== "all") {
      list = list.filter((t) => t.layer === state.catalog.activeLayerFilter);
    }

    if (q) {
      list = list.filter((t) => {
        const nameMatch = t.name.toLowerCase().includes(q);
        const descMatch = (t.description || "").toLowerCase().includes(q);
        const colMatch = (t.columns || []).some((c) => c.name.toLowerCase().includes(q) || (c.description || "").toLowerCase().includes(q));
        return nameMatch || descMatch || colMatch;
      });
    }

    document.getElementById("catalog-table-counter").textContent = `${list.length} Tablo`;

    if (list.length === 0) {
      container.innerHTML = `<div class="text-muted py-3">Aramaya uygun tablo şeması bulunamadı.</div>`;
      return;
    }

    let html = "";
    list.forEach((t) => {
      const upstream = (t.upstream_tables || []).map((u) => `<span class="badge badge-outline">${u}</span>`).join(" ");

      let columnsRows = "";
      (t.columns || []).forEach((c) => {
        columnsRows += `
          <tr>
            <td><code>${c.name}</code></td>
            <td><span class="badge badge-outline">${c.type}</span></td>
            <td>${c.nullable ? "Evet" : "Hayır"}</td>
            <td>${c.description || "-"}</td>
          </tr>
        `;
      });

      html += `
        <div class="table-card">
          <div class="table-card-header" onclick="this.parentElement.classList.toggle('expanded')">
            <div class="table-card-title-group">
              <span class="badge ${getLayerBadgeClass(t.layer)}">${t.layer.toUpperCase()}</span>
              <span class="table-card-name">${t.name}</span>
              <span class="badge badge-outline" style="font-size:0.75rem;">${(t.row_count || 0).toLocaleString("tr-TR")} satır</span>
            </div>
            <div style="display:flex;align-items:center;gap:12px;">
              <span style="font-size:0.8rem;color:#64748b;">${t.owner || ""}</span>
              <span style="font-size:0.9rem;color:#94a3b8;">▼</span>
            </div>
          </div>
          <div class="table-card-body">
            <p style="font-size:0.85rem;color:#334155;margin-bottom:10px;">${t.description || ""}</p>
            ${upstream ? `<div style="margin-bottom:12px;font-size:0.8rem;"><strong>Veri Soybağı (Upstream):</strong> ${upstream}</div>` : ""}
            <div class="table-container">
              <table class="data-table" style="font-size:0.8rem;">
                <thead>
                  <tr>
                    <th>Kolon Adı</th>
                    <th>Veri Tipi</th>
                    <th>Nullable</th>
                    <th>İş Açıklaması</th>
                  </tr>
                </thead>
                <tbody>
                  ${columnsRows}
                </tbody>
              </table>
            </div>
          </div>
        </div>
      `;
    });
    container.innerHTML = html;
  }

  function getLayerBadgeClass(layer) {
    switch (layer) {
      case "bronze": return "badge-warning";
      case "silver": return "badge-info";
      case "gold": return "badge-success";
      default: return "badge-outline";
    }
  }

  // ==============================================================================
  // Initial Bootstrap
  // ==============================================================================
  loadOpportunities();
  loadVerificationQueue();
});

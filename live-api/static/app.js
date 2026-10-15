// Mapa ao vivo: recebe posições pelo WebSocket e anima os navios entre mensagens.
"use strict";

// ---------------------------------------------------------------- configuração

const CENTER = [-46.30, -24.02];          // Porto de Santos + fundeadouro
const ZOOM = 10.6;
const BASE_STYLE = "https://tiles.openfreemap.org/styles/liberty";
const SEAMARK_TILES = "https://tiles.openseamap.org/seamark/{z}/{x}/{y}.png";

const MOVING_KN = 0.5;                    // abaixo disso o navio é considerado parado
const MAX_EXTRAPOLATION_S = 180;          // não estima posição além de 3 min sem sinal
const BLEND_MS = 2000;                    // tempo para "deslizar" até a posição corrigida
const STALE_S = 30 * 60;                  // sem sinal há mais de 30 min -> esmaecido
const FRAME_MS = 200;                     // atualização da animação (5 fps basta)

const CATEGORY_COLORS = {
  "Carga": "#2f6fd6",
  "Tanque": "#d64545",
  "Passageiros": "#8e44ad",
  "Rebocador": "#e08a00",
  "Serviço": "#16a085",
  "Prático": "#c2185b",
  "Pesca": "#6d8a00",
  "Recreio": "#00a3c4",
  "Alta velocidade": "#5d4037",
  "Outros": "#7f8c8d",
  "Desconhecido": "#9aa5b1",
};

// ---------------------------------------------------------------- estado

/** mmsi -> { data, from: [lon, lat] | null, blendStart: ms } */
const ships = new Map();

// ---------------------------------------------------------------- geometria

const M_PER_DEG_LAT = 111320;
const KN_TO_MS = 0.514444;

function isMoving(s) {
  return s.sog != null && s.sog >= MOVING_KN && s.cog != null;
}

/** Posição estimada agora a partir da última mensagem (dead reckoning). */
function predicted(s, nowMs) {
  if (!isMoving(s) || s.ts == null) return [s.lon, s.lat];
  const dt = Math.min(Math.max((nowMs / 1000) - s.ts, 0), MAX_EXTRAPOLATION_S);
  const dist = s.sog * KN_TO_MS * dt;
  const rad = (s.cog * Math.PI) / 180;
  const dLat = (dist * Math.cos(rad)) / M_PER_DEG_LAT;
  const dLon = (dist * Math.sin(rad)) / (M_PER_DEG_LAT * Math.cos((s.lat * Math.PI) / 180));
  return [s.lon + dLon, s.lat + dLat];
}

/** Posição desenhada: desliza da posição antiga até a estimada durante BLEND_MS. */
function displayed(entry, nowMs) {
  const target = predicted(entry.data, nowMs);
  if (!entry.from) return target;
  const k = Math.min((nowMs - entry.blendStart) / BLEND_MS, 1);
  if (k >= 1) { entry.from = null; return target; }
  const e = k * (2 - k); // ease-out
  return [entry.from[0] + (target[0] - entry.from[0]) * e, entry.from[1] + (target[1] - entry.from[1]) * e];
}

// ---------------------------------------------------------------- dados recebidos

function upsert(data, nowMs) {
  const entry = ships.get(data.mmsi);
  if (!entry) {
    ships.set(data.mmsi, { data, from: null, blendStart: 0, pos: null });
    return;
  }
  // Começa a correção a partir de onde o navio está desenhado agora
  entry.from = entry.pos || displayed(entry, nowMs);
  entry.blendStart = nowMs;
  entry.data = data;
}

function applySnapshot(list) {
  const now = Date.now();
  const seen = new Set();
  for (const s of list) { seen.add(s.mmsi); upsert(s, now); }
  for (const mmsi of [...ships.keys()]) if (!seen.has(mmsi)) ships.delete(mmsi);
}

// ---------------------------------------------------------------- desenho

function shipFeatures(nowMs) {
  const points = [];
  const trails = [];
  for (const entry of ships.values()) {
    const s = entry.data;
    const pos = displayed(entry, nowMs);
    entry.pos = pos;
    const age = s.ts != null ? nowMs / 1000 - s.ts : Infinity;
    const moving = isMoving(s);
    const rotation = s.heading != null ? s.heading : (s.cog != null ? s.cog : 0);
    points.push({
      type: "Feature",
      geometry: { type: "Point", coordinates: pos },
      properties: {
        mmsi: s.mmsi,
        icon: moving ? "ship-moving" : "ship-stopped",
        rotation,
        color: CATEGORY_COLORS[s.category] || CATEGORY_COLORS.Outros,
        opacity: age > STALE_S ? 0.35 : 1,
        label: s.name || "",
      },
    });
    if (s.track && s.track.length > 1 && moving) {
      const coords = s.track.map((p) => [p[0], p[1]]);
      coords.push(pos);
      trails.push({
        type: "Feature",
        geometry: { type: "LineString", coordinates: coords },
        properties: { color: CATEGORY_COLORS[s.category] || CATEGORY_COLORS.Outros },
      });
    }
  }
  return {
    points: { type: "FeatureCollection", features: points },
    trails: { type: "FeatureCollection", features: trails },
  };
}

/** Ícones desenhados no canvas como SDF, para poder colorir por categoria. */
function makeIcon(kind) {
  const size = 48;
  const c = document.createElement("canvas");
  c.width = c.height = size;
  const g = c.getContext("2d");
  g.fillStyle = "#000";
  g.beginPath();
  if (kind === "moving") {
    // Seta apontando para cima (proa); o MapLibre gira pelo heading
    g.moveTo(24, 3); g.lineTo(39, 42); g.lineTo(24, 34); g.lineTo(9, 42);
  } else {
    // Losango: navio parado/fundeado
    g.moveTo(24, 8); g.lineTo(38, 24); g.lineTo(24, 40); g.lineTo(10, 24);
  }
  g.closePath();
  g.fill();
  return g.getImageData(0, 0, size, size);
}

// ---------------------------------------------------------------- painel

function updateStats() {
  let moving = 0;
  for (const { data } of ships.values()) if (isMoving(data)) moving++;
  document.getElementById("st-total").textContent = ships.size;
  document.getElementById("st-moving").textContent = moving;
  document.getElementById("st-stopped").textContent = ships.size - moving;
}

function buildLegend() {
  const ul = document.getElementById("legend");
  for (const [name, color] of Object.entries(CATEGORY_COLORS)) {
    const li = document.createElement("li");
    li.innerHTML = `<i style="background:${color}"></i>${name}`;
    ul.appendChild(li);
  }
}

function setConnection(on, text) {
  const el = document.getElementById("conn");
  el.className = "conn " + (on ? "conn--on" : "conn--off");
  document.getElementById("conn-text").textContent = text;
}

const esc = (v) => String(v ?? "").replace(/[&<>"']/g, (ch) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[ch]));

function fmtAge(ts) {
  if (ts == null) return "–";
  const s = Math.max(0, Math.round(Date.now() / 1000 - ts));
  if (s < 60) return `${s} s atrás`;
  if (s < 3600) return `${Math.round(s / 60)} min atrás`;
  return `${(s / 3600).toFixed(1)} h atrás`;
}

function popupHtml(s) {
  const row = (k, v) => (v == null || v === "" ? "" : `<tr><td>${k}</td><td>${esc(v)}</td></tr>`);
  return `<div class="popup">
    <h2>${esc(s.name || "Sem nome")}</h2>
    <table>
      ${row("MMSI", s.mmsi)}
      ${row("Categoria", s.category)}
      ${row("Classe AIS", s.ais_class)}
      ${row("Status", s.nav_status_text)}
      ${row("Velocidade", s.sog != null ? `${s.sog.toFixed(1)} nós` : null)}
      ${row("Rumo", s.cog != null ? `${Math.round(s.cog)}°` : null)}
      ${row("Proa", s.heading != null ? `${Math.round(s.heading)}°` : null)}
      ${row("Comprimento", s.length_m ? `${s.length_m} m` : null)}
      ${row("Destino", s.destination)}
      ${row("Última posição", fmtAge(s.ts))}
    </table>
  </div>`;
}

// ---------------------------------------------------------------- WebSocket

function connect() {
  const proto = location.protocol === "https:" ? "wss" : "ws";
  const ws = new WebSocket(`${proto}://${location.host}/ws`);
  ws.onopen = () => setConnection(true, "ao vivo");
  ws.onmessage = (ev) => {
    const msg = JSON.parse(ev.data);
    const now = Date.now();
    if (msg.type === "snapshot") {
      applySnapshot(msg.ships);
      setConnection(true, msg.warmup_done === false ? "carregando…" : "ao vivo");
    } else if (msg.type === "update") {
      upsert(msg.ship, now);
    } else if (msg.type === "remove") {
      for (const m of msg.mmsi) ships.delete(m);
    }
    updateStats();
  };
  ws.onclose = () => {
    setConnection(false, "reconectando…");
    setTimeout(connect, 2000);
  };
  ws.onerror = () => ws.close();
}

// ---------------------------------------------------------------- mapa

buildLegend();

const map = new maplibregl.Map({
  container: "map",
  style: BASE_STYLE,
  center: CENTER,
  zoom: ZOOM,
  attributionControl: { compact: true },
});
map.addControl(new maplibregl.NavigationControl({ visualizePitch: false }), "top-right");
map.addControl(new maplibregl.ScaleControl({ unit: "nautical" }), "bottom-right");

map.on("load", () => {
  map.addImage("ship-moving", makeIcon("moving"), { sdf: true });
  map.addImage("ship-stopped", makeIcon("stopped"), { sdf: true });

  // Camada náutica (boias, canais, fundeadouros)
  map.addSource("seamarks", {
    type: "raster",
    tiles: [SEAMARK_TILES],
    tileSize: 256,
    attribution: '© <a href="https://www.openseamap.org" target="_blank">OpenSeaMap</a>',
  });
  map.addLayer({ id: "seamarks", type: "raster", source: "seamarks", minzoom: 9 });

  const empty = { type: "FeatureCollection", features: [] };
  map.addSource("trails", { type: "geojson", data: empty });
  map.addSource("ships", { type: "geojson", data: empty });

  map.addLayer({
    id: "trails", type: "line", source: "trails",
    paint: { "line-color": ["get", "color"], "line-width": 1.6, "line-opacity": 0.55, "line-dasharray": [2, 1.5] },
  });
  map.addLayer({
    id: "ships", type: "symbol", source: "ships",
    layout: {
      "icon-image": ["get", "icon"],
      "icon-rotate": ["get", "rotation"],
      "icon-rotation-alignment": "map",
      "icon-allow-overlap": true,
      "icon-ignore-placement": true,
      "icon-size": ["interpolate", ["linear"], ["zoom"], 9, 0.32, 13, 0.55, 16, 0.8],
      "text-field": ["step", ["zoom"], "", 13, ["get", "label"]],
      "text-size": 11,
      "text-offset": [0, 1.4],
      "text-anchor": "top",
      "text-optional": true,
    },
    paint: {
      "icon-color": ["get", "color"],
      "icon-halo-color": "#ffffff",
      "icon-halo-width": 1.5,
      "icon-opacity": ["get", "opacity"],
      "text-color": "#1d2733",
      "text-halo-color": "#ffffff",
      "text-halo-width": 1.2,
    },
  });

  map.on("click", "ships", (e) => {
    const mmsi = e.features[0].properties.mmsi;
    const entry = ships.get(mmsi);
    if (!entry) return;
    new maplibregl.Popup({ offset: 12 }).setLngLat(e.lngLat).setHTML(popupHtml(entry.data)).addTo(map);
  });
  map.on("mouseenter", "ships", () => { map.getCanvas().style.cursor = "pointer"; });
  map.on("mouseleave", "ships", () => { map.getCanvas().style.cursor = ""; });

  setInterval(() => {
    const { points, trails } = shipFeatures(Date.now());
    map.getSource("ships").setData(points);
    map.getSource("trails").setData(trails);
  }, FRAME_MS);

  connect();
});

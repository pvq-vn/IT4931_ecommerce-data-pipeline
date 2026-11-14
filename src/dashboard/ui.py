"""
Liquid Glass design system for the IT4931 Streamlit dashboard.

Presentation only: design tokens (CSS), an icon set, a Plotly template and
small HTML builders. Nothing in this module reads or changes pipeline data.
"""

from __future__ import annotations

import html
import json
import socket
from concurrent.futures import ThreadPoolExecutor, wait
from typing import Iterable, List, Sequence
from urllib.parse import quote

import plotly.graph_objects as go
import plotly.io as pio

# ---------------------------------------------------------------------------
# Tokens
# ---------------------------------------------------------------------------
BG = "#F5F9FF"
SURFACE = "#FFFFFF"
ICE = "#EAF4FF"
PRIMARY = "#2563EB"
ACCENT = "#60A5FA"
NAVY = "#172554"
TEXT = "#172033"
MUTED = "#64748B"
SLATE = "#94A3B8"
SKY_SOFT = "#BFDBFE"
GRID = "rgba(23,37,84,0.07)"
FONT = "Inter, ui-sans-serif, system-ui, -apple-system, 'Segoe UI', Roboto, sans-serif"

# Storage architectures: baseline stays neutral, the optimised tiers carry the blues.
ARCH_COLORS = {"CSV": SLATE, "Parquet": ACCENT, "Partitioned Parquet": PRIMARY}
STATUS_COLORS = {
    "Completed": PRIMARY,
    "Processing": ACCENT,
    "Cancelled": "#F08A8A",
    "Returned": "#F2B861",
    "Refunded": "#CBD5E1",
}
EVENT_COLORS = {"view": SKY_SOFT, "cart": ACCENT, "remove_from_cart": SLATE, "purchase": PRIMARY}
EVENT_LABELS = {
    "view": "Xem sản phẩm",
    "cart": "Thêm giỏ hàng",
    "remove_from_cart": "Bỏ khỏi giỏ",
    "purchase": "Mua hàng",
}

# ---------------------------------------------------------------------------
# Icons (line icons, 24px grid, 1.75 stroke; paths after the Lucide set, ISC licence)
# ---------------------------------------------------------------------------
_ICON_PATHS = {
    "layers": '<path d="m12.83 2.18a2 2 0 0 0-1.66 0L2.6 6.08a1 1 0 0 0 0 1.83l8.58 3.91a2 2 0 0 0 1.66 0l8.58-3.9a1 1 0 0 0 0-1.83Z"/><path d="m22 17.65-9.17 4.16a2 2 0 0 1-1.66 0L2 17.65"/><path d="m22 12.65-9.17 4.16a2 2 0 0 1-1.66 0L2 12.65"/>',
    "grid": '<rect x="3" y="3" width="7" height="7" rx="1.5"/><rect x="14" y="3" width="7" height="7" rx="1.5"/><rect x="14" y="14" width="7" height="7" rx="1.5"/><rect x="3" y="14" width="7" height="7" rx="1.5"/>',
    "bars": '<path d="M3 3v18h18"/><path d="M18 17V9"/><path d="M13 17V5"/><path d="M8 17v-3"/>',
    "activity": '<path d="M22 12h-4l-3 9L9 3l-3 9H2"/>',
    "gauge": '<path d="m12 14 4-4"/><path d="M3.34 19a10 10 0 1 1 17.32 0"/>',
    "bag": '<path d="M6 2 3 6v14a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2V6l-3-4Z"/><path d="M3 6h18"/><path d="M16 10a4 4 0 0 1-8 0"/>',
    "dollar": '<path d="M12 2v20"/><path d="M17 5H9.5a3.5 3.5 0 0 0 0 7h5a3.5 3.5 0 0 1 0 7H6"/>',
    "pointer": '<path d="m9 9 5 12 1.8-5.2L21 14Z"/><path d="M7.2 2.2 8 5.1"/><path d="m5.1 8-2.9-.8"/><path d="M14 4.1 12 6"/><path d="m6 12-1.9 2"/>',
    "users": '<path d="M16 21v-2a4 4 0 0 0-4-4H6a4 4 0 0 0-4 4v2"/><circle cx="9" cy="7" r="4"/><path d="M22 21v-2a4 4 0 0 0-3-3.87"/><path d="M16 3.13a4 4 0 0 1 0 7.75"/>',
    "zap": '<path d="M13 2 3 14h9l-1 8 10-12h-9l1-8z"/>',
    "download": '<path d="M12 15V3"/><path d="m7 10 5 5 5-5"/><path d="M20 21H4"/>',
    "database": '<ellipse cx="12" cy="5" rx="9" ry="3"/><path d="M3 5v14a9 3 0 0 0 18 0V5"/><path d="M3 12a9 3 0 0 0 18 0"/>',
    "radio": '<path d="M4.9 16.1C1 12.2 1 5.8 4.9 1.9"/><path d="M7.8 4.7a6.14 6.14 0 0 0-.8 7.5"/><circle cx="12" cy="9" r="2"/><path d="M16.2 4.8c2 2 2.26 5.11.8 7.47"/><path d="M19.1 1.9a9.96 9.96 0 0 1 0 14.1"/><path d="m8 22 4-11 4 11"/>',
    "cpu": '<rect x="4" y="4" width="16" height="16" rx="2"/><rect x="9" y="9" width="6" height="6"/><path d="M15 2v2M15 20v2M2 15h2M2 9h2M20 15h2M20 9h2M9 2v2M9 20v2"/>',
    "drive": '<path d="M22 12H2"/><path d="M5.45 5.11 2 12v6a2 2 0 0 0 2 2h16a2 2 0 0 0 2-2v-6l-3.45-6.89A2 2 0 0 0 16.76 4H7.24a2 2 0 0 0-1.79 1.11z"/><path d="M6 16h.01M10 16h.01"/>',
    "file": '<path d="M15 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V7Z"/><path d="M14 2v4a2 2 0 0 0 2 2h4"/><path d="M8 13h8M8 17h5"/>',
    "monitor": '<rect x="2" y="3" width="20" height="14" rx="2"/><path d="M8 21h8M12 17v4"/>',
    "clock": '<circle cx="12" cy="12" r="10"/><path d="M12 6v6l4 2"/>',
    "shield": '<path d="M20 13c0 5-3.5 7.5-7.66 8.95a1 1 0 0 1-.67-.01C7.5 20.5 4 18 4 13V6a1 1 0 0 1 1-1c2 0 4.5-1.2 6.24-2.72a1.17 1.17 0 0 1 1.52 0C14.51 3.81 17 5 19 5a1 1 0 0 1 1 1z"/><path d="m9 12 2 2 4-4"/>',
    "sliders": '<path d="M4 21v-7M4 10V3M12 21v-9M12 8V3M20 21v-5M20 12V3M2 14h4M10 8h4M18 16h4"/>',
    "info": '<circle cx="12" cy="12" r="10"/><path d="M12 16v-4M12 8h.01"/>',
}


def icon(name: str, size: int = 18, cls: str = "") -> str:
    paths = _ICON_PATHS[name]
    return (
        f'<svg class="ic {cls}" width="{size}" height="{size}" viewBox="0 0 24 24" fill="none" '
        f'stroke="currentColor" stroke-width="1.75" stroke-linecap="round" stroke-linejoin="round" '
        f'aria-hidden="true">{paths}</svg>'
    )


def _icon_data_uri(name: str) -> str:
    svg = (
        '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="black" '
        f'stroke-width="1.9" stroke-linecap="round" stroke-linejoin="round">{_ICON_PATHS[name]}</svg>'
    )
    return "data:image/svg+xml;utf8," + quote(svg)


def esc(value: object) -> str:
    return html.escape(str(value), quote=True)


# ---------------------------------------------------------------------------
# Global stylesheet
# ---------------------------------------------------------------------------
NAV_ICONS = ["grid", "bars", "activity", "gauge"]


def global_css() -> str:
    nav_icon_rules = "".join(
        f'[data-testid="column"]:has(.navm) div[role="radiogroup"] > label:nth-child({i + 1}) > div:last-child::before'
        f'{{-webkit-mask-image:url("{_icon_data_uri(n)}");mask-image:url("{_icon_data_uri(n)}");}}'
        for i, n in enumerate(NAV_ICONS)
    )
    return f"""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400..700&display=swap');
:root {{
  --bg:{BG}; --surface:{SURFACE}; --ice:{ICE}; --primary:{PRIMARY}; --accent:{ACCENT};
  --navy:{NAVY}; --text:{TEXT}; --muted:{MUTED}; --slate:{SLATE};
  --hair: rgba(23,37,84,.08);
  --ring: 0 0 0 1px rgba(37,99,235,.07);
  --panel: linear-gradient(180deg, rgba(255,255,255,.86), rgba(255,255,255,.68));
  --shadow-sm: 0 1px 2px rgba(23,37,84,.04), 0 6px 18px -8px rgba(23,37,84,.10);
  --shadow-md: 0 2px 8px rgba(23,37,84,.05), 0 22px 44px -18px rgba(23,37,84,.20);
  --r-sm: 10px; --r-md: 14px; --r-lg: 20px; --r-xl: 24px;
  --ease: cubic-bezier(.16,1,.3,1);
  --font: {FONT};
}}
html, body, .stApp, [class*="css"], button, input, textarea, select {{ font-family: var(--font) !important; }}
.stApp {{
  color: var(--text);
  background:
    radial-gradient(1100px 560px at 6% -12%, rgba(96,165,250,.20), transparent 62%),
    radial-gradient(900px 520px at 104% -6%, rgba(37,99,235,.10), transparent 58%),
    radial-gradient(900px 620px at 100% 108%, rgba(96,165,250,.14), transparent 60%),
    linear-gradient(180deg, #F5F9FF 0%, #F1F7FF 55%, #EDF4FF 100%);
  background-attachment: fixed;
}}
::selection {{ background: rgba(37,99,235,.16); color: var(--navy); }}
* {{ scrollbar-width: thin; scrollbar-color: rgba(23,37,84,.18) transparent; }}
:focus-visible {{ outline: 2px solid rgba(37,99,235,.55); outline-offset: 2px; border-radius: 8px; }}

/* ---- Streamlit chrome ---- */
[data-testid="stSidebar"], [data-testid="collapsedControl"], [data-testid="stDecoration"],
[data-testid="stToolbarActions"], #MainMenu, .stDeployButton, footer {{ display: none !important; }}
header[data-testid="stHeader"] {{ background: transparent; height: 0; }}
[data-testid="stStatusWidget"] {{
  position: fixed; right: 20px; bottom: 20px; top: auto; z-index: 1000;
  background: rgba(255,255,255,.9); border: 1px solid var(--hair); border-radius: 999px;
  padding: 2px 10px; box-shadow: var(--shadow-sm);
}}
.block-container {{ max-width: 1380px; padding: 14px 32px 56px; }}
[data-testid="stVerticalBlock"] {{ gap: 16px; }}
[data-testid="stHorizontalBlock"] {{ gap: 16px; }}
.element-container:has(> .stMarkdown style), .element-container:has(.marker) {{ display: none; }}
[data-stale="true"], .stale-element {{ opacity: .55 !important; transition: opacity .25s var(--ease); }}

/* ---- Panels: every bordered container is a glass panel ---- */
[data-testid="stVerticalBlock"] > [data-testid="stVerticalBlockBorderWrapper"] {{
  background: var(--panel);
  border: 1px solid rgba(255,255,255,.95) !important;
  border-radius: var(--r-lg) !important;
  box-shadow: inset 0 1px 0 rgba(255,255,255,1), var(--ring), var(--shadow-sm);
  padding: 4px 6px;
  animation: rise .42s var(--ease) both;
}}
[data-testid="stVerticalBlockBorderWrapper"] [data-testid="stVerticalBlockBorderWrapper"] {{
  animation: none;
}}
@keyframes rise {{ from {{ opacity: .35; transform: translateY(6px); }} to {{ opacity: 1; transform: none; }} }}

[data-testid="column"] > [data-testid="stVerticalBlockBorderWrapper"],
[data-testid="column"] > [data-testid="stVerticalBlockBorderWrapper"] > div,
[data-testid="column"] > [data-testid="stVerticalBlockBorderWrapper"] > div > [data-testid="stVerticalBlock"] {{ height: 100%; }}
[data-testid="column"] > [data-testid="stVerticalBlockBorderWrapper"] > div > [data-testid="stVerticalBlock"] > [data-testid="stVerticalBlockBorderWrapper"] {{ flex: 1 1 auto; }}

/* ---- Floating glass bar ---- */
[data-testid="stVerticalBlock"] > [data-testid="stVerticalBlockBorderWrapper"]:has(.nav-anchor) {{
  position: sticky; top: 12px; z-index: 990;
  background: linear-gradient(180deg, rgba(255,255,255,.78), rgba(255,255,255,.58)) !important;
  -webkit-backdrop-filter: saturate(180%) blur(18px); backdrop-filter: saturate(180%) blur(18px);
  border-radius: 22px !important;
  box-shadow: inset 0 1px 0 rgba(255,255,255,1), 0 0 0 1px rgba(37,99,235,.08), 0 10px 30px -14px rgba(23,37,84,.22);
  padding: 0 8px; margin-bottom: 6px;
  animation: none;
}}
[data-testid="stVerticalBlock"] > [data-testid="stVerticalBlockBorderWrapper"]:has(.nav-anchor) [data-testid="stHorizontalBlock"] {{ align-items: center; gap: 12px; }}
.brand {{ display: flex; align-items: center; gap: 12px; min-width: 0; padding: 8px 0; }}
.brand-mark {{
  width: 38px; height: 38px; border-radius: 12px; flex: none; display: grid; place-items: center; color: #fff;
  background: linear-gradient(145deg, #3B82F6, {NAVY});
  box-shadow: inset 0 1px 0 rgba(255,255,255,.35), 0 6px 14px -6px rgba(37,99,235,.7);
}}
.brand-name {{ font-weight: 650; font-size: 15px; color: var(--navy); letter-spacing: -.01em; line-height: 1.2; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }}
.brand-sub {{ font-size: 12px; color: var(--muted); line-height: 1.3; white-space: nowrap; }}

/* Segmented navigation (st.radio restyled) */
[data-testid="column"]:has(.navm) [data-testid="stRadio"] > label {{ display: none; }}
[data-testid="column"]:has(.navm) div[role="radiogroup"] {{
  display: flex; flex-wrap: nowrap; gap: 2px; padding: 4px; margin: 0 auto; width: fit-content;
  background: rgba(234,244,255,.75); border: 1px solid rgba(37,99,235,.08); border-radius: 16px;
  box-shadow: inset 0 1px 2px rgba(23,37,84,.05);
}}
[data-testid="column"]:has(.navm) div[role="radiogroup"] > label {{
  margin: 0 !important; padding: 8px 14px 8px 12px; border-radius: 12px; cursor: pointer;
  transition: background .22s var(--ease), box-shadow .22s var(--ease), color .22s var(--ease);
}}
[data-testid="column"]:has(.navm) div[role="radiogroup"] > label > div:first-child {{ display: none; }}
[data-testid="column"]:has(.navm) div[role="radiogroup"] > label > div:last-child {{ display: flex; align-items: center; gap: 8px; }}
[data-testid="column"]:has(.navm) div[role="radiogroup"] > label > div:last-child::before {{
  content: ""; width: 16px; height: 16px; flex: none; background: var(--muted);
  -webkit-mask-size: contain; mask-size: contain; -webkit-mask-repeat: no-repeat; mask-repeat: no-repeat;
  transition: background .22s var(--ease);
}}
{nav_icon_rules}
[data-testid="column"]:has(.navm) div[role="radiogroup"] > label p {{ font-size: 13.5px; font-weight: 550; color: var(--muted); white-space: nowrap; margin: 0; transition: color .22s var(--ease); }}
[data-testid="column"]:has(.navm) div[role="radiogroup"] > label:hover {{ background: rgba(255,255,255,.7); }}
[data-testid="column"]:has(.navm) div[role="radiogroup"] > label:hover p {{ color: var(--text); }}
[data-testid="column"]:has(.navm) div[role="radiogroup"] > label:has(input:checked) {{
  background: #fff; box-shadow: 0 1px 2px rgba(23,37,84,.08), 0 6px 16px -8px rgba(37,99,235,.45), inset 0 0 0 1px rgba(37,99,235,.10);
}}
[data-testid="column"]:has(.navm) div[role="radiogroup"] > label:has(input:checked) p {{ color: var(--navy); font-weight: 650; }}
[data-testid="column"]:has(.navm) div[role="radiogroup"] > label:has(input:checked) > div:last-child::before {{ background: var(--primary); }}

/* Infrastructure status */
.infra {{ display: flex; justify-content: flex-end; gap: 4px; flex-wrap: nowrap; }}
.infra-item {{
  position: relative; display: inline-flex; align-items: center; gap: 6px; padding: 6px 9px; border-radius: 10px;
  font-size: 12px; font-weight: 550; color: var(--muted); cursor: default; white-space: nowrap;
  transition: background .2s var(--ease);
}}
.infra-item:hover {{ background: rgba(234,244,255,.9); color: var(--text); }}
.dot {{ width: 7px; height: 7px; border-radius: 50%; flex: none; background: var(--slate); box-shadow: 0 0 0 3px rgba(148,163,184,.18); }}
.dot.online {{ background: #10B981; box-shadow: 0 0 0 3px rgba(16,185,129,.16); }}
.dot.fallback {{ background: var(--accent); box-shadow: 0 0 0 3px rgba(96,165,250,.20); }}
.tip {{
  position: absolute; right: 0; top: calc(100% + 8px); width: max-content; max-width: 260px; z-index: 5;
  padding: 8px 10px; border-radius: 10px; background: rgba(23,37,84,.94); color: #F1F5FF;
  font-size: 12px; font-weight: 450; line-height: 1.45; white-space: normal;
  opacity: 0; transform: translateY(-3px); pointer-events: none; transition: opacity .16s, transform .16s var(--ease);
}}
.infra-item:hover .tip, .infra-item:focus .tip {{ opacity: 1; transform: none; }}

/* ---- Type ---- */
h1, h2, h3, h4 {{ color: var(--navy) !important; letter-spacing: -.02em; }}
.page-head {{ display: flex; align-items: flex-end; justify-content: space-between; gap: 24px; margin: 18px 2px 2px; }}
.page-title {{ font-size: 26px; font-weight: 700; color: var(--navy); letter-spacing: -.025em; line-height: 1.2; margin: 0; }}
.page-desc {{ font-size: 14.5px; color: var(--muted); margin: 6px 0 0; max-width: 72ch; line-height: 1.55; }}
.card-head {{ padding: 10px 6px 2px; }}
.card-title {{ font-size: 15px; font-weight: 650; color: var(--navy); letter-spacing: -.01em; margin: 0; display: flex; align-items: center; gap: 8px; }}
.card-desc {{ font-size: 13px; color: var(--muted); margin: 4px 0 0; line-height: 1.5; }}
.insight {{
  display: flex; gap: 10px; align-items: flex-start; margin: 2px 6px 10px; padding: 10px 12px; border-radius: 12px;
  background: rgba(234,244,255,.72); color: #1E3A8A; font-size: 13px; line-height: 1.5;
}}
.insight .ic {{ flex: none; margin-top: 1px; color: var(--primary); }}
.insight b {{ color: var(--navy); font-weight: 650; }}
.note {{ font-size: 12px; color: var(--muted); margin: 0 6px 8px; line-height: 1.5; }}
.num {{ font-variant-numeric: tabular-nums; }}

/* ---- Hero ---- */
.hero {{ display: grid; grid-template-columns: minmax(0,1fr) auto; gap: 28px; align-items: end; padding: 22px 18px 18px; }}
.hero h1 {{ font-size: clamp(30px, 3.1vw, 42px); font-weight: 700; letter-spacing: -.035em; line-height: 1.08; margin: 0; color: var(--navy); padding: 0; }}
.hero p {{ font-size: 15.5px; color: #475569; margin: 12px 0 0; max-width: 64ch; line-height: 1.6; }}
.hero-meta {{ display: grid; gap: 8px; width: 340px; }}
.hero-meta .pill {{ justify-content: flex-start; }}
.hero-meta .pill:first-child {{ background: rgba(234,244,255,.9); font-weight: 600; color: var(--navy); }}
.pill {{
  display: inline-flex; align-items: center; gap: 8px; padding: 7px 12px; border-radius: 999px;
  background: rgba(255,255,255,.8); border: 1px solid rgba(37,99,235,.10); font-size: 12.5px; color: var(--text);
  white-space: nowrap; font-weight: 500;
}}
.pill .ic {{ color: var(--primary); }}
.pill b {{ font-weight: 650; color: var(--navy); }}

/* ---- KPI tiles ---- */
.kpis {{ display: grid; grid-template-columns: repeat(6, minmax(0,1fr)); gap: 14px; }}
.kpi {{
  position: relative; overflow: hidden; padding: 16px 16px 14px; border-radius: var(--r-lg);
  background: var(--panel); border: 1px solid rgba(255,255,255,.95);
  box-shadow: inset 0 1px 0 #fff, var(--ring), var(--shadow-sm);
  transition: transform .25s var(--ease), box-shadow .25s var(--ease);
  animation: rise .45s var(--ease) both;
}}
.kpi::after {{
  content: ""; position: absolute; inset: 0 0 auto 0; height: 46%; pointer-events: none;
  background: linear-gradient(180deg, rgba(255,255,255,.65), rgba(255,255,255,0));
}}
.kpi:hover {{ transform: translateY(-2px); box-shadow: inset 0 1px 0 #fff, 0 0 0 1px rgba(37,99,235,.14), var(--shadow-md); }}
.kpi-top {{ display: flex; align-items: center; justify-content: space-between; gap: 8px; }}
.kpi-label {{ font-size: 12.5px; color: var(--muted); font-weight: 550; line-height: 1.3; }}
.chip {{ width: 30px; height: 30px; border-radius: 10px; display: grid; place-items: center; color: var(--primary); background: var(--ice); flex: none; }}
.kpi-value {{ font-size: 27px; font-weight: 700; color: var(--navy); letter-spacing: -.03em; margin: 12px 0 6px; font-variant-numeric: tabular-nums; line-height: 1.1; white-space: nowrap; }}
.kpi-value small {{ font-size: 15px; font-weight: 600; color: #3B5BA9; margin-left: 2px; }}
.kpi-sub {{ font-size: 12px; color: var(--muted); line-height: 1.45; }}
.trend {{ display: inline-flex; align-items: center; gap: 3px; font-weight: 650; font-size: 11.5px; padding: 1px 7px; border-radius: 999px; margin-right: 4px; }}
.trend.up {{ color: #047857; background: rgba(16,185,129,.12); }}
.trend.down {{ color: #B42318; background: rgba(240,138,138,.18); }}
.trend.flat {{ color: var(--muted); background: rgba(148,163,184,.16); }}

/* ---- Architecture map ---- */
.arch-wrap {{ overflow-x: auto; padding: 4px 4px 10px; }}
.arch {{ width: 100%; height: auto; display: block; }}
.arch-tall {{ display: none; max-width: 420px; margin: 0 auto; }}
.arch text {{ font-family: var(--font); }}
.arch .node rect.card {{ fill: rgba(255,255,255,.92); stroke: rgba(37,99,235,.14); }}
.arch .node:hover rect.card {{ stroke: rgba(37,99,235,.45); }}
.arch .t1 {{ font-size: 14px; font-weight: 650; fill: {NAVY}; }}
.arch .t2 {{ font-size: 12px; fill: {MUTED}; }}
.arch .t3 {{ font-size: 12.5px; font-weight: 600; fill: {PRIMARY}; font-variant-numeric: tabular-nums; }}
.arch .col {{ font-size: 12px; font-weight: 600; fill: {MUTED}; }}
.arch .edge {{ fill: none; stroke-width: 2; stroke-linecap: round; }}
.arch .flow {{ fill: none; stroke-width: 2.4; stroke-linecap: round; stroke-dasharray: 2 14; animation: flow 1.6s linear infinite; }}
.arch .lbl {{ font-size: 11px; fill: {MUTED}; }}
@keyframes flow {{ to {{ stroke-dashoffset: -32; }} }}
.legend {{ display: flex; gap: 18px; flex-wrap: wrap; font-size: 12px; color: var(--muted); padding: 0 8px 12px; }}
.legend i {{ display: inline-block; width: 18px; height: 3px; border-radius: 3px; margin-right: 6px; vertical-align: middle; }}

/* ---- Spec list, live strip, scoreboard ---- */
.specs {{ display: grid; grid-template-columns: repeat(2, minmax(0,1fr)); gap: 2px 18px; padding: 4px 8px 12px; }}
.spec {{ display: flex; justify-content: space-between; gap: 12px; padding: 9px 0; border-bottom: 1px solid var(--hair); font-size: 13px; }}
.spec span {{ color: var(--muted); }}
.spec b {{ color: var(--navy); font-weight: 600; text-align: right; }}
.live {{ display: grid; grid-template-columns: repeat(6, minmax(0,1fr)); gap: 0; padding: 4px 4px 10px; }}
.live-cell {{ padding: 8px 14px; border-left: 1px solid var(--hair); }}
.live-cell:first-child {{ border-left: 0; }}
.live-cell .lc-l {{ display: block; font-size: 12px; color: var(--muted); font-weight: 550; }}
.live-cell b {{ display: block; font-size: 22px; font-weight: 700; color: var(--navy); letter-spacing: -.02em; margin: 6px 0 2px; font-variant-numeric: tabular-nums; }}
.badge {{ display: inline-flex; align-items: center; gap: 6px; padding: 4px 10px; border-radius: 999px; font-size: 12px; font-weight: 600; white-space: nowrap; }}
.badge.is-live {{ color: #047857; background: rgba(16,185,129,.12); }}
.badge.is-live .dot {{ background: #10B981; animation: pulse 2.4s ease-in-out infinite; }}
.badge.snap {{ color: #1E3A8A; background: rgba(234,244,255,.95); border: 1px solid rgba(37,99,235,.12); }}
.badge.ok {{ color: var(--primary); background: rgba(37,99,235,.08); }}
.badge.off {{ color: var(--muted); background: rgba(148,163,184,.14); }}
@keyframes pulse {{ 0%,100% {{ box-shadow: 0 0 0 3px rgba(16,185,129,.18); }} 50% {{ box-shadow: 0 0 0 6px rgba(16,185,129,.04); }} }}
.score {{ display: grid; grid-template-columns: repeat(4, minmax(0,1fr)); gap: 0; padding: 4px 4px 12px; }}
.score-item {{ padding: 6px 16px 4px; border-left: 1px solid var(--hair); }}
.score-item:first-child {{ border-left: 0; }}
.score-q {{ font-size: 13px; font-weight: 650; color: var(--navy); line-height: 1.35; min-height: 36px; }}
.score-row {{ display: grid; grid-template-columns: 112px 1fr 58px; align-items: center; gap: 8px; margin-top: 9px; font-size: 12px; color: var(--muted); }}
.score-row .bar {{ height: 8px; border-radius: 6px; background: rgba(148,163,184,.16); overflow: hidden; }}
.score-row .bar i {{ display: block; height: 100%; border-radius: 6px; }}
.score-row b {{ text-align: right; font-weight: 600; color: var(--text); font-variant-numeric: tabular-nums; }}
.score-row.best span, .score-row.best b {{ color: var(--navy); font-weight: 700; }}
.score-foot {{ margin-top: 12px; padding-top: 10px; border-top: 1px solid var(--hair); font-size: 12px; color: var(--muted); line-height: 1.5; }}
.score-foot b {{ color: var(--primary); font-weight: 700; }}
.plan-head {{ display: flex; align-items: center; justify-content: space-between; gap: 10px; flex-wrap: wrap; margin: 8px 6px 6px; font-size: 13px; font-weight: 650; color: var(--navy); }}
.foot {{ display: flex; justify-content: space-between; gap: 16px; flex-wrap: wrap; padding: 18px 6px 0; font-size: 12.5px; color: var(--muted); border-top: 1px solid var(--hair); margin-top: 8px; }}
.foot b {{ color: var(--text); font-weight: 600; }}

/* ---- Widgets ---- */
[data-testid="stWidgetLabel"] p, .stSelectbox label p, .stMultiSelect label p {{ font-size: 12.5px !important; font-weight: 600 !important; color: var(--muted) !important; }}
[data-baseweb="select"] > div {{
  border-radius: 12px !important; border: 1px solid rgba(37,99,235,.12) !important; background: rgba(255,255,255,.9) !important;
  box-shadow: 0 1px 2px rgba(23,37,84,.04); transition: border-color .2s, box-shadow .2s; min-height: 40px;
}}
[data-baseweb="select"] > div:hover {{ border-color: rgba(37,99,235,.3) !important; }}
[data-baseweb="tag"] {{ background: var(--ice) !important; border-radius: 8px !important; }}
[data-baseweb="tag"] span {{ color: var(--navy) !important; font-weight: 550; }}
[data-baseweb="popover"] ul {{ border-radius: 12px; }}
.stButton > button {{
  border-radius: 12px; font-weight: 600; font-size: 13.5px; padding: 8px 16px; min-height: 40px;
  border: 1px solid rgba(37,99,235,.14); background: rgba(255,255,255,.9); color: var(--navy);
  transition: transform .18s var(--ease), box-shadow .18s var(--ease), background .18s;
}}
.stButton > button:hover {{ border-color: rgba(37,99,235,.35); color: var(--primary); box-shadow: var(--shadow-sm); }}
.stButton > button[kind="primary"] {{
  background: linear-gradient(180deg, #3B82F6, var(--primary)); color: #fff; border: 0;
  box-shadow: inset 0 1px 0 rgba(255,255,255,.25), 0 8px 18px -8px rgba(37,99,235,.7);
}}
.stButton > button[kind="primary"]:hover {{ color: #fff; transform: translateY(-1px); }}
.stButton > button:active {{ transform: translateY(0); }}
[data-testid="stRadio"] > label {{ display: none; }}
div[role="radiogroup"] {{ display: inline-flex; gap: 2px; padding: 3px; border-radius: 12px; background: rgba(234,244,255,.9); border: 1px solid rgba(37,99,235,.08); flex-wrap: wrap; }}
div[role="radiogroup"] > label {{ margin: 0 !important; padding: 5px 12px; border-radius: 9px; cursor: pointer; transition: background .2s var(--ease); }}
div[role="radiogroup"] > label > div:first-child {{ display: none; }}
div[role="radiogroup"] > label p {{ font-size: 12.5px; font-weight: 600; color: var(--muted); margin: 0; white-space: nowrap; }}
div[role="radiogroup"] > label:has(input:checked) {{ background: #fff; box-shadow: 0 1px 3px rgba(23,37,84,.1); }}
div[role="radiogroup"] > label:has(input:checked) p {{ color: var(--navy); }}
[data-testid="stSlider"] {{ padding: 0 8px; }}
[data-testid="stTickBar"] {{ display: none; }}
[data-testid="stThumbValue"], [data-testid="stSlider"] div {{ font-family: var(--font) !important; font-size: 12.5px; font-weight: 600; color: var(--navy); }}
[data-testid="stCode"] code, .stCodeBlock code {{ white-space: pre-wrap !important; word-break: break-word; }}
[data-testid="stCode"] pre, .stCodeBlock pre {{
  background: rgba(248,251,255,.95) !important; border: 1px solid var(--hair); border-radius: 12px !important;
  font-size: 12px !important; white-space: pre-wrap !important; word-break: break-word;
}}
[data-testid="stPlotlyChart"] {{ padding: 0 2px; }}
.stAlert {{ border-radius: 14px; }}

/* ---- Responsive ---- */
@media (max-width: 1360px) {{
  .infra-item > .ic {{ display: none; }}
  .infra-item {{ padding: 6px 7px; }}
  .brand-sub {{ display: none; }}
}}
@media (max-width: 1140px) {{
  .kpis {{ grid-template-columns: repeat(3, minmax(0,1fr)); }}
  .live {{ grid-template-columns: repeat(3, minmax(0,1fr)); row-gap: 10px; }}
  .live-cell:nth-child(4) {{ border-left: 0; }}
  .score {{ grid-template-columns: repeat(2, minmax(0,1fr)); row-gap: 14px; }}
  .score-item:nth-child(3) {{ border-left: 0; }}
}}
@media (max-width: 900px) {{
  .block-container {{ padding: 10px 14px 48px; }}
  .hero {{ grid-template-columns: 1fr; }}
  .hero-meta {{ width: 100%; }}
  .kpis {{ grid-template-columns: repeat(2, minmax(0,1fr)); }}
  .specs {{ grid-template-columns: 1fr; }}
  [data-testid="column"]:has(.navm) div[role="radiogroup"] {{ overflow-x: auto; max-width: 100%; }}
  [data-testid="stVerticalBlock"] > [data-testid="stVerticalBlockBorderWrapper"]:has(.nav-anchor) {{ position: relative; top: 0; }}
}}
@media (max-width: 640px) {{
  .arch-wide {{ display: none; }}
  .arch-tall {{ display: block; }}
  [data-testid="column"]:has(.navm) div[role="radiogroup"] {{ display: grid; grid-template-columns: 1fr 1fr; width: 100%; overflow: visible; }}
  [data-testid="column"]:has(.navm) div[role="radiogroup"] > label > div:last-child {{ justify-content: center; }}
  .score {{ grid-template-columns: 1fr; }}
  .score-item {{ border-left: 0; border-top: 1px solid var(--hair); padding: 12px 6px; }}
  .score-item:first-child {{ border-top: 0; }}
  .live {{ grid-template-columns: repeat(2, minmax(0,1fr)); }}
  .live-cell:nth-child(odd) {{ border-left: 0; }}
  .infra {{ justify-content: flex-start; }}
}}
@media (prefers-reduced-motion: reduce) {{
  *, *::before, *::after {{ animation: none !important; transition: none !important; }}
}}
</style>
"""


# ---------------------------------------------------------------------------
# Plotly template
# ---------------------------------------------------------------------------
def register_plotly_template() -> None:
    if "liquid" in pio.templates:
        return
    axis = dict(
        gridcolor=GRID,
        zeroline=False,
        showline=True,
        linecolor="rgba(23,37,84,0.12)",
        ticks="",
        tickfont=dict(size=11, color=MUTED),
        title=dict(font=dict(size=11.5, color=MUTED), standoff=10),
        automargin=True,
    )
    pio.templates["liquid"] = go.layout.Template(
        layout=go.Layout(
            font=dict(family=FONT, size=12, color=TEXT),
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            colorway=[PRIMARY, ACCENT, NAVY, "#93C5FD", SLATE, "#1D4ED8", SKY_SOFT],
            xaxis=axis,
            yaxis=axis,
            margin=dict(l=8, r=12, t=10, b=8),
            hoverlabel=dict(
                bgcolor="rgba(255,255,255,0.97)",
                bordercolor="rgba(37,99,235,0.25)",
                font=dict(family=FONT, size=12, color=TEXT),
                align="left",
            ),
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="left",
                x=0,
                font=dict(size=11.5, color=MUTED),
                bgcolor="rgba(0,0,0,0)",
                itemclick="toggle",
            ),
            bargap=0.28,
            barcornerradius=5,
            hoverdistance=24,
        )
    )
    pio.templates.default = "plotly_white+liquid"


PLOTLY_CONFIG = {
    "displaylogo": False,
    "modeBarButtonsToRemove": ["lasso2d", "select2d", "autoScale2d", "toggleSpikelines"],
    "toImageButtonOptions": {"format": "png", "scale": 2},
}


# ---------------------------------------------------------------------------
# HTML builders
# ---------------------------------------------------------------------------
def page_head(title: str, desc: str, right: str = "") -> str:
    return (
        f'<div class="page-head"><div><h2 class="page-title">{esc(title)}</h2>'
        f'<p class="page-desc">{desc}</p></div><div>{right}</div></div>'
    )


def card_head(title: str, desc: str = "", ic: str | None = None) -> str:
    ic_html = icon(ic, 16) if ic else ""
    d = f'<p class="card-desc">{desc}</p>' if desc else ""
    return f'<div class="card-head"><h3 class="card-title">{ic_html}{esc(title)}</h3>{d}</div>'


def insight(text: str) -> str:
    return f'<div class="insight">{icon("info", 16)}<div>{text}</div></div>'


def trend_chip(delta_pct: float | None, suffix: str = "%") -> str:
    if delta_pct is None:
        return ""
    cls = "up" if delta_pct > 0.05 else "down" if delta_pct < -0.05 else "flat"
    sign = "+" if delta_pct > 0 else ""
    return f'<span class="trend {cls}">{sign}{delta_pct:,.1f}{suffix}</span>'


def kpi_tiles(items: Sequence[dict]) -> str:
    cells = []
    for i, it in enumerate(items):
        cells.append(
            f'<div class="kpi" style="animation-delay:{i * 40}ms">'
            f'<div class="kpi-top"><span class="kpi-label">{esc(it["label"])}</span>'
            f'<span class="chip">{icon(it["icon"], 16)}</span></div>'
            f'<div class="kpi-value">{it["value"]}</div>'
            f'<div class="kpi-sub">{it.get("trend", "")}{it["sub"]}</div></div>'
        )
    return f'<div class="kpis">{"".join(cells)}</div>'


def infra_status(items: Iterable[dict]) -> str:
    parts = []
    for it in items:
        parts.append(
            f'<span class="infra-item" tabindex="0">{icon(it["icon"], 15)}'
            f'<span class="lbl-txt">{esc(it["label"])}</span><span class="dot {it["state"]}"></span>'
            f'<span class="tip" role="tooltip"><b>{esc(it["label"])}</b> · {esc(it["detail"])}</span></span>'
        )
    return f'<div class="infra">{"".join(parts)}</div>'


def spec_list(rows: Sequence[tuple[str, str]]) -> str:
    return '<div class="specs">' + "".join(
        f'<div class="spec"><span>{esc(k)}</span><b>{esc(v)}</b></div>' for k, v in rows
    ) + "</div>"


# ---------------------------------------------------------------------------
# Infrastructure probe (TCP reachability only; reports exactly what was checked)
# ---------------------------------------------------------------------------
def _tcp_ok(host: str, port: int, timeout: float = 0.35) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def probe_endpoints(targets: dict[str, List[tuple[str, int]]], budget_s: float = 0.8) -> dict[str, str | None]:
    """Return, per service, the first 'host:port' that accepted a TCP connection (or None)."""
    pool = ThreadPoolExecutor(max_workers=8)
    futures = {}
    for name, endpoints in targets.items():
        for host, port in endpoints:
            futures[pool.submit(_tcp_ok, host, port)] = (name, f"{host}:{port}")
    done, _ = wait(list(futures), timeout=budget_s)
    pool.shutdown(wait=False, cancel_futures=True)
    result: dict[str, str | None] = {name: None for name in targets}
    for fut in done:
        name, endpoint = futures[fut]
        if fut.result() and result[name] is None:
            result[name] = endpoint
    return result


# ---------------------------------------------------------------------------
# Paginated, sortable data table (rendered in a lightweight component frame)
# ---------------------------------------------------------------------------
_TABLE_TEMPLATE = """<!doctype html><html><head><meta charset="utf-8">
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400..700&display=swap" rel="stylesheet">
<style>
*{box-sizing:border-box}html,body{margin:0;background:transparent;font-family:__FONT__;color:#172033}
.wrap{border-radius:14px;overflow:hidden;border:1px solid rgba(23,37,84,.08);background:rgba(255,255,255,.88)}
.scroll{overflow-x:auto}.wrap{position:relative}.wrap.more::after{content:'';position:absolute;top:0;right:0;bottom:44px;width:36px;pointer-events:none;background:linear-gradient(90deg,rgba(255,255,255,0),rgba(255,255,255,.95))}
table{width:100%;border-collapse:collapse;font-size:12.5px;font-variant-numeric:tabular-nums}
th{position:sticky;top:0;background:#F3F8FF;color:#475569;font-weight:600;font-size:11.5px;text-align:left;
padding:10px 12px;white-space:nowrap;border-bottom:1px solid rgba(23,37,84,.08);cursor:pointer;user-select:none}
th:hover{color:#172554}th.r,td.r{text-align:right}
th .s{display:inline-block;width:10px;margin-left:4px;color:#2563EB;vertical-align:-1px}
td{padding:8px 12px;line-height:17px;border-bottom:1px solid rgba(23,37,84,.05);white-space:nowrap;color:#172033}
tr:last-child td{border-bottom:0}tbody tr{transition:background .15s}tbody tr:hover{background:#EEF5FF}
td.hl{color:#172554;font-weight:650}
.bar{display:flex;align-items:center;justify-content:space-between;gap:12px;padding:8px 12px;font-size:12px;color:#64748B;
border-top:1px solid rgba(23,37,84,.06);background:rgba(248,251,255,.9)}
.pg{display:flex;gap:4px;align-items:center}
button{font:inherit;font-size:12px;font-weight:600;border:1px solid rgba(37,99,235,.14);background:#fff;color:#172554;
border-radius:8px;padding:4px 10px;cursor:pointer;transition:border-color .15s,color .15s}
button:hover:not(:disabled){border-color:rgba(37,99,235,.4);color:#2563EB}
button:disabled{opacity:.4;cursor:default}button:focus-visible{outline:2px solid rgba(37,99,235,.5);outline-offset:1px}
</style></head><body><div class="wrap"><div class="scroll"><table><thead><tr id="h"></tr></thead><tbody id="b"></tbody></table></div>
<div class="bar"><span id="info"></span><div class="pg"><button id="p" aria-label="Trang trước">Trước</button><span id="pn"></span><button id="n" aria-label="Trang sau">Sau</button></div></div></div>
<script>
const UP='<svg width="10" height="10" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.4" stroke-linecap="round" stroke-linejoin="round"><path d="m18 15-6-6-6 6"/></svg>',DOWN='<svg width="10" height="10" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.4" stroke-linecap="round" stroke-linejoin="round"><path d="m6 9 6 6 6-6"/></svg>';const C=__COLS__, R=__ROWS__, PS=__PS__;let page=0,sk=null,sd=1,rows=R.slice();
const h=document.getElementById('h'),b=document.getElementById('b');
C.forEach((c,i)=>{const th=document.createElement('th');th.className=c.align==='right'?'r':'';th.innerHTML=c.label+'<span class="s"></span>';
th.onclick=()=>{if(sk===i)sd=-sd;else{sk=i;sd=c.align==='right'?-1:1}rows=R.slice().sort((a,b)=>{const x=a[i].v,y=b[i].v;return (x>y?1:x<y?-1:0)*sd});page=0;draw()};h.appendChild(th)});
function draw(){const pages=Math.max(1,Math.ceil(rows.length/PS));page=Math.min(page,pages-1);
b.innerHTML=rows.slice(page*PS,page*PS+PS).map(r=>'<tr>'+r.map((c,i)=>'<td class="'+(C[i].align==='right'?'r ':'')+(C[i].hl?'hl':'')+'">'+c.d+'</td>').join('')+'</tr>').join('');
[...h.children].forEach((th,i)=>th.querySelector('.s').innerHTML=sk===i?(sd>0?UP:DOWN):'');
document.getElementById('info').textContent=rows.length?`${page*PS+1}–${Math.min(rows.length,page*PS+PS)} / ${rows.length} dòng`:'Không có dữ liệu';
document.getElementById('pn').textContent=`${page+1} / ${pages}`;
document.getElementById('p').disabled=page===0;document.getElementById('n').disabled=page>=pages-1;}
const W=document.querySelector('.wrap'),S=document.querySelector('.scroll');function cue(){W.classList.toggle('more',S.scrollWidth-S.clientWidth-S.scrollLeft>4)}S.addEventListener('scroll',cue);addEventListener('resize',cue);setTimeout(cue,50);document.getElementById('p').onclick=()=>{page--;draw()};document.getElementById('n').onclick=()=>{page++;draw()};draw();
</script></body></html>"""


def table_html(columns: Sequence[dict], rows: Sequence[Sequence[tuple[object, str]]], page_size: int) -> tuple[str, int]:
    """columns: [{label, align, hl}]; rows: [[(sort_value, display_text), ...]]. Returns (html, height)."""
    cols_json = json.dumps([{"label": esc(c["label"]), "align": c.get("align", "left"), "hl": c.get("hl", False)} for c in columns])
    data = [[{"v": (v if isinstance(v, (int, float)) else str(v)), "d": esc(d)} for v, d in r] for r in rows]
    doc = (
        _TABLE_TEMPLATE.replace("__FONT__", FONT)
        .replace("__COLS__", cols_json)
        .replace("__ROWS__", json.dumps(data, ensure_ascii=False))
        .replace("__PS__", str(page_size))
    )
    visible = min(page_size, max(1, len(rows)))
    height = 39 + visible * 34 + 43 + 4
    return doc, height

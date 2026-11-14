---
name: IT4931 E-Commerce Big Data Pipeline
description: Liquid Glass serving-layer console for a Lambda Architecture course project
colors:
  primary: "#2563EB"
  accent: "#60A5FA"
  navy: "#172554"
  ice: "#EAF4FF"
  ground: "#F5F9FF"
  surface: "#FFFFFF"
  ink: "#172033"
  muted: "#64748B"
  slate: "#94A3B8"
  sky-soft: "#BFDBFE"
  primary-bright: "#3B82F6"
  insight-ink: "#1E3A8A"
  online: "#10B981"
  positive-ink: "#047857"
  negative-ink: "#B42318"
  status-cancelled: "#F08A8A"
  status-returned: "#F2B861"
  status-refunded: "#CBD5E1"
typography:
  display:
    fontFamily: "Inter, ui-sans-serif, system-ui, -apple-system, 'Segoe UI', Roboto, sans-serif"
    fontSize: "clamp(30px, 3.1vw, 42px)"
    fontWeight: 700
    lineHeight: 1.08
    letterSpacing: "-0.035em"
  headline:
    fontFamily: "Inter, ui-sans-serif, system-ui, -apple-system, 'Segoe UI', Roboto, sans-serif"
    fontSize: "26px"
    fontWeight: 700
    lineHeight: 1.2
    letterSpacing: "-0.025em"
  metric:
    fontFamily: "Inter, ui-sans-serif, system-ui, -apple-system, 'Segoe UI', Roboto, sans-serif"
    fontSize: "27px"
    fontWeight: 700
    lineHeight: 1.1
    letterSpacing: "-0.03em"
    fontFeature: "tnum"
  title:
    fontFamily: "Inter, ui-sans-serif, system-ui, -apple-system, 'Segoe UI', Roboto, sans-serif"
    fontSize: "15px"
    fontWeight: 650
    lineHeight: 1.3
    letterSpacing: "-0.01em"
  body:
    fontFamily: "Inter, ui-sans-serif, system-ui, -apple-system, 'Segoe UI', Roboto, sans-serif"
    fontSize: "14.5px"
    fontWeight: 400
    lineHeight: 1.55
  body-sm:
    fontFamily: "Inter, ui-sans-serif, system-ui, -apple-system, 'Segoe UI', Roboto, sans-serif"
    fontSize: "13px"
    fontWeight: 400
    lineHeight: 1.5
  label:
    fontFamily: "Inter, ui-sans-serif, system-ui, -apple-system, 'Segoe UI', Roboto, sans-serif"
    fontSize: "12.5px"
    fontWeight: 550
    lineHeight: 1.3
  caption:
    fontFamily: "Inter, ui-sans-serif, system-ui, -apple-system, 'Segoe UI', Roboto, sans-serif"
    fontSize: "12px"
    fontWeight: 400
    lineHeight: 1.45
rounded:
  xs: "8px"
  sm: "10px"
  control: "12px"
  md: "14px"
  lg: "20px"
  bar: "22px"
  xl: "24px"
  pill: "999px"
spacing:
  xs: "4px"
  sm: "8px"
  md: "12px"
  lg: "16px"
  xl: "24px"
  page-x: "32px"
  page-x-mobile: "14px"
components:
  button-secondary:
    backgroundColor: "rgba(255,255,255,.9)"
    textColor: "{colors.navy}"
    typography: "{typography.label}"
    rounded: "{rounded.control}"
    padding: "8px 16px"
    height: "40px"
  button-secondary-hover:
    textColor: "{colors.primary}"
  button-primary:
    backgroundColor: "{colors.primary}"
    textColor: "{colors.surface}"
    rounded: "{rounded.control}"
    padding: "8px 16px"
    height: "40px"
  nav-segment:
    backgroundColor: "rgba(234,244,255,.75)"
    rounded: "16px"
    padding: "4px"
  nav-segment-item:
    textColor: "{colors.muted}"
    rounded: "{rounded.control}"
    padding: "8px 14px 8px 12px"
  nav-segment-item-active:
    backgroundColor: "{colors.surface}"
    textColor: "{colors.navy}"
  segmented-filter:
    backgroundColor: "rgba(234,244,255,.9)"
    rounded: "{rounded.control}"
    padding: "3px"
  kpi-tile:
    backgroundColor: "rgba(255,255,255,.78)"
    textColor: "{colors.navy}"
    rounded: "{rounded.lg}"
    padding: "16px 16px 14px"
  icon-chip:
    backgroundColor: "{colors.ice}"
    textColor: "{colors.primary}"
    rounded: "{rounded.sm}"
    size: "30px"
  pill:
    backgroundColor: "rgba(255,255,255,.8)"
    textColor: "{colors.ink}"
    rounded: "{rounded.pill}"
    padding: "7px 12px"
  insight:
    backgroundColor: "rgba(234,244,255,.72)"
    textColor: "{colors.insight-ink}"
    rounded: "{rounded.control}"
    padding: "10px 12px"
  select:
    backgroundColor: "rgba(255,255,255,.9)"
    textColor: "{colors.ink}"
    rounded: "{rounded.control}"
    height: "40px"
  tooltip:
    backgroundColor: "rgba(23,37,84,.94)"
    textColor: "#F1F5FF"
    rounded: "{rounded.sm}"
    padding: "8px 10px"
---

# Design System: IT4931 E-Commerce Big Data Pipeline

## Overview

**Creative North Star: "The Glass Control Room"**

A light, cool console seen through clear glass. The ground is a pale ice field lit by two faint blue light pools (top-left and top-right, a third bottom-right); on it float white translucent panels, each with a bright one-pixel rim, a white inner top highlight and a soft navy-tinted shadow. Exactly one surface is truly frosted: the sticky floating bar that carries identity, the four-area segmented navigation and live infrastructure status. Everything else stays crisp so numbers read cleanly on a projector.

The system is data-first and calm. Navy carries headline numbers and titles; one saturated blue marks selection and the best result; a lighter sky blue carries the speed path and secondary series; slate carries the baseline. Colour encodes meaning (architecture, path, status), never decoration. Density is moderate: 16px gutters, 20px panel corners, tabular numerals everywhere a number can be compared.

The default Streamlit chrome (sidebar, toolbar, deploy button, footer, decoration stripe) is removed; the console owns the full width.

**Key Characteristics:**
- Ice ground with radial blue light fields, fixed to the viewport
- White glass panels: gradient fill, white rim, inner highlight, navy-tinted soft shadow
- Backdrop blur reserved for the floating bar only
- Inter with tabular numerals; navy for numbers and headings, muted slate-blue for supporting text
- One 24px line-icon set at 1.75 stroke, always inline SVG in `currentColor`
- Meaning-bearing colour maps for architectures, Lambda paths, order status and event types
- Short ease-out-expo motion that switches off entirely under reduced motion

## Colors

A cool, single-hue blue family on an ice ground, with a small set of semantic hues reserved for status and trend.

### Primary
- **Signal Blue** (primary): selection, active nav icon, primary buttons, link-weight values, the Partitioned Parquet tier, Completed orders, purchase events, and the best result in any comparison.
- **Bright Blue** (primary-bright): only as the light top stop of the primary-button and brand-mark gradients.

### Secondary
- **Speed Sky** (accent): the speed-layer path in the architecture map, the Parquet tier, Processing orders, cart events, the fallback status dot, secondary chart series.
- **Pale Sky** (sky-soft): view events, the low end of sequential colour scales.

### Tertiary
- **Deep Navy** (navy): headings, KPI and metric values, the batch-layer path, winner outlines in benchmark charts, tooltip ground (at 94% opacity), the dark stop of the brand mark.
- **Insight Ink** (insight-ink): text inside insight callouts and snapshot badges.

### Neutral
- **Ice Ground** (ground): page background base, under the radial light fields.
- **Glass White** (surface): panel and tile base, active segment fill.
- **Ice Tint** (ice): icon chips, multiselect tags, insight and segment-track grounds (always translucent over the ground).
- **Ink** (ink): body text and table cells.
- **Muted Slate-Blue** (muted): labels, descriptions, captions, inactive nav, axis ticks.
- **Baseline Slate** (slate): the CSV baseline tier, remove-from-cart events, offline status dots.
- **Hairline** (`rgba(23,37,84,.08)`): dividers, spec rows, table borders. Chart grid is `rgba(23,37,84,.07)`.

### Semantic
- **Online Green** (online) on `rgba(16,185,129,.12)` with **positive-ink** text: service online, live badge, upward trend.
- **Negative Ink** (negative-ink) on `rgba(240,138,138,.18)`: downward trend chip.
- **Order status map:** Completed = primary, Processing = accent, Cancelled = status-cancelled, Returned = status-returned, Refunded = status-refunded.
- **Event map:** view = sky-soft, cart = accent, remove_from_cart = slate, purchase = primary.

### Named Rules
**The Architecture Ladder Rule.** Storage tiers are always CSV = slate, Parquet = accent, Partitioned Parquet = primary. The baseline stays neutral; the optimised tiers carry the blues. Never reassign these per chart.

**The Two Paths Rule.** In anything that draws the Lambda flow, the batch path is navy and the speed path is accent; serving-to-dashboard is primary.

**The Winner Rule.** In a comparison, every value within 5% of the best is a winner: full opacity with a 1.2px navy outline and a value label; all others drop to 0.38 opacity with no label. Ties are named as ties, never as a single winner.

## Typography

**Display Font:** Inter (with ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto)
**Body Font:** Inter
**Label/Mono Font:** Inter for labels; code blocks use Streamlit's monospace at 12px with wrapping.

**Character:** One neutral grotesque, tightened at large sizes (negative tracking scales from -0.01em at titles to -0.035em at display) and set with tabular numerals wherever numbers align or compare.

### Hierarchy
- **Display** (700, clamp 30-42px, 1.08): the overview hero title only.
- **Headline** (700, 26px, 1.2): page titles of the batch, streaming and benchmark areas.
- **Metric** (700, 27px, 1.1, tabular): KPI values; live-strip values use 22px. Units sit in a `small` at 15px/600 in `#3B5BA9`.
- **Title** (650, 15px): card titles, always led by a 16px line icon; brand name in the bar.
- **Body** (400, 14.5-15.5px, 1.55-1.6, max 64-72ch): page and hero descriptions; hero copy uses `#475569`.
- **Body-sm** (400, 13px, 1.5): card descriptions, insight text, spec rows.
- **Label** (550-600, 12.5px): KPI labels, widget labels, nav items (13.5px), pills.
- **Caption** (400, 12px): sub-lines, notes, legends, tooltips, table footers. Chart ticks 11px, legends 11.5px.

### Named Rules
**The Tabular Rule.** Every comparable number (KPIs, tables, scoreboard, map counts) uses `font-variant-numeric: tabular-nums`.

**The Navy Number Rule.** Values are navy and bold; their labels are muted and lighter. The number is always the loudest thing in its cell.

## Layout

Single column, max width 1380px, padding 14px 32px 56px (10px 14px 48px under 900px). Vertical and horizontal block gaps are 16px; KPI tiles use 14px. The floating bar is sticky at top 12px and becomes static under 900px.

Grids collapse by breakpoint: KPI tiles 6 → 3 (1140px) → 2 (900px); live strip 6 → 3 → 2 (640px); scoreboard 4 → 2 → 1; spec list 2 → 1 (900px); hero two-column (text + 340px meta stack) → one column at 900px. At 1360px the bar drops infra icons and the brand sub-line. At 640px the nav becomes a 2x2 grid and the architecture map switches from the wide to the tall SVG.

Internal rows inside a panel (live strip, scoreboard) are divided by vertical hairlines, not nested cards. Columns of bordered panels stretch to equal height.

## Elevation & Depth

Hybrid: translucency plus soft, navy-tinted, negative-spread shadows. Depth comes from a white inner top highlight and a bright rim more than from shadow mass. Blur exists in exactly one place.

### Shadow Vocabulary
- **Rim ring** (`0 0 0 1px rgba(37,99,235,.07)`): the faint blue outline on every glass panel.
- **Inner highlight** (`inset 0 1px 0 #fff`): the lit top edge of panels and tiles.
- **Resting** (`0 1px 2px rgba(23,37,84,.04), 0 6px 18px -8px rgba(23,37,84,.10)`): panels, tiles, button hover, status widget.
- **Lifted** (`0 2px 8px rgba(23,37,84,.05), 0 22px 44px -18px rgba(23,37,84,.20)`): KPI tile hover, with a 2px rise.
- **Floating bar** (`inset 0 1px 0 #fff, 0 0 0 1px rgba(37,99,235,.08), 0 10px 30px -14px rgba(23,37,84,.22)`) with `backdrop-filter: saturate(180%) blur(18px)`.
- **Blue glow** (`0 8px 18px -8px rgba(37,99,235,.7)`): primary button and brand mark only.
- **Active segment** (`0 1px 2px rgba(23,37,84,.08), 0 6px 16px -8px rgba(37,99,235,.45), inset 0 0 0 1px rgba(37,99,235,.10)`).

### Named Rules
**The One Frost Rule.** Backdrop blur belongs to the floating bar alone. Content panels are translucent gradients without blur, so charts and numbers stay sharp.

**The Tinted Shadow Rule.** Shadows are navy (`23,37,84`) or primary (`37,99,235`) tinted with negative spread; never neutral black, never hard offset.

## Shapes

Soft continuous corners, scaled by element size: 8px for small in-frame buttons and tags, 10px for icon chips, tooltips and infra items, 12px for controls, insights and code, 14px for tables and alerts, 16px for the nav track, 20px for panels and tiles, 22px for the floating bar, 999px for pills, badges and trend chips. Chart bars carry 5px corners (8px on the storage chart); scoreboard bars are 8px tall with 6px ends. Borders are 1px and translucent; the glass rim is near-white, control borders are primary at 8-14% alpha.

## Components

### Buttons
Quiet glass by default, saturated only for the one primary action.
- **Shape:** 12px corners, min height 40px, 8px 16px padding, 13.5px/600.
- **Primary:** vertical gradient bright blue → signal blue, white text, no border, inner highlight plus blue glow. Hover lifts 1px.
- **Secondary:** white at 90% with a primary 14% border, navy text. Hover deepens the border to 35%, turns text primary, adds the resting shadow.
- **Focus:** global 2px outline `rgba(37,99,235,.55)` offset 2px.

### Chips
- **Icon chip:** 30px ice square, 10px corners, primary icon; top-right of each KPI tile.
- **Pill:** white 80%, primary 10% border, 12.5px/500, icon in primary, bold part in navy. The first hero pill is ice-filled and navy.
- **Badges / trend chips:** 999px, 12px (trend 11.5px) at 600-650; live = green with pulsing dot, snapshot = ice with insight ink, ok = primary on 8% primary, off = muted on slate 14%.

### Cards / Containers
- **Corner Style:** 20px.
- **Background:** `linear-gradient(180deg, rgba(255,255,255,.86), rgba(255,255,255,.68))`.
- **Shadow Strategy:** inner highlight + rim ring + resting (see Elevation).
- **Border:** 1px `rgba(255,255,255,.95)`.
- **Internal Padding:** 4px 6px on the wrapper; card heads 10px 6px 2px. Panels rise in (6px, 0.42s) on load; nested panels do not animate.
- **KPI tile:** the same glass plus a top-46% white sheen overlay; staggered rise at 40ms per tile.

### Inputs / Fields
- **Select / multiselect:** 12px corners, white 90%, primary 12% border, 40px min height; hover border 30%. Tags are ice with navy 550 text, 8px corners.
- **Segmented filter (radio):** ice track 3px padding, 12px corners; items 5px 12px at 9px corners, 12.5px/600 muted; checked item is white with a small shadow and navy text.
- **Labels:** 12.5px/600 muted.

### Navigation
The floating glass bar: brand mark (38px, 12px corners, bright-blue → navy gradient, white layers icon), brand name and sub-line left; the segmented nav centred; infra status right. Nav track is ice 75% with an inset shadow, 4px padding, 16px corners. Each item has a 16px masked line icon in muted plus a 13.5px/550 label; hover is white 70%; active is white with the active-segment shadow, navy 650 label and a primary icon. Under 640px the four items form a 2x2 grid.

### Infrastructure status
Compact items (12px/550 muted, 10px corners) each with a line icon, label and a 7px dot with a 3px halo: green = online, accent = fallback, slate = no response. Hover or focus reveals a navy tooltip with the probed endpoint.

### Insight callout
One per chart: ice 72% ground, 12px corners, 16px primary info icon, 13px insight-ink text with navy bold keywords.

### Data table
Rendered in its own frame: 14px corners, hairline border, white 88%. Sticky header `#F3F8FF`, 11.5px/600 `#475569`, click to sort; rows 12.5px tabular, hover `#EEF5FF`, highlighted column navy 650. Footer bar shows range and 8px-corner prev/next buttons. A right-edge white fade appears while the table can scroll horizontally.

### Architecture flow map (signature)
SVG nodes are white 92% rounded cards with a primary 14% stroke (45% on hover); title 14px/650 navy, sub-line 12px muted, real count 12.5px/600 primary tabular. Edges are a faint 28%-opacity path with an arrowhead, overlaid by a dotted (2 14) moving flow line in the path colour at 1.6s per cycle. Wide layout on desktop, tall layout under 640px.

### Charts (Plotly template "liquid")
Transparent paper and plot, hairline grid, 12% axis line, no zeroline or ticks, 11px muted tick labels, horizontal legend top-left, white hover label with primary 25% border, bar gap 0.28, 5px bar corners. Colorway: primary, accent, navy, `#93C5FD`, slate, `#1D4ED8`, sky-soft. Line series use a 2-2.6px line with white-filled markers ringed in the series colour. Mode bar keeps only useful actions and exports PNG at 2x.

## Do's and Don'ts

### Do:
- **Do** build every new panel from the glass recipe: gradient white fill, white rim, inner highlight, rim ring, resting shadow, 20px corners.
- **Do** use the architecture ladder (CSV slate, Parquet accent, Partitioned primary) and the 5% tie rule for every benchmark view.
- **Do** colour Lambda paths batch = navy, speed = accent.
- **Do** lead each card title with a 16px line icon from the shared set (1.75 stroke, `currentColor`).
- **Do** follow every chart with a one-line insight callout that explains why, with bold navy keywords.
- **Do** use tabular numerals for every comparable number.
- **Do** keep motion short and ease-out (`cubic-bezier(.16,1,.3,1)`, 0.16-0.45s) and let `prefers-reduced-motion` disable all of it.

### Don't:
- **Don't** add backdrop blur to content panels; frost belongs to the floating bar.
- **Don't** reintroduce the Streamlit sidebar, toolbar or stock Plotly theme.
- **Don't** use neutral-black or hard offset shadows.
- **Don't** spend primary blue on decoration; it marks selection, the primary action and the best result.
- **Don't** highlight a single winner when values are within 5% of each other.
- **Don't** use emoji or text glyphs as icons; icons come from the inline SVG set.

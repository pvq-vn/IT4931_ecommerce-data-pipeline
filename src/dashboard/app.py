"""
Streamlit Visualization Dashboard (Core 4 - Visualization Layer).
Connects to the PostgreSQL Serving Layer (with automatic local SQLite fallback)
to display:
1. Lambda Architecture Overview & 5V Big Data Veracity Report
2. Batch Layer Analytics (Spark SQL Revenue, Top 10 Products, Order Status, MLlib RFM Segments)
3. Speed Layer Real-Time Monitoring (Kafka + Spark Structured Streaming 1m/10s Sliding Window & 10m Watermark)
4. SQL Benchmark & HDFS/Parquet/Snappy Storage Optimization Experiments (Proposal Section IV)

Presentation layer only: every figure is read from the serving layer or from src.config.
"""

from __future__ import annotations

import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict
from urllib.parse import urlparse

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
import streamlit.components.v1 as components
from plotly.subplots import make_subplots

ROOT_DIR = Path(__file__).resolve().parent.parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.config import DB_CONFIG, HDFS_CONFIG, KAFKA_CONFIG, STREAMING_CONFIG
from src.dashboard import ui
from src.ingestion.event_generator import ClickstreamEventGenerator
from src.serving.db_client import ServingDatabaseClient
from src.streaming.stream_processor import ClickstreamStreamProcessor

st.set_page_config(
    page_title="IT4931 - E-Commerce Big Data Pipeline (Group 3)",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="collapsed",
)

TABLES = [
    "batch_revenue_summary",
    "batch_top_products",
    "batch_order_status_summary",
    "batch_customer_segments",
    "data_quality_report",
    "streaming_window_metrics",
    "streaming_category_metrics",
    "benchmark_results",
    "storage_comparison",
    "rowgroup_experiments",
]

VIEWS = {
    "overview": "Tổng quan",
    "batch": "Batch Analytics",
    "streaming": "Real-time Streaming",
    "benchmark": "Benchmark",
}


# =============================================================================
# Data access (unchanged client; cached so switching areas never re-reads the DB)
# =============================================================================
@st.cache_resource
def get_db_client() -> ServingDatabaseClient:
    return ServingDatabaseClient()


@st.cache_data(ttl=120, show_spinner=False)
def load_serving_tables() -> Dict[str, pd.DataFrame]:
    db = get_db_client()
    return {name: db.read_table(name) for name in TABLES}


def _host_port(value: str, default_port: int) -> tuple[str, int]:
    parsed = urlparse(value if "://" in value else f"tcp://{value}")
    return parsed.hostname or value, parsed.port or default_port


@st.cache_data(ttl=60, show_spinner=False)
def infrastructure_status() -> Dict[str, str | None]:
    kafka = [_host_port(s.strip(), 9092) for s in KAFKA_CONFIG.bootstrap_servers.split(",") if s.strip()]
    nn_host, nn_port = _host_port(HDFS_CONFIG.namenode_uri, 9000)
    spark_env = os.getenv("SPARK_MASTER_URL", "")
    spark = [_host_port(spark_env, 7077)] if spark_env.startswith("spark://") else []
    spark += [("spark-master-service", 7077), ("localhost", 7077)]
    return ui.probe_endpoints(
        {
            "kafka": kafka + [("localhost", 29092)],
            "hdfs": [(nn_host, nn_port), (nn_host, 9870), ("localhost", 9870)],
            "spark": spark,
        }
    )


# =============================================================================
# Formatting helpers
# =============================================================================
def money(v: float) -> str:
    v = float(v)
    if abs(v) >= 1e9:
        return f"${v / 1e9:,.2f}B"
    if abs(v) >= 1e6:
        return f"${v / 1e6:,.1f}M"
    if abs(v) >= 1e3:
        return f"${v / 1e3:,.1f}K"
    return f"${v:,.2f}"


def pretty_category(code: str) -> str:
    parts = str(code).replace("_", " ").split(".")
    return " · ".join(p.capitalize() for p in parts[-2:]) if len(parts) > 1 else str(code).capitalize()


def arch_short(label: str) -> str:
    s = str(label).strip()
    if s.startswith("(1)"):
        return "CSV"
    if s.startswith("(2)"):
        return "Parquet"
    if s.startswith("(3)"):
        return "Partitioned Parquet"
    low = s.lower()
    if "csv" in low:
        return "CSV"
    if "unpartitioned" in low or "không phân vùng" in low:
        return "Parquet"
    return "Partitioned Parquet" if "partitioned" in low else "Parquet"


def query_short(qid: str) -> str:
    head, _, rest = str(qid).partition("_")
    return f"{head} · {rest.replace('_', ' ')}" if rest else head


def vi_duration(text: str) -> str:
    return (
        str(text).replace("minutes", "phút").replace("minute", "phút")
        .replace("seconds", "giây").replace("second", "giây")
    )


def window_overlap() -> int:
    try:
        return max(1, round(pd.Timedelta(STREAMING_CONFIG.window_duration) / pd.Timedelta(STREAMING_CONFIG.slide_duration)))
    except (ValueError, ZeroDivisionError):
        return 1


def pct_change(new: float, old: float) -> float | None:
    return None if not old else (new - old) * 100.0 / old


def html(markup: str) -> None:
    st.markdown(markup, unsafe_allow_html=True)


def chart(fig: go.Figure, height: int = 320) -> None:
    fig.update_layout(height=height, template="plotly_white+liquid",
                      paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                      font=dict(family=ui.FONT, size=12, color=ui.TEXT))
    st.plotly_chart(fig, use_container_width=True, theme=None, config=ui.PLOTLY_CONFIG)


def data_table(columns: list[dict], rows: list[list[tuple[object, str]]], page_size: int = 8) -> None:
    doc, height = ui.table_html(columns, rows, page_size)
    components.html(doc, height=height, scrolling=False)


def panel():
    return st.container(border=True)


# =============================================================================
# Shell: floating glass bar
# =============================================================================
def render_shell(db: ServingDatabaseClient, infra: Dict[str, str | None]) -> str:
    keys = list(VIEWS)
    if "view" not in st.session_state:
        requested = st.query_params.get("view", "overview")
        st.session_state["view"] = requested if requested in VIEWS else "overview"

    def _sync() -> None:
        st.query_params["view"] = st.session_state["view"]

    with st.container(border=True):
        c_brand, c_nav, c_status = st.columns([3.1, 5.4, 3.1])
        with c_brand:
            html(
                '<div class="brand"><span class="nav-anchor"></span>'
                f'<span class="brand-mark">{ui.icon("layers", 20)}</span>'
                '<div style="min-width:0"><div class="brand-name">E-Commerce Big Data Pipeline</div>'
                '<div class="brand-sub">IT4931 · Nhóm 3 · Lambda Architecture</div></div></div>'
            )
        with c_nav:
            html('<span class="marker navm"></span>')
            st.radio(
                "Khu vực",
                keys,
                key="view",
                horizontal=True,
                format_func=lambda k: VIEWS[k],
                label_visibility="collapsed",
                on_change=_sync,
            )
        with c_status:
            if db.is_postgres:
                db_item = {"label": "Database", "icon": "database", "state": "online",
                           "detail": f"PostgreSQL {DB_CONFIG.host}:{DB_CONFIG.port}/{DB_CONFIG.database}"}
            else:
                db_item = {"label": "Database", "icon": "database", "state": "fallback",
                           "detail": f"SQLite cục bộ ({DB_CONFIG.sqlite_fallback_path.name}); PostgreSQL không phản hồi"}

            def item(key: str, label: str, ic: str) -> dict:
                ep = infra.get(key)
                return {"label": label, "icon": ic, "state": "online" if ep else "offline",
                        "detail": f"Kết nối TCP tới {ep}" if ep else "Không phản hồi trong lần kiểm tra gần nhất (60 giây)"}

            html(ui.infra_status([item("kafka", "Kafka", "radio"), item("spark", "Spark", "cpu"),
                                  item("hdfs", "HDFS", "drive"), db_item]))
    return st.session_state["view"]


# =============================================================================
# 1. Overview
# =============================================================================
def render_overview(t: Dict[str, pd.DataFrame], db: ServingDatabaseClient, infra: Dict[str, str | None]) -> None:
    rev_df, stream_df, bench_df = t["batch_revenue_summary"], t["streaming_window_metrics"], t["benchmark_results"]
    qual_df, storage_df = t["data_quality_report"], t["storage_comparison"]

    # ---- Hero
    online = sum(1 for v in infra.values() if v)
    if online == len(infra):
        state_txt = "Pipeline đang chạy · Kafka, Spark, HDFS phản hồi"
    elif online == 0:
        state_txt = "Chế độ cục bộ · đọc kết quả đã tính từ tầng Serving"
    else:
        state_txt = f"{online}/{len(infra)} dịch vụ hạ tầng phản hồi"
    last_window = ""
    if not stream_df.empty:
        last_window = pd.to_datetime(stream_df["window_end"]).max().strftime("%H:%M:%S · %d/%m/%Y")
    batch_time = ""
    if not db.is_postgres and DB_CONFIG.sqlite_fallback_path.exists():
        batch_time = datetime.fromtimestamp(DB_CONFIG.sqlite_fallback_path.stat().st_mtime).strftime("%H:%M · %d/%m/%Y")
    meta = [f'<span class="pill"><span class="dot {"online" if online else "fallback"}"></span>{ui.esc(state_txt)}</span>']
    if last_window:
        meta.append(f'<span class="pill">{ui.icon("clock", 14)}Cửa sổ stream gần nhất <b class="num">{last_window}</b></span>')
    if batch_time:
        meta.append(f'<span class="pill">{ui.icon("database", 14)}Serving DB cập nhật <b class="num">{batch_time}</b></span>')
    with panel():
        html(
            '<div class="hero"><div><h1>E-Commerce Big Data Pipeline</h1>'
            '<p>Kiến trúc Lambda: dữ liệu lịch sử đi qua HDFS và Spark Batch, luồng clickstream đi qua Kafka và '
            'Spark Structured Streaming; cả hai hội tụ tại tầng Serving để phục vụ phân tích và đối chứng hiệu năng.</p></div>'
            f'<div class="hero-meta">{"".join(meta)}</div></div>'
        )

    # ---- KPI tiles
    total_batch_orders = int(rev_df["total_orders"].sum())
    total_batch_rev = float(rev_df["total_revenue"].sum())
    completed = int(rev_df["completed_orders"].sum())
    monthly = rev_df.groupby(["year", "month"], as_index=False)["total_revenue"].sum().sort_values(["year", "month"])
    rev_trend = pct_change(monthly["total_revenue"].iloc[-1], monthly["total_revenue"].iloc[-2]) if len(monthly) > 1 else None
    last_m = f"T{int(monthly['month'].iloc[-1])}" if len(monthly) else ""
    prev_m = f"T{int(monthly['month'].iloc[-2])}" if len(monthly) > 1 else ""

    raw_records = int(qual_df["raw_records_count"].iloc[0]) if not qual_df.empty else total_batch_orders
    sw = stream_df.assign(_ts=pd.to_datetime(stream_df["window_start"])).sort_values("_ts") if not stream_df.empty else stream_df
    total_stream_events = int(sw["total_events"].sum()) if not sw.empty else 0
    latest_active_users = int(sw.iloc[-1]["active_users"]) if not sw.empty else 0
    prev_active = int(sw.iloc[-2]["active_users"]) if len(sw) > 1 else None
    best = bench_df.loc[bench_df["speedup_vs_csv"].idxmax()] if not bench_df.empty else None
    max_speedup = float(bench_df["speedup_vs_csv"].max()) if not bench_df.empty else 1.0
    max_io_saved = float(bench_df["io_reduction_pct"].max()) if not bench_df.empty else 0.0

    users_delta = ""
    if prev_active is not None:
        d = latest_active_users - prev_active
        users_delta = f'<span class="trend {"up" if d > 0 else "down" if d < 0 else "flat"}">{d:+d}</span>'
    html(
        ui.kpi_tiles(
            [
                {"label": "Tổng đơn hàng", "icon": "bag", "value": f"{total_batch_orders:,}",
                 "sub": f"Sau làm sạch từ {raw_records:,} bản ghi thô"},
                {"label": "Tổng doanh thu", "icon": "dollar", "value": money(total_batch_rev),
                 "trend": ui.trend_chip(rev_trend),
                 "sub": f"{last_m} so với {prev_m} · {completed:,} đơn hoàn tất" if rev_trend is not None else f"{completed:,} đơn hoàn tất"},
                {"label": "Sự kiện clickstream", "icon": "pointer", "value": f"{total_stream_events:,}",
                 "sub": f"Tổng theo {len(sw):,} cửa sổ trượt (mỗi sự kiện thuộc ~{window_overlap()} cửa sổ)"},
                {"label": "Người dùng hoạt động", "icon": "users", "value": f"{latest_active_users:,}",
                 "trend": users_delta, "sub": "Cửa sổ gần nhất, so với cửa sổ trước"},
                {"label": "Tăng tốc truy vấn", "icon": "zap", "value": f"{max_speedup:.2f}<small>×</small>",
                 "sub": f"{query_short(best['query_id']).split(' · ')[0]} trên {arch_short(best['storage_architecture'])}, so với CSV" if best is not None else "So với CSV thô"},
                {"label": "Giảm I/O", "icon": "download", "value": f"{max_io_saved:.1f}<small>%</small>",
                 "sub": "Partition pruning + pushdown (ước lượng)"},
            ]
        )
    )

    # ---- Architecture map (signature)
    with panel():
        html(ui.card_head(
            "Luồng dữ liệu Lambda",
            "Mỗi khối hiển thị số liệu thật của pipeline. Nhánh batch (xanh đậm) xử lý lịch sử trên HDFS; "
            "nhánh speed (xanh nhạt) xử lý clickstream qua Kafka; hai nhánh hội tụ ở tầng Serving.",
            "layers",
        ))
        html(architecture_svg(t, db, infra))
        html(
            f'<div class="legend"><span><i style="background:{ui.NAVY}"></i>Batch layer</span>'
            f'<span><i style="background:{ui.ACCENT}"></i>Speed layer</span>'
            f'<span><i style="background:{ui.PRIMARY}"></i>Serving → Dashboard</span>'
            '<span><span class="dot online" style="display:inline-block;margin-right:6px"></span>Dịch vụ phản hồi</span>'
            '<span><span class="dot fallback" style="display:inline-block;margin-right:6px"></span>Dự phòng (SQLite)</span>'
            '<span><span class="dot" style="display:inline-block;margin-right:6px"></span>Không phản hồi</span></div>'
        )

    # ---- Storage tiers + configuration
    c1, c2 = st.columns([3, 2])
    with c1, panel():
        html(ui.card_head("Dung lượng 3 tầng lưu trữ HDFS", "Raw CSV → Refined Parquet (Snappy) → Partitioned Parquet theo year/month.", "drive"))
        if not storage_df.empty:
            sd = storage_df.copy()
            sd["short"] = ["Raw CSV", "Refined Parquet", "Partitioned Parquet"][: len(sd)]
            colors = [ui.SLATE, ui.ACCENT, ui.PRIMARY][: len(sd)]
            fig = go.Figure(go.Bar(
                x=sd["short"], y=sd["total_size_mb"], marker=dict(color=colors, cornerradius=8),
                text=[f"{v:,.2f} MB · {r:.2f}×" for v, r in zip(sd["total_size_mb"], sd["compression_ratio"])],
                textposition="outside", cliponaxis=False,
                customdata=sd[["format_name", "compression_codec", "file_count", "avg_file_size_mb"]],
                hovertemplate="<b>%{x}</b><br>%{customdata[0]} · %{customdata[1]}<br>Dung lượng: %{y:,.2f} MB"
                              "<br>Số tệp: %{customdata[2]} · TB %{customdata[3]:,.2f} MB<extra></extra>",
            ))
            fig.update_layout(yaxis_title="MB", showlegend=False, margin=dict(t=24))
            chart(fig, 300)
            raw_mb, ref_mb = float(sd["total_size_mb"].iloc[0]), float(sd["total_size_mb"].iloc[1])
            part_mb = float(sd["total_size_mb"].iloc[-1])
            html(ui.insight(
                f"Parquet + Snappy giảm dung lượng từ <b>{raw_mb:,.2f} MB</b> xuống <b>{ref_mb:,.2f} MB</b> "
                f"(<b>{(1 - ref_mb / raw_mb) * 100:.1f}%</b>). Bản phân vùng lớn hơn {part_mb - ref_mb:,.2f} MB vì mỗi "
                f"thư mục tháng có footer và từ điển riêng, đổi lại khả năng bỏ qua cả thư mục khi truy vấn."
            ))
    with c2, panel():
        html(ui.card_head("Cấu hình kiến trúc", "Giá trị đọc trực tiếp từ src/config.py và kết nối hiện tại.", "sliders"))
        html(ui.spec_list([
            ("Serving DB", db.backend_name),
            ("HDFS block size", f"{HDFS_CONFIG.block_size_bytes // (1024 * 1024)} MB"),
            ("HDFS replication", str(HDFS_CONFIG.replication_factor)),
            ("Parquet codec", f"{HDFS_CONFIG.parquet_compression.capitalize()} + Dictionary"),
            ("Kafka topic", KAFKA_CONFIG.topic),
            ("Kafka partitions", f"{KAFKA_CONFIG.num_partitions} · key user_id"),
            ("Sliding window", f"{vi_duration(STREAMING_CONFIG.window_duration)}, trượt {vi_duration(STREAMING_CONFIG.slide_duration)}"),
            ("Watermark", vi_duration(STREAMING_CONFIG.watermark_delay)),
            ("Trigger", vi_duration(STREAMING_CONFIG.trigger_interval)),
            ("Tốc độ sinh sự kiện", f"{KAFKA_CONFIG.events_per_second:g} sự kiện/giây"),
        ]))

    # ---- Veracity
    with panel():
        html(ui.card_head("Báo cáo độ tin cậy dữ liệu (Veracity)", "Kết quả làm sạch khi chuyển Raw → Refined.", "shield"))
        if not qual_df.empty:
            data_table(
                [{"label": "Kiểm tra"}, {"label": "Bản ghi thô", "align": "right"}, {"label": "Bản ghi lỗi", "align": "right"},
                 {"label": "Bản ghi giữ lại", "align": "right", "hl": True}, {"label": "Tỷ lệ lỗi", "align": "right"},
                 {"label": "Cách xử lý"}],
                [[(r.check_name, r.check_name), (r.raw_records_count, f"{int(r.raw_records_count):,}"),
                  (r.invalid_records_filtered, f"{int(r.invalid_records_filtered):,}"),
                  (r.clean_records_retained, f"{int(r.clean_records_retained):,}"),
                  (r.defect_rate_pct, f"{float(r.defect_rate_pct):.3f}%"), (r.resolution_method, r.resolution_method)]
                 for r in qual_df.itertuples()],
                page_size=6,
            )

    html(
        '<div class="foot"><span><b>IT4931 · Lưu trữ và Xử lý Dữ liệu Lớn</b> · GVHD: TS. Vũ Tuyết Trinh</span>'
        '<span>Nhóm 3: Phạm Văn Quyết (202416331) · Vũ Việt Tiến (202416362) · '
        'Nguyễn Xuân Hồng Quân (202416321) · Đặng Minh Quân (202416323)</span></div>'
    )


def architecture_svg(t: Dict[str, pd.DataFrame], db: ServingDatabaseClient, infra: Dict[str, str | None]) -> str:
    qual_df, storage_df, stream_df = t["data_quality_report"], t["storage_comparison"], t["streaming_window_metrics"]
    raw = int(qual_df["raw_records_count"].iloc[0]) if not qual_df.empty else 0
    clean = int(qual_df["clean_records_retained"].iloc[-1]) if not qual_df.empty else 0
    sizes = " → ".join(f"{v:,.1f}" for v in storage_df["total_size_mb"]) + " MB" if not storage_df.empty else "—"
    n_tables = sum(1 for d in t.values() if not d.empty)
    spark_ok, kafka_ok, hdfs_ok = bool(infra.get("spark")), bool(infra.get("kafka")), bool(infra.get("hdfs"))

    def node(x, y, w, title, sub, value, ic, status=None, h=92):
        dot = ""
        if status is not None:
            fill = "#10B981" if status == "online" else ui.ACCENT if status == "fallback" else ui.SLATE
            dot = f'<circle cx="{x + w - 16}" cy="{y + 18}" r="4" fill="{fill}"/>'
        icon_svg = ui.icon(ic, 18).replace("<svg ", f'<svg x="{x + 14}" y="{y + 12}" ', 1)
        return (
            f'<g class="node"><rect class="card" x="{x}" y="{y}" width="{w}" height="{h}" rx="16"/>'
            f'<g style="color:{ui.PRIMARY}">{icon_svg}</g>{dot}'
            f'<text class="t1" x="{x + 40}" y="{y + 26}">{ui.esc(title)}</text>'
            f'<text class="t2" x="{x + 14}" y="{y + 54}">{ui.esc(sub)}</text>'
            f'<text class="t3" x="{x + 14}" y="{y + 76}">{ui.esc(value)}</text></g>'
        )

    def edge(d, color, label=None, lx=0, ly=0):
        lbl = f'<text class="lbl" x="{lx}" y="{ly}">{ui.esc(label)}</text>' if label else ""
        return (
            f'<path class="edge" d="{d}" stroke="{color}" stroke-opacity=".28" marker-end="url(#ah-{color[1:]})"/>'
            f'<path class="flow" d="{d}" stroke="{color}"/>{lbl}'
        )

    def marker(color):
        return (
            f'<marker id="ah-{color[1:]}" viewBox="0 0 10 10" refX="8.5" refY="5" markerWidth="7" markerHeight="7" orient="auto">'
            f'<path d="M0 0L10 5L0 10z" fill="{color}" fill-opacity=".55"/></marker>'
        )

    st_ = lambda ok: "online" if ok else "offline"  # noqa: E731
    db_state = "online" if db.is_postgres else "fallback"
    window = f"{vi_duration(STREAMING_CONFIG.window_duration)}/{vi_duration(STREAMING_CONFIG.slide_duration)}"
    cols = [(16, "Nguồn dữ liệu"), (262, "Thu thập & lưu trữ"), (528, "Xử lý"), (798, "Phục vụ"), (1016, "Trực quan")]
    head = "".join(f'<text class="col" x="{x + 2}" y="26">{ui.esc(lbl)}</text>' for x, lbl in cols)
    body = "".join([
        edge("M192 102 H254", ui.NAVY),
        edge("M192 258 H254", ui.ACCENT),
        edge("M360 212 V156", ui.ACCENT, "lưu bản thô", 368, 188),
        edge("M458 102 H520", ui.NAVY),
        edge("M458 258 H520", ui.ACCENT),
        edge("M728 102 C766 102 760 160 790 160", ui.NAVY),
        edge("M728 258 C766 258 760 200 790 200", ui.ACCENT),
        edge("M974 180 H1008", ui.PRIMARY),
        node(16, 56, 176, "Historical CSV", "Giao dịch lịch sử", f"{raw:,} bản ghi", "file"),
        node(16, 212, 176, "Event Generator", "Clickstream JSON", f"{KAFKA_CONFIG.events_per_second:g} sự kiện/giây", "pointer"),
        node(262, 56, 196, "HDFS", "Raw → Refined → Partitioned", sizes, "drive", st_(hdfs_ok)),
        node(262, 212, 196, "Apache Kafka", f"topic {KAFKA_CONFIG.topic}", f"{KAFKA_CONFIG.num_partitions} partitions · key user_id", "radio", st_(kafka_ok)),
        node(528, 56, 200, "Batch Layer", "Spark SQL · MLlib K-Means", f"{clean:,} bản ghi sạch", "cpu", st_(spark_ok)),
        node(528, 212, 200, "Speed Layer", f"Cửa sổ {window}", f"Watermark {vi_duration(STREAMING_CONFIG.watermark_delay)}", "activity", st_(spark_ok)),
        node(798, 134, 176, "Serving Layer", "PostgreSQL" if db.is_postgres else "SQLite (fallback)", f"{n_tables} bảng có dữ liệu", "database", db_state),
        node(1016, 134, 128, "Dashboard", "Streamlit", "Đang xem", "monitor"),
    ])
    defs = "<defs>" + marker(ui.NAVY) + marker(ui.ACCENT) + marker(ui.PRIMARY) + "</defs>"
    # Narrow screens: the same graph stacked top-to-bottom, with shorter secondary lines.
    tall = "".join([
        edge("M88 100 V132", ui.NAVY),
        edge("M272 100 V132", ui.ACCENT),
        edge("M188 186 H180", ui.ACCENT),
        edge("M88 232 V264", ui.NAVY),
        edge("M272 232 V264", ui.ACCENT),
        edge("M88 364 C88 386 128 380 138 398", ui.NAVY),
        edge("M272 364 C272 386 232 380 222 398", ui.ACCENT),
        edge("M180 496 V528", ui.PRIMARY),
        node(4, 8, 168, "Historical CSV", "Giao dịch lịch sử", f"{raw:,} bản ghi", "file"),
        node(188, 8, 168, "Event Generator", "Clickstream JSON", f"{KAFKA_CONFIG.events_per_second:g} sự kiện/giây", "pointer"),
        node(4, 140, 168, "HDFS", "3 tầng lưu trữ", sizes, "drive", st_(hdfs_ok)),
        node(188, 140, 168, "Apache Kafka", f"{KAFKA_CONFIG.num_partitions} partitions", KAFKA_CONFIG.topic, "radio", st_(kafka_ok)),
        node(4, 272, 168, "Batch Layer", "Spark SQL · MLlib", f"{clean:,} bản ghi", "cpu", st_(spark_ok)),
        node(188, 272, 168, "Speed Layer", f"Cửa sổ {window}", f"Watermark {vi_duration(STREAMING_CONFIG.watermark_delay)}", "activity", st_(spark_ok)),
        node(96, 404, 168, "Serving Layer", "PostgreSQL" if db.is_postgres else "SQLite (fallback)", f"{n_tables} bảng có dữ liệu", "database", db_state),
        node(96, 536, 168, "Dashboard", "Streamlit", "Đang xem", "monitor"),
    ])
    label = "Sơ đồ luồng dữ liệu Lambda: nguồn, Kafka và HDFS, Batch và Speed layer, Serving, Dashboard"
    return (
        f'<div class="arch-wrap"><svg class="arch arch-wide" viewBox="0 0 1160 320" role="img" aria-label="{label}">'
        f"{defs}{head}{body}</svg>"
        f'<svg class="arch arch-tall" viewBox="0 0 360 636" role="img" aria-label="{label}">{tall}</svg></div>'
    )


# =============================================================================
# 2. Batch analytics
# =============================================================================
def render_batch(t: Dict[str, pd.DataFrame]) -> None:
    rev_df, top10_df = t["batch_revenue_summary"], t["batch_top_products"]
    status_df, seg_df = t["batch_order_status_summary"], t["batch_customer_segments"]

    html(ui.page_head(
        "Batch Analytics",
        f"Kết quả Spark SQL và MLlib trên {int(rev_df['total_orders'].sum()):,} giao dịch đã làm sạch. "
        "Bộ lọc áp dụng cho xu hướng doanh thu và doanh thu theo danh mục.",
    ))

    rev = rev_df.copy()
    rev["period"] = rev["year"].astype(int).astype(str) + "-" + rev["month"].astype(int).astype(str).str.zfill(2)
    periods = sorted(rev["period"].unique())
    categories = sorted(rev["category"].unique())

    with panel():
        f1, f2, f3 = st.columns([2.2, 3, 0.9])
        with f1:
            if len(periods) > 1:
                p_from, p_to = st.select_slider("Khoảng thời gian", options=periods, value=(periods[0], periods[-1]), key="b_period")
            else:
                p_from = p_to = periods[0]
        with f2:
            chosen = st.multiselect("Danh mục", categories, default=[], key="b_cats", format_func=pretty_category,
                                    placeholder="Tất cả danh mục")
        with f3:
            st.write("")
            if st.button("Đặt lại", use_container_width=True, key="b_reset"):
                for k in ("b_period", "b_cats"):
                    st.session_state.pop(k, None)
                st.rerun()

    flt = rev[(rev["period"] >= p_from) & (rev["period"] <= p_to)]
    if chosen:
        flt = flt[flt["category"].isin(chosen)]

    c1, c2 = st.columns([3, 2])
    with c1, panel():
        html(ui.card_head("Xu hướng doanh thu theo tháng", "Doanh thu đơn hoàn tất và tổng số đơn mỗi tháng.", "activity"))
        monthly = flt.groupby("period", as_index=False).agg(
            total_revenue=("total_revenue", "sum"), total_orders=("total_orders", "sum"),
            completed_orders=("completed_orders", "sum"),
        ).sort_values("period")
        if monthly.empty:
            st.info("Không có dữ liệu trong khoảng lọc.")
        else:
            fig = make_subplots(specs=[[{"secondary_y": True}]])
            fig.add_bar(x=monthly["period"], y=monthly["total_orders"], name="Số đơn", marker_color="rgba(96,165,250,.28)",
                        hovertemplate="%{y:,} đơn<extra>Số đơn</extra>", secondary_y=True)
            fig.add_scatter(x=monthly["period"], y=monthly["total_revenue"], name="Doanh thu", mode="lines+markers",
                            line=dict(color=ui.PRIMARY, width=2.6, shape="spline", smoothing=0.6),
                            marker=dict(size=7, color="#fff", line=dict(color=ui.PRIMARY, width=2)),
                            fill="tozeroy", fillcolor="rgba(37,99,235,.08)",
                            customdata=monthly["completed_orders"],
                            hovertemplate="$%{y:,.0f}<br>%{customdata:,} đơn hoàn tất<extra>Doanh thu</extra>", secondary_y=False)
            fig.update_layout(hovermode="x unified")
            fig.update_yaxes(title_text="Doanh thu ($)", tickprefix="$", tickformat="~s", secondary_y=False)
            fig.update_yaxes(title_text="Số đơn", showgrid=False, tickformat="~s", secondary_y=True)
            chart(fig, 330)
            peak = monthly.loc[monthly["total_revenue"].idxmax()]
            q4 = monthly[monthly["period"].str[-2:].isin(["10", "11", "12"])]["total_revenue"].sum()
            share = q4 * 100 / max(1.0, monthly["total_revenue"].sum())
            html(ui.insight(
                f"Đỉnh doanh thu ở <b>{peak['period']}</b> với <b>{money(peak['total_revenue'])}</b>. "
                + (f"Quý 4 chiếm <b>{share:.1f}%</b> doanh thu trong khoảng đã chọn." if q4 > 0 else "")
            ))
    with c2, panel():
        html(ui.card_head("Doanh thu theo danh mục", "Sắp xếp theo doanh thu; di chuột để xem số đơn và giá trị đơn TB.", "bars"))
        cat = flt.groupby("category", as_index=False).agg(
            total_revenue=("total_revenue", "sum"), total_orders=("total_orders", "sum"),
            completed=("completed_orders", "sum"),
        ).sort_values("total_revenue")
        if cat.empty:
            st.info("Không có dữ liệu trong khoảng lọc.")
        else:
            cat["label"] = cat["category"].map(pretty_category)
            cat["aov"] = cat["total_revenue"] / cat["completed"].clip(lower=1)
            colors = [ui.PRIMARY if i == len(cat) - 1 else "rgba(37,99,235,.55)" for i in range(len(cat))]
            fig = go.Figure(go.Bar(
                y=cat["label"], x=cat["total_revenue"], orientation="h", marker_color=colors,
                customdata=cat[["total_orders", "aov", "category"]],
                hovertemplate="<b>%{y}</b><br>Doanh thu: $%{x:,.0f}<br>Số đơn: %{customdata[0]:,}"
                              "<br>Giá trị đơn TB: $%{customdata[1]:,.2f}<extra>%{customdata[2]}</extra>",
            ))
            fig.update_xaxes(tickprefix="$", tickformat="~s")
            fig.update_yaxes(showline=False)
            chart(fig, 330)
            top = cat.iloc[-1]
            html(ui.insight(
                f"<b>{pretty_category(top['category'])}</b> dẫn đầu với "
                f"<b>{top['total_revenue'] * 100 / max(1.0, cat['total_revenue'].sum()):.1f}%</b> doanh thu."
            ))

    c3, c4 = st.columns([3, 2])
    with c3, panel():
        html(ui.card_head("Top 10 sản phẩm theo doanh thu", "Kết quả JOIN giao dịch với danh mục sản phẩm · toàn bộ lịch sử, không áp dụng bộ lọc.", "bag"))
        if not top10_df.empty:
            tp = top10_df.sort_values("total_revenue")
            fig = go.Figure(go.Bar(
                y=tp["product_name"], x=tp["total_revenue"], orientation="h",
                marker=dict(color=tp["profit_margin_pct"], colorscale=[[0, "#BFDBFE"], [1, ui.PRIMARY]],
                            colorbar=dict(title=dict(text="Biên LN %", font=dict(size=11, color=ui.MUTED)),
                                          thickness=10, len=0.75, tickfont=dict(size=10, color=ui.MUTED), outlinewidth=0)),
                text=[f"{u:,} sp" for u in tp["total_units_sold"]], textposition="inside", insidetextanchor="end",
                textfont=dict(color=["#fff" if (m - tp["profit_margin_pct"].min()) > 0.45 * max(1e-9, tp["profit_margin_pct"].max() - tp["profit_margin_pct"].min()) else ui.NAVY for m in tp["profit_margin_pct"]], size=11),
                customdata=tp[["brand", "category", "total_units_sold", "avg_price", "profit_margin_pct"]],
                hovertemplate="<b>%{y}</b><br>%{customdata[0]} · %{customdata[1]}<br>Doanh thu: $%{x:,.0f}"
                              "<br>Đã bán: %{customdata[2]:,} · Giá TB $%{customdata[3]:,.2f}<br>Biên LN: %{customdata[4]:.2f}%<extra></extra>",
            ))
            fig.update_xaxes(tickprefix="$", tickformat="~s", nticks=5)
            fig.update_yaxes(showline=False)
            fig.update_layout(uniformtext=dict(minsize=10, mode="hide"))
            chart(fig, 380)
            lead = top10_df.sort_values("total_revenue").iloc[-1]
            best_margin = top10_df.loc[top10_df["profit_margin_pct"].idxmax()]
            html(ui.insight(
                f"<b>{ui.esc(lead['product_name'])}</b> chiếm <b>{lead['total_revenue'] * 100 / top10_df['total_revenue'].sum():.1f}%</b> "
                f"doanh thu của nhóm Top 10; biên lợi nhuận cao nhất thuộc <b>{ui.esc(best_margin['product_name'])}</b> "
                f"({best_margin['profit_margin_pct']:.1f}%)."
            ))
    with c4, panel():
        html(ui.card_head("Phân bố trạng thái đơn hàng", "Tỷ lệ theo số đơn · toàn bộ lịch sử.", "grid"))
        if not status_df.empty:
            sd = status_df.sort_values("order_count", ascending=False)
            fig = go.Figure(go.Pie(
                labels=sd["order_status"], values=sd["order_count"], hole=0.66, sort=False,
                marker=dict(colors=[ui.STATUS_COLORS.get(s, ui.SLATE) for s in sd["order_status"]], line=dict(color="#fff", width=2)),
                textinfo="percent", textfont=dict(size=11), insidetextorientation="horizontal",
                customdata=sd["total_value"],
                hovertemplate="<b>%{label}</b><br>%{value:,} đơn (%{percent})<br>Giá trị: $%{customdata:,.0f}<extra></extra>",
            ))
            fig.update_layout(
                legend=dict(orientation="v", x=1.0, y=0.5, yanchor="middle", xanchor="left"),
                annotations=[dict(text=f"<b>{int(sd['order_count'].sum()):,}</b><br><span style='font-size:11px;color:{ui.MUTED}'>đơn hàng</span>",
                                  x=0.5, y=0.5, showarrow=False, font=dict(size=17, color=ui.NAVY))],
                margin=dict(l=8, r=8, t=8, b=8),
            )
            chart(fig, 330)
            lost = sd[sd["order_status"].isin(["Cancelled", "Returned", "Refunded"])]["order_count"].sum()
            done = sd[sd["order_status"] == "Completed"]["order_count"].sum()
            total = max(1, sd["order_count"].sum())
            html(ui.insight(
                f"<b>{done * 100 / total:.1f}%</b> đơn hoàn tất; hủy, trả và hoàn tiền cộng lại "
                f"<b>{lost * 100 / total:.1f}%</b>."
            ))

    with panel():
        html(ui.card_head("Phân khúc khách hàng RFM (K-Means, k = 4)",
                          "So sánh tỷ trọng khách hàng với tỷ trọng doanh thu của từng phân khúc.", "users"))
        if not seg_df.empty:
            sg = seg_df.sort_values("segment_id")
            sg = sg.assign(cust_pct=sg["customer_count"] * 100 / max(1, sg["customer_count"].sum()))
            r1, r2 = st.columns([1, 1.25])
            with r1:
                fig = go.Figure()
                fig.add_bar(y=sg["segment_name"], x=sg["cust_pct"], orientation="h", name="% khách hàng",
                            marker_color="rgba(148,163,184,.55)", hovertemplate="%{x:.1f}% khách hàng<extra></extra>")
                fig.add_bar(y=sg["segment_name"], x=sg["share_of_revenue_pct"], orientation="h", name="% doanh thu",
                            marker_color=ui.PRIMARY, hovertemplate="%{x:.1f}% doanh thu<extra></extra>")
                fig.update_layout(barmode="group", bargap=0.32, hovermode="y unified")
                fig.update_xaxes(ticksuffix="%")
                fig.update_yaxes(autorange="reversed", showline=False)
                chart(fig, 300)
            with r2:
                data_table(
                    [{"label": "Phân khúc", "hl": True}, {"label": "Khách hàng", "align": "right"},
                     {"label": "Recency", "align": "right"}, {"label": "Tần suất", "align": "right"},
                     {"label": "Chi tiêu TB", "align": "right"}],
                    [[(r.segment_name, r.segment_name), (r.customer_count, f"{int(r.customer_count):,}"),
                      (r.avg_recency_days, f"{r.avg_recency_days:.1f} ngày"), (r.avg_frequency_orders, f"{r.avg_frequency_orders:.2f} đơn"),
                      (r.avg_monetary_spend, f"${r.avg_monetary_spend:,.0f}")] for r in sg.itertuples()],
                    page_size=4,
                )
            top_seg = sg.loc[(sg["share_of_revenue_pct"] - sg["cust_pct"]).idxmax()]
            html(ui.insight(
                f"<b>{ui.esc(top_seg['segment_name'])}</b> chiếm <b>{top_seg['cust_pct']:.1f}%</b> khách hàng nhưng tạo "
                f"<b>{top_seg['share_of_revenue_pct']:.1f}%</b> doanh thu, chi tiêu TB {money(top_seg['avg_monetary_spend'])}."
            ))

    with panel():
        html(ui.card_head("Bảng Top 10 sản phẩm", "Bấm tiêu đề cột để sắp xếp.", "file"))
        if not top10_df.empty:
            data_table(
                [{"label": "#", "align": "right"}, {"label": "Mã SP"}, {"label": "Sản phẩm", "hl": True}, {"label": "Thương hiệu"},
                 {"label": "Danh mục"}, {"label": "Đã bán", "align": "right"}, {"label": "Doanh thu", "align": "right"},
                 {"label": "Giá TB", "align": "right"}, {"label": "Biên LN", "align": "right"}],
                [[(r.rank_id, str(r.rank_id)), (r.product_id, r.product_id), (r.product_name, r.product_name), (r.brand, r.brand),
                  (r.category, pretty_category(r.category)), (r.total_units_sold, f"{int(r.total_units_sold):,}"),
                  (r.total_revenue, f"${r.total_revenue:,.2f}"), (r.avg_price, f"${r.avg_price:,.2f}"),
                  (r.profit_margin_pct, f"{r.profit_margin_pct:.2f}%")] for r in top10_df.sort_values("rank_id").itertuples()],
                page_size=10,
            )


# =============================================================================
# 3. Real-time streaming
# =============================================================================
def regenerate_clickstream() -> None:
    db = get_db_client()
    gen = ClickstreamEventGenerator(seed=int(pd.Timestamp.now().timestamp()) % 10000)
    events = gen.generate_batch(count=1800, step_ms=100)
    proc = ClickstreamStreamProcessor(db_client=db)
    proc.process_clickstream_events_windowed(events)
    load_serving_tables.clear()
    st.session_state["_toast"] = "Đã cập nhật cửa sổ trượt 1m/10s từ luồng clickstream mới."


def sliding_window_svg(overlap: int) -> str:
    win, slide = vi_duration(STREAMING_CONFIG.window_duration), vi_duration(STREAMING_CONFIG.slide_duration)
    n = min(overlap, 6)
    x0, unit, w = 18, 40, 6 * 40
    rows = []
    for i in range(n):
        x = x0 + i * unit
        y = 26 + i * 22
        rows.append(
            f'<rect x="{x}" y="{y}" width="{w}" height="14" rx="7" fill="{ui.PRIMARY}" fill-opacity="{0.14 + i * 0.07:.2f}" '
            f'stroke="{ui.PRIMARY}" stroke-opacity=".25"/>'
            f'<text x="{x + 8}" y="{y + 11}" font-size="11" fill="{ui.NAVY}" font-weight="600">W{i + 1}</text>'
        )
    ev_x = x0 + (n - 1) * unit + 22
    ticks = "".join(
        f'<line x1="{x0 + k * unit}" y1="{26 + n * 22 + 4}" x2="{x0 + k * unit}" y2="{26 + n * 22 + 10}" stroke="{ui.SLATE}"/>'
        f'<text x="{x0 + k * unit}" y="{26 + n * 22 + 24}" font-size="11" fill="{ui.MUTED}" text-anchor="middle">+{k * 10}s</text>'
        for k in range(0, 12, 2)
    )
    h = 26 + n * 22 + 32
    return (
        f'<svg viewBox="0 0 {x0 * 2 + 11 * unit} {h}" style="width:100%;height:auto;font-family:{ui.FONT}" role="img" '
        f'aria-label="Minh họa cửa sổ trượt {win}, bước {slide}">'
        f'{"".join(rows)}'
        f'<line x1="{ev_x}" y1="14" x2="{ev_x}" y2="{26 + n * 22}" stroke="{ui.NAVY}" stroke-dasharray="3 3"/>'
        f'<circle cx="{ev_x}" cy="14" r="5" fill="{ui.NAVY}"/>'
        f'<text x="{ev_x + 10}" y="18" font-size="12" fill="{ui.NAVY}" font-weight="650">1 sự kiện → {n} cửa sổ</text>'
        f"{ticks}</svg>"
    )


def render_streaming(t: Dict[str, pd.DataFrame]) -> None:
    stream_df, stream_cat_df = t["streaming_window_metrics"], t["streaming_category_metrics"]
    overlap = window_overlap()

    sw = stream_df.copy()
    if not sw.empty:
        sw["ws"] = pd.to_datetime(sw["window_start"])
        sw = sw.sort_values("ws")
    badge = '<span class="badge off">Chưa có dữ liệu</span>'
    if not sw.empty:
        last_end = pd.to_datetime(sw["window_end"]).max()
        now_utc = pd.Timestamp.now(tz="UTC").tz_localize(None)
        if (now_utc - last_end) <= pd.Timedelta(minutes=3):
            badge = '<span class="badge is-live"><span class="dot"></span>Live</span>'
        else:
            badge = f'<span class="badge snap">{ui.icon("clock", 13)}Ảnh chụp · cửa sổ cuối {last_end:%H:%M:%S} UTC</span>'

    h1, h2 = st.columns([5, 1.6])
    with h1:
        html(ui.page_head(
            "Real-time Streaming",
            f"Kafka → Spark Structured Streaming theo event-time: cửa sổ {vi_duration(STREAMING_CONFIG.window_duration)} "
            f"trượt mỗi {vi_duration(STREAMING_CONFIG.slide_duration)}, watermark {vi_duration(STREAMING_CONFIG.watermark_delay)}. "
            + badge,
        ))
    with h2:
        st.write("")
        st.write("")
        st.button("Sinh luồng clickstream mới", type="primary", use_container_width=True,
                  on_click=regenerate_clickstream, key="gen_stream",
                  help="Sinh 1.800 sự kiện mới và tính lại cửa sổ trượt ở tầng Speed")

    if sw.empty:
        st.info("Chưa có cửa sổ trượt nào trong tầng Serving. Bấm “Sinh luồng clickstream mới” để tạo dữ liệu.")
        return

    # ---- Live strip (latest window vs previous)
    cur = sw.iloc[-1]
    prev = sw.iloc[-2] if len(sw) > 1 else None

    def cell(label: str, value: str, key: str, kind: str = "count") -> str:
        delta = ""
        if prev is not None:
            d = float(cur[key]) - float(prev[key])
            cls = "up" if d > 0 else "down" if d < 0 else "flat"
            sign = "+" if d > 0 else "−" if d < 0 else "±"
            if kind == "money":
                txt = f"{sign}{money(abs(d))}"
            elif kind == "pp":
                txt = f"{sign}{abs(d):.2f} điểm %"
            else:
                txt = f"{sign}{abs(int(d)):,}"
            delta = f'<span class="trend {cls}">{txt}</span>'
        return f'<div class="live-cell"><span class="lc-l">{label}</span><b>{value}</b>{delta}</div>'

    with panel():
        html(ui.card_head(
            "Cửa sổ gần nhất",
            f"{pd.Timestamp(cur['ws']):%H:%M:%S} → {pd.to_datetime(cur['window_end']):%H:%M:%S} · so với cửa sổ liền trước",
            "activity",
        ))
        html(
            '<div class="live">'
            + cell("Sự kiện", f"{int(cur['total_events']):,}", "total_events")
            + cell("Người dùng", f"{int(cur['active_users']):,}", "active_users")
            + cell("Phiên", f"{int(cur['active_sessions']):,}", "active_sessions")
            + cell("Lượt mua", f"{int(cur['purchase_count']):,}", "purchase_count")
            + cell("Chuyển đổi", f"{float(cur['conversion_rate_pct']):.2f}%", "conversion_rate_pct", kind="pp")
            + cell("Doanh số", money(cur["gross_sales"]), "gross_sales", kind="money")
            + "</div>"
        )

    rng = st.radio("Phạm vi", ["Toàn bộ", "10 phút cuối", "5 phút cuối"], horizontal=True,
                   label_visibility="collapsed", key="s_range")
    minutes = {"10 phút cuối": 10, "5 phút cuối": 5}.get(rng)
    view = sw if minutes is None else sw[sw["ws"] >= sw["ws"].max() - pd.Timedelta(minutes=minutes)]
    cat_view = stream_cat_df
    if minutes is not None and not stream_cat_df.empty:
        cat_view = stream_cat_df[pd.to_datetime(stream_cat_df["window_start"]) >= view["ws"].min()]

    c1, c2 = st.columns([3, 2])
    with c1, panel():
        html(ui.card_head("Chỉ số theo cửa sổ trượt", "Mỗi điểm là một cửa sổ, đặt tại thời điểm bắt đầu (window_start).", "activity"))
        fig = go.Figure()
        for col, name, color, width in [("total_events", "Tổng sự kiện", ui.NAVY, 2.4),
                                        ("active_users", "Người dùng hoạt động", ui.ACCENT, 2.2),
                                        ("purchase_count", "Lượt mua", ui.PRIMARY, 2.2)]:
            fig.add_scatter(x=view["ws"], y=view[col], name=name, mode="lines", line=dict(color=color, width=width, shape="spline", smoothing=0.5),
                            hovertemplate="%{y:,}<extra>" + name + "</extra>")
        fig.update_layout(hovermode="x unified")
        fig.update_xaxes(tickformat="%H:%M:%S")
        chart(fig, 320)
        peak = view.loc[view["total_events"].idxmax()]
        html(ui.insight(
            f"Cửa sổ đông nhất bắt đầu lúc <b>{pd.Timestamp(peak['ws']):%H:%M:%S}</b> với <b>{int(peak['total_events']):,}</b> sự kiện "
            f"và {int(peak['active_users']):,} người dùng. Các cửa sổ ở hai đầu ít sự kiện hơn vì chỉ phủ một phần luồng dữ liệu."
        ))
    with c2, panel():
        html(ui.card_head("Phễu chuyển đổi clickstream", f"Cộng dồn theo cửa sổ trượt; mỗi sự kiện được đếm ở ~{overlap} cửa sổ nên tỷ lệ giữa các bước không đổi.", "pointer"))
        views_n, carts_n, buys_n = int(view["view_count"].sum()), int(view["cart_count"].sum()), int(view["purchase_count"].sum())
        fig = go.Figure(go.Funnel(
            y=["Xem sản phẩm", "Thêm giỏ hàng", "Mua hàng"], x=[views_n, carts_n, buys_n],
            texttemplate="%{value:,}<br>%{percentInitial:.1%}", textfont=dict(color=[ui.NAVY, "#fff", "#fff"], size=12),
            marker=dict(color=["#93C5FD", ui.ACCENT, ui.PRIMARY]),
            connector=dict(fillcolor="rgba(37,99,235,.06)", line=dict(width=0)),
            hovertemplate="<b>%{y}</b><br>%{x:,} lượt<br>%{percentInitial:.1%} so với bước đầu<extra></extra>",
        ))
        fig.update_yaxes(showline=False)
        chart(fig, 320)
        html(ui.insight(
            f"<b>{carts_n * 100 / max(1, views_n):.1f}%</b> lượt xem dẫn tới thêm giỏ; "
            f"tỷ lệ mua trên lượt xem là <b>{buys_n * 100 / max(1, views_n):.1f}%</b>."
        ))

    c3, c4 = st.columns([3, 2])
    with c3, panel():
        html(ui.card_head("Phân bố sự kiện theo danh mục", "Loại tương tác trên từng danh mục trong phạm vi đã chọn.", "bars"))
        if not cat_view.empty:
            ce = cat_view.groupby(["category", "event_type"], as_index=False)["event_count"].sum()
            order = ce.groupby("category")["event_count"].sum().sort_values().index.tolist()
            fig = go.Figure()
            for et in ["view", "cart", "remove_from_cart", "purchase"]:
                sub = ce[ce["event_type"] == et].set_index("category").reindex(order).fillna(0)
                fig.add_bar(y=[pretty_category(c) for c in order], x=sub["event_count"], orientation="h",
                            name=ui.EVENT_LABELS[et], marker_color=ui.EVENT_COLORS[et],
                            hovertemplate="%{x:,}<extra>" + ui.EVENT_LABELS[et] + "</extra>")
            fig.update_layout(barmode="stack", hovermode="y unified", bargap=0.3)
            fig.update_yaxes(showline=False)
            chart(fig, 380)
    with c4, panel():
        html(ui.card_head("Cửa sổ trượt hoạt động thế nào",
                          f"Cửa sổ dài {vi_duration(STREAMING_CONFIG.window_duration)}, mở mới mỗi "
                          f"{vi_duration(STREAMING_CONFIG.slide_duration)}, nên các cửa sổ chồng lên nhau.", "layers"))
        html(f'<div style="padding:6px 10px 2px">{sliding_window_svg(overlap)}</div>')
        html(ui.insight(
            f"Một sự kiện rơi vào <b>{overlap}</b> cửa sổ liên tiếp. Vì vậy cộng các cửa sổ lại sẽ ra số lượt lớn hơn số sự kiện thật "
            f"khoảng {overlap} lần; dùng cửa sổ đơn lẻ để đọc lưu lượng tại một thời điểm."
        ))

    c5, c6 = st.columns([3, 2])
    with c5, panel():
        html(ui.card_head("Minh họa watermark", "Sự kiện đến muộn được nhận hoặc bị loại theo từng cửa sổ.", "clock"))
        fig = go.Figure()
        fig.add_bar(x=view["ws"], y=view["late_events_accepted"], name=f"Được nhận (≤ {vi_duration(STREAMING_CONFIG.watermark_delay)})",
                    marker_color=ui.ACCENT, hovertemplate="%{y:,} được nhận<extra></extra>")
        fig.add_bar(x=view["ws"], y=view["late_events_dropped"], name=f"Bị loại (> {vi_duration(STREAMING_CONFIG.watermark_delay)})",
                    marker_color="#F08A8A", hovertemplate="%{y:,} bị loại<extra></extra>")
        fig.update_layout(barmode="group", hovermode="x unified", bargap=0.15)
        fig.update_xaxes(tickformat="%H:%M:%S")
        fig.update_yaxes(title_text="Sự kiện trễ")
        chart(fig, 300)
    with c6, panel():
        html(ui.card_head("Quy tắc watermark", "Cách tầng Speed xử lý dữ liệu đến muộn.", "shield"))
        html(ui.spec_list([
            ("Ngưỡng watermark", vi_duration(STREAMING_CONFIG.watermark_delay)),
            ("Tỷ lệ sự kiện trễ (generator)", f"{KAFKA_CONFIG.late_data_probability * 100:.0f}%"),
            ("Độ trễ tối đa sinh ra", f"{KAFKA_CONFIG.max_late_minutes} phút"),
            ("Trễ được nhận (cửa sổ cuối)", f"{int(cur['late_events_accepted']):,}"),
            ("Trễ bị loại (cửa sổ cuối)", f"{int(cur['late_events_dropped']):,}"),
            ("Cửa sổ có sự kiện bị loại", f"{int((view['late_events_dropped'] > 0).sum()):,} / {len(view):,}"),
        ]))
        html(ui.insight(
            "Sự kiện trễ trong ngưỡng vẫn được cộng vào đúng cửa sổ theo event-time; sự kiện vượt ngưỡng bị bỏ để "
            "giải phóng state store."
        ))

    with panel():
        html(ui.card_head("Bảng chỉ số cửa sổ trượt", "streaming_window_metrics · mới nhất trước · bấm tiêu đề để sắp xếp.", "file"))
        tbl = view.sort_values("ws", ascending=False)
        data_table(
            [{"label": "Bắt đầu"}, {"label": "Kết thúc"}, {"label": "Sự kiện", "align": "right", "hl": True},
             {"label": "Người dùng", "align": "right"}, {"label": "Phiên", "align": "right"}, {"label": "Xem", "align": "right"},
             {"label": "Giỏ", "align": "right"}, {"label": "Bỏ giỏ", "align": "right"}, {"label": "Mua", "align": "right"},
             {"label": "Doanh số", "align": "right"}, {"label": "Chuyển đổi", "align": "right"},
             {"label": "Trễ nhận", "align": "right"}, {"label": "Trễ loại", "align": "right"}],
            [[(str(r.window_start), str(r.window_start)[11:19]), (str(r.window_end), str(r.window_end)[11:19]),
              (r.total_events, f"{int(r.total_events):,}"), (r.active_users, f"{int(r.active_users):,}"),
              (r.active_sessions, f"{int(r.active_sessions):,}"), (r.view_count, f"{int(r.view_count):,}"),
              (r.cart_count, f"{int(r.cart_count):,}"), (r.remove_cart_count, f"{int(r.remove_cart_count):,}"),
              (r.purchase_count, f"{int(r.purchase_count):,}"), (r.gross_sales, f"${r.gross_sales:,.2f}"),
              (r.conversion_rate_pct, f"{r.conversion_rate_pct:.2f}%"), (r.late_events_accepted, f"{int(r.late_events_accepted):,}"),
              (r.late_events_dropped, f"{int(r.late_events_dropped):,}")] for r in tbl.itertuples()],
            page_size=10,
        )


# =============================================================================
# 4. Performance benchmark
# =============================================================================
ARCH_ORDER = ["CSV", "Parquet", "Partitioned Parquet"]


TIE_PCT = 5.0


def ranked(values: pd.Series) -> tuple[list[str], float]:
    """Labels tied with the minimum (within TIE_PCT) and the gap to the next value in %."""
    v = values.sort_values()
    best = float(v.iloc[0])
    tied = [k for k, x in v.items() if best == 0 or (float(x) - best) * 100 / best < TIE_PCT]
    rest = v.drop(tied)
    gap = (float(rest.iloc[0]) - best) * 100 / best if len(rest) and best else 0.0
    return tied, gap


def benchmark_metric_chart(bd: pd.DataFrame, col: str, unit: str, fmt: str, title_y: str) -> go.Figure:
    fig = go.Figure()
    qmin = bd.groupby("query_id")[col].transform("min")
    winners = (bd[col] - qmin) * 100 / qmin.where(qmin > 0, 1) < TIE_PCT  # every bar within the tie threshold
    for arch in ARCH_ORDER:
        sub = bd[bd["arch"] == arch]
        is_best = winners.loc[sub.index].to_numpy()
        fig.add_bar(
            x=sub["q"], y=sub[col], name=arch,
            marker=dict(color=ui.ARCH_COLORS[arch], opacity=[1.0 if b else 0.38 for b in is_best],
                        line=dict(color=[ui.NAVY if b else "rgba(0,0,0,0)" for b in is_best], width=1.2)),
            text=[f"{v:{fmt}}{unit}" if b else "" for v, b in zip(sub[col], is_best)], textposition="outside", cliponaxis=False,
            textfont=dict(size=11, color=ui.NAVY),
            customdata=sub[["query_name", "speedup_vs_csv", "rows_scanned"]],
            hovertemplate="<b>" + arch + "</b><br>%{customdata[0]}<br>" + title_y + ": %{y:" + fmt + "}" + unit
                          + "<br>Tăng tốc: %{customdata[1]:.2f}× · Dòng: %{customdata[2]:,}<extra></extra>",
        )
    fig.update_layout(barmode="group", bargap=0.24, bargroupgap=0.06, margin=dict(t=30))
    fig.update_yaxes(title_text=title_y)
    return fig


def render_benchmark(t: Dict[str, pd.DataFrame]) -> None:
    bench_df, rg_df = t["benchmark_results"], t["rowgroup_experiments"]
    html(ui.page_head(
        "Performance Benchmark",
        "Đối chứng 4 truy vấn SQL trên 3 mô hình lưu trữ: CSV thô, Parquet không phân vùng và Parquet phân vùng "
        "year/month. Viền đậm đánh dấu kết quả tốt nhất của từng truy vấn.",
    ))
    if bench_df.empty:
        st.info("Chưa có kết quả benchmark trong tầng Serving.")
        return

    bd = bench_df.copy()
    bd["arch"] = bd["storage_architecture"].map(arch_short)
    bd["q"] = bd["query_id"].map(lambda q: str(q).split("_")[0])
    queries = list(dict.fromkeys(bd["query_id"]))

    # ---- Scoreboard
    items = []
    for qid in queries:
        sub = bd[bd["query_id"] == qid].set_index("arch")
        tmax = float(sub["execution_time_sec"].max()) or 1.0
        best_arch = sub["execution_time_sec"].idxmin()
        tied, _ = ranked(sub["execution_time_sec"])
        rows = "".join(
            f'<div class="score-row{" best" if a in tied else ""}"><span>{a}</span>'
            f'<div class="bar"><i style="width:{float(sub.loc[a, "execution_time_sec"]) * 100 / tmax:.1f}%;background:{ui.ARCH_COLORS[a]}"></i></div>'
            f'<b>{float(sub.loc[a, "execution_time_sec"]):.3f}s</b></div>'
            for a in ARCH_ORDER if a in sub.index
        )
        b = sub.loc[best_arch]
        name = str(b["query_name"]).split(":", 1)[-1].strip()
        items.append(
            f'<div class="score-item"><div class="score-q">{ui.esc(query_short(qid))}<div class="kpi-sub" style="font-weight:450">{ui.esc(name)}</div></div>'
            f'{rows}<div class="score-foot">'
            + (f'Nhanh nhất: <b>{best_arch}</b>' if len(tied) == 1 else f'Ngang nhau (chênh dưới {TIE_PCT:g}%): <b>{" và ".join(tied)}</b>')
            + f' · {float(b["speedup_vs_csv"]):.2f}× so với CSV · giảm {float(b["io_reduction_pct"]):.1f}% I/O</div></div>'
        )
    with panel():
        html(ui.card_head("Kết quả theo truy vấn", "Thời gian thực thi của từng mô hình; thanh ngắn hơn là nhanh hơn.", "gauge"))
        html(f'<div class="score">{"".join(items)}</div>')

    c1, c2, c3 = st.columns(3)
    with c1, panel():
        html(ui.card_head("Thời gian thực thi", "Giây · thấp hơn là tốt hơn.", "clock"))
        chart(benchmark_metric_chart(bd, "execution_time_sec", "s", ".3f", "Thời gian (s)"), 320)
    with c2, panel():
        html(ui.card_head("Dung lượng đọc (Bytes read)", "MB · thấp hơn là tốt hơn. Với Parquet đây là ước lượng theo số cột và độ chọn lọc.", "download"))
        chart(benchmark_metric_chart(bd, "bytes_read_mb", "", ".2f", "MB"), 320)
    with c3, panel():
        html(ui.card_head("Bộ nhớ RAM", "MB · RSS của tiến trình sau khi chạy truy vấn.", "cpu"))
        chart(benchmark_metric_chart(bd, "ram_usage_mb", "", ".0f", "MB"), 320)

    # ---- Why the differences (every sentence derived from the measured row and its flags)
    reasons = []
    for qid in queries:
        sub = bd[bd["query_id"] == qid].set_index("arch")
        tied, gap = ranked(sub["execution_time_sec"])
        parts = []
        if "CSV" in sub.index:
            csv = sub.loc["CSV"]
            parts.append(f"So với CSV ({float(csv['bytes_read_mb']):.2f} MB, {float(csv['execution_time_sec']):.3f}s), "
                         "định dạng cột chỉ đọc cột cần thiết và áp dụng <b>predicate pushdown</b>.")
        if {"Parquet", "Partitioned Parquet"} <= set(sub.index):
            pq_row, pt_row = sub.loc["Parquet"], sub.loc["Partitioned Parquet"]
            pq_mb, pt_mb = float(pq_row["bytes_read_mb"]), float(pt_row["bytes_read_mb"])
            if bool(pt_row["partition_pruning_active"]):
                parts.append(f"Lọc theo year/month kích hoạt <b>partition pruning</b>: bản phân vùng đọc {pt_mb:.2f} MB "
                             f"so với {pq_mb:.2f} MB của Parquet không phân vùng.")
            else:
                parts.append(f"Không lọc theo year/month nên không có pruning; bản phân vùng đọc {pt_mb:.2f} MB, "
                             f"{'nhiều' if pt_mb > pq_mb else 'ít'} hơn Parquet ({pq_mb:.2f} MB) vì phải mở nhiều tệp nhỏ.")
        verdict = (f"{tied[0]} nhanh nhất, hơn mô hình kế tiếp {gap:.0f}%." if len(tied) == 1
                   else f"{' và '.join(tied)} ngang nhau (chênh dưới {TIE_PCT:g}%).")
        reasons.append(f"<div class='spec' style='display:block'><b style='text-align:left'>{ui.esc(query_short(qid))} · {verdict}</b>"
                       f"<div class='card-desc'>{' '.join(parts)}</div></div>")

    c4, c5 = st.columns([3, 2])
    with c4, panel():
        html(ui.card_head("Kích thước Row Group (parquet.block.size)",
                          "Thời gian truy vấn trung bình và thời gian ghi theo từng cấu hình.", "layers"))
        if not rg_df.empty:
            rg = rg_df.sort_values("row_group_size_mb")
            x = [f"{int(v)} MB" for v in rg["row_group_size_mb"]]
            fig = make_subplots(specs=[[{"secondary_y": True}]])
            fig.add_bar(x=x, y=rg["write_time_sec"], name="Thời gian ghi (s)", marker_color="rgba(96,165,250,.35)",
                        hovertemplate="%{y:.3f}s<extra>Ghi</extra>", secondary_y=True)
            fig.add_scatter(x=x, y=rg["avg_query_time_sec"], name="Truy vấn TB (s)", mode="lines+markers",
                            line=dict(color=ui.PRIMARY, width=2.6), marker=dict(size=8, color="#fff", line=dict(color=ui.PRIMARY, width=2)),
                            customdata=rg[["storage_size_mb", "scan_volume_mb"]],
                            hovertemplate="%{y:.4f}s<br>Dung lượng %{customdata[0]:.2f} MB · quét ~%{customdata[1]:.2f} MB<extra>Truy vấn</extra>",
                            secondary_y=False)
            fig.update_layout(hovermode="x unified")
            fig.update_yaxes(title_text="Truy vấn TB (s)", secondary_y=False)
            fig.update_yaxes(title_text="Ghi (s)", showgrid=False, secondary_y=True)
            chart(fig, 320)
            rg_tied, _ = ranked(rg.set_index(rg["row_group_size_mb"].astype(int).astype(str) + " MB")["avg_query_time_sec"])
            slowest = rg.loc[rg["avg_query_time_sec"].idxmax()]
            lead = (f"Truy vấn nhanh nhất ở <b>{rg_tied[0]}</b>" if len(rg_tied) == 1
                    else f"<b>{' và '.join(rg_tied)}</b> cho thời gian truy vấn ngang nhau")
            html(ui.insight(
                f"{lead} ({rg['avg_query_time_sec'].min():.4f}s); chậm nhất là <b>{int(slowest['row_group_size_mb'])} MB</b> "
                f"({slowest['avg_query_time_sec']:.4f}s). Lưu ý: backend ánh xạ mỗi mức MB sang số dòng mỗi row group."
            ))
    with c5, panel():
        html(ui.card_head("Vì sao có khác biệt", "Giải thích từ cờ pushdown/pruning và số liệu đo được.", "info"))
        html(f'<div style="padding:0 8px 10px">{"".join(reasons)}</div>')

    with panel():
        html(ui.card_head("Bảng kết quả benchmark", "benchmark_results · bấm tiêu đề cột để sắp xếp.", "file"))
        data_table(
            [{"label": "Truy vấn"}, {"label": "Mô hình", "hl": True}, {"label": "Thời gian", "align": "right"},
             {"label": "Bytes read", "align": "right"}, {"label": "Dòng", "align": "right"}, {"label": "CPU", "align": "right"},
             {"label": "RAM", "align": "right"}, {"label": "Tăng tốc", "align": "right"}, {"label": "Giảm I/O", "align": "right"},
             {"label": "Pushdown"}, {"label": "Pruning"}],
            [[(r.query_id, query_short(r.query_id)), (r.arch, r.arch), (r.execution_time_sec, f"{r.execution_time_sec:.4f}s"),
              (r.bytes_read_mb, f"{r.bytes_read_mb:,.3f} MB"), (r.rows_scanned, f"{int(r.rows_scanned):,}"),
              (r.cpu_utilization_pct, f"{r.cpu_utilization_pct:.1f}%"), (r.ram_usage_mb, f"{r.ram_usage_mb:,.0f} MB"),
              (r.speedup_vs_csv, f"{r.speedup_vs_csv:.2f}×"), (r.io_reduction_pct, f"{r.io_reduction_pct:.2f}%"),
              (str(bool(r.predicate_pushdown_active)), "Có" if bool(r.predicate_pushdown_active) else "Không"),
              (str(bool(r.partition_pruning_active)), "Có" if bool(r.partition_pruning_active) else "Không")]
             for r in bd.itertuples()],
            page_size=12,
        )

    if not rg_df.empty:
        with panel():
            html(ui.card_head("Bảng thực nghiệm Row Group", "rowgroup_experiments · căn chỉnh với HDFS block 128 MB.", "file"))
            data_table(
                [{"label": "Row group", "align": "right", "hl": True}, {"label": "Ghi", "align": "right"},
                 {"label": "Dung lượng", "align": "right"}, {"label": "Truy vấn TB", "align": "right"},
                 {"label": "Quét (ước lượng)", "align": "right"}, {"label": "RAM", "align": "right"}, {"label": "Căn chỉnh HDFS"}],
                [[(r.row_group_size_mb, f"{int(r.row_group_size_mb)} MB"), (r.write_time_sec, f"{r.write_time_sec:.3f}s"),
                  (r.storage_size_mb, f"{r.storage_size_mb:.2f} MB"), (r.avg_query_time_sec, f"{r.avg_query_time_sec:.4f}s"),
                  (r.scan_volume_mb, f"{r.scan_volume_mb:.3f} MB"), (r.memory_peak_mb, f"{r.memory_peak_mb:,.0f} MB"),
                  (r.hdfs_block_alignment, r.hdfs_block_alignment)] for r in rg_df.sort_values("row_group_size_mb").itertuples()],
                page_size=4,
            )

    with panel():
        html(ui.card_head("Kế hoạch thực thi vật lý", "PartitionFilters và PushedFilters của từng mô hình cho truy vấn đã chọn.", "cpu"))
        q_sel = st.radio("Truy vấn", queries, horizontal=True, format_func=query_short, label_visibility="collapsed", key="plan_q")
        sub = bd[bd["query_id"] == q_sel]
        for r in sub.itertuples():
            flags = (
                f'<span class="badge {"ok" if r.predicate_pushdown_active else "off"}">Pushdown {"bật" if r.predicate_pushdown_active else "tắt"}</span> '
                f'<span class="badge {"ok" if r.partition_pruning_active else "off"}">Pruning {"bật" if r.partition_pruning_active else "tắt"}</span>'
            )
            html(f'<div class="plan-head"><span>{ui.esc(r.arch)} · {r.execution_time_sec:.4f}s · {r.speedup_vs_csv:.2f}× · '
                 f'giảm {r.io_reduction_pct:.2f}% I/O</span><span>{flags}</span></div>')
            st.code(str(r.physical_plan_snippet), language="text")


# =============================================================================
# App
# =============================================================================
def render_dashboard() -> None:
    ui.register_plotly_template()
    html(ui.global_css())
    db = get_db_client()
    infra = infrastructure_status()
    view = render_shell(db, infra)

    if "_toast" in st.session_state:
        st.toast(st.session_state.pop("_toast"))

    t = load_serving_tables()
    if t["batch_revenue_summary"].empty or t["benchmark_results"].empty:
        st.warning(
            "Chưa có dữ liệu tổng hợp trong tầng Serving. Hãy chạy script `python scripts/run_local_demo.py` "
            "hoặc khởi động cụm Docker Compose để nạp dữ liệu thực nghiệm."
        )
        return

    if view == "overview":
        render_overview(t, db, infra)
    elif view == "batch":
        render_batch(t)
    elif view == "streaming":
        render_streaming(t)
    else:
        render_benchmark(t)


if __name__ == "__main__":
    render_dashboard()

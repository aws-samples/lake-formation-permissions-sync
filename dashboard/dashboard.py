"""
Lake Formation Permissions Sync — Streamlit Dashboard

Live monitoring dashboard that connects to DynamoDB to show
real-time sync status, events, errors, and configuration.

Usage:
    streamlit run dashboard.py
    streamlit run dashboard.py -- --config config/glue_config.conf
"""

import ast
import os
import time
from collections import Counter
from configparser import ConfigParser

import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# ---------------------------------------------------------------------------
# AWS / boto3 — imported lazily so the app starts even without credentials
# ---------------------------------------------------------------------------
try:
    import boto3
    from botocore.exceptions import NoCredentialsError, ProfileNotFoundError

    HAS_BOTO = True
except ImportError:
    HAS_BOTO = False


# ──────────────────────────────────────────────────────────────────────────────
# Page config (must be first Streamlit command)
# ──────────────────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Lake Formation Sync",
    page_icon="🔄",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ──────────────────────────────────────────────────────────────────────────────
# Custom CSS for a cleaner look
# ──────────────────────────────────────────────────────────────────────────────
st.markdown(
    """
<style>
    /* Tighter metric cards */
    [data-testid="stMetric"] {
        background: white;
        border: 1px solid #e5e7eb;
        border-radius: 12px;
        padding: 16px 20px;
    }
    [data-testid="stMetricLabel"] { font-size: 13px; }
    /* Sidebar styling */
    [data-testid="stSidebar"] { background: #fafafa; }
    /* Dataframe full width */
    .stDataFrame { width: 100%; }
    /* Section headers */
    .config-section {
        background: white;
        border: 1px solid #e5e7eb;
        border-radius: 12px;
        padding: 20px;
        margin-bottom: 16px;
    }
</style>
""",
    unsafe_allow_html=True,
)


# ──────────────────────────────────────────────────────────────────────────────
# Config file handling
# ──────────────────────────────────────────────────────────────────────────────
DEFAULT_CONFIG_PATHS = [
    "config/glue_config.conf",
]


@st.cache_data(ttl=60)
def load_config_file(path: str) -> ConfigParser:
    """Load a .conf file into a ConfigParser."""
    config = ConfigParser()
    if os.path.exists(path):
        config.read(path)
    return config


def find_config_path() -> str:
    """Find the first config file that exists."""
    for p in DEFAULT_CONFIG_PATHS:
        if os.path.exists(p):
            return p
    return DEFAULT_CONFIG_PATHS[0]


def parse_config(config: ConfigParser) -> dict:
    """Extract dashboard-friendly dict from the config file."""
    data = {
        "source_region": "us-east-1",
        "destination_region": "us-west-2",
        "sync_glue_catalog": True,
        "sync_lf_permissions": True,
        "delete_target_catalog_objects": False,
        "update_table_s3_location": False,
        "rewrite_iceberg_metadata": False,
        "database_list": [],
        "s3_bucket_mapping": {},
        "cloudtrail_lookup_hours": 1,
        "backup_file_bucket": "",
        "backup_file_folder": "",
        "lf_storage_bucket": "",
        "lf_storage_folder": "",
        "lf_storage_file_name": "",
        "lambda_runtime": "python3.13",
        "dlq_name": "lfDRDeadLetterQueue",
    }

    if config.has_section("AwsDataCatalog"):
        sec = config["AwsDataCatalog"]
        data["source_region"] = sec.get("source_region", data["source_region"])
        data["destination_region"] = sec.get("destination_region", data["destination_region"])
        data["cloudtrail_lookup_hours"] = int(sec.get("cloudtrail_lookup_hour_duration", "1"))
        data["backup_file_bucket"] = sec.get("backup_file_bucket", "")
        data["backup_file_folder"] = sec.get("backup_file_folder", "")
        try:
            data["database_list"] = ast.literal_eval(sec.get("database_list", "[]"))
        except (ValueError, SyntaxError):
            data["database_list"] = []
        try:
            raw = sec.get("target_s3_locations", sec.get("s3bucketmapping", "{}"))
            data["s3_bucket_mapping"] = ast.literal_eval(raw)
        except (ValueError, SyntaxError):
            data["s3_bucket_mapping"] = {}

    if config.has_section("Operation"):
        sec = config["Operation"]
        data["sync_glue_catalog"] = sec.getboolean("sync_glue_catalog", True)
        data["sync_lf_permissions"] = sec.getboolean("sync_lf_permissions", True)
        data["delete_target_catalog_objects"] = sec.getboolean("delete_target_catalog_objects", False)

    if config.has_section("Target_s3_update"):
        sec = config["Target_s3_update"]
        data["update_table_s3_location"] = sec.getboolean("update_table_s3_location", False)
        data["rewrite_iceberg_metadata"] = sec.getboolean("rewrite_iceberg_metadata", fallback=False)

    if config.has_section("LakeFormationPermissions"):
        sec = config["LakeFormationPermissions"]
        data["lf_storage_bucket"] = sec.get("lf_storage_bucket", "")
        data["lf_storage_folder"] = sec.get("lf_storage_file_folder", "")
        data["lf_storage_file_name"] = sec.get("lf_storage_file_name", "")

    if config.has_section("Lambda"):
        sec = config["Lambda"]
        data["lambda_runtime"] = sec.get("runtime", data["lambda_runtime"])
        data["dlq_name"] = sec.get("dlq_name", data["dlq_name"])

    return data


# ──────────────────────────────────────────────────────────────────────────────
# DynamoDB data fetching
# ──────────────────────────────────────────────────────────────────────────────
DYNAMODB_TABLE_NAME = os.environ.get("DYNAMODB_TABLE", "glue_lf_events")


def get_ddb_resource(region: str):
    """Return a DynamoDB Table resource."""
    if not HAS_BOTO:
        return None
    try:
        ddb = boto3.resource("dynamodb", region_name=region)
        return ddb.Table(DYNAMODB_TABLE_NAME)
    except (NoCredentialsError, ProfileNotFoundError):
        return None


@st.cache_data(ttl=30, show_spinner="Fetching events from DynamoDB...")
def fetch_events(region: str, limit: int = 200) -> list[dict]:
    """Scan the events table and return rows."""
    table = get_ddb_resource(region)
    if table is None:
        return []
    try:
        items = []
        resp = table.scan(Limit=limit)
        items.extend(resp.get("Items", []))
        while "LastEvaluatedKey" in resp and len(items) < limit:
            resp = table.scan(ExclusiveStartKey=resp["LastEvaluatedKey"], Limit=limit - len(items))
            items.extend(resp.get("Items", []))
        return items
    except Exception:
        return []


def compute_stats(events: list[dict]) -> dict:
    """Derive dashboard metrics from raw DynamoDB events."""
    total = len(events)
    processed = sum(1 for e in events if e.get("Processed") == "Y")
    failed = sum(1 for e in events if e.get("Processed") == "N")
    pending = total - processed - failed

    # Checkpoint (high-water-mark row)
    checkpoint_time = None
    for e in events:
        if e.get("EventId") == "CHECKPOINT" and "LastEventTime" in e:
            checkpoint_time = e["LastEventTime"]
            break

    # Event type breakdown
    type_counts = Counter()
    hourly_counts = Counter()
    for e in events:
        if e.get("EventId") == "CHECKPOINT":
            continue
        etype = e.get("EventName", e.get("eventName", "Unknown"))
        type_counts[etype] += 1
        ts = e.get("EventTime", e.get("eventTime", ""))
        if ts:
            try:
                s = str(ts)
                # Handle both YYYYMMDDHHMMSS and YYYY-MM-DD HH formats
                if len(s) >= 10 and s[4:5] != "-":
                    hour = f"{s[:4]}-{s[4:6]}-{s[6:8]} {s[8:10]}"
                else:
                    hour = s[:13]  # Already YYYY-MM-DD HH
                hourly_counts[hour] += 1
            except Exception:
                pass

    # Success rate
    success_rate = (processed / total * 100) if total > 0 else 0.0

    return {
        "total": total,
        "processed": processed,
        "failed": failed,
        "pending": pending,
        "success_rate": success_rate,
        "checkpoint_time": checkpoint_time,
        "type_counts": dict(type_counts.most_common(20)),
        "hourly_counts": dict(sorted(hourly_counts.items())),
    }


# ──────────────────────────────────────────────────────────────────────────────
# Sidebar navigation
# ──────────────────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("### 🔄 LF Sync")
    st.caption("Lake Formation Permissions Sync")
    st.divider()

    page = st.radio(
        "Navigate",
        ["Dashboard", "Events", "Configuration"],
        label_visibility="collapsed",
    )

    st.divider()

    # Connection status
    config_path = find_config_path()
    raw_config = load_config_file(config_path)
    cfg = parse_config(raw_config)

    st.caption("Connection")
    st.text(f"{cfg['source_region']} → {cfg['destination_region']}")

    # Auto-refresh toggle
    auto_refresh = st.toggle("Auto-refresh (30s)", value=False)
    if auto_refresh:
        time.sleep(0.1)  # prevent tight loop on first render
        st.rerun()


# ──────────────────────────────────────────────────────────────────────────────
# Fetch data once per page load
# ──────────────────────────────────────────────────────────────────────────────
events = fetch_events(cfg["source_region"])
stats = compute_stats(events)


# ══════════════════════════════════════════════════════════════════════════════
# PAGE: Dashboard
# ══════════════════════════════════════════════════════════════════════════════
if page == "Dashboard":
    st.title("Overview")

    # ── Metric cards ──
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total events", f"{stats['total']:,}")
    with col2:
        st.metric("Processed", f"{stats['processed']:,}", delta=f"{stats['success_rate']:.1f}% success")
    with col3:
        st.metric(
            "Failed",
            f"{stats['failed']:,}",
            delta=None if stats["failed"] == 0 else f"{stats['failed']} errors",
            delta_color="inverse",
        )
    with col4:
        checkpoint_display = stats["checkpoint_time"] or "—"
        st.metric("Checkpoint", str(checkpoint_display)[:19])

    st.divider()

    # ── Charts row ──
    chart_col1, chart_col2 = st.columns(2)

    with chart_col1:
        st.subheader("Event activity")
        if stats["hourly_counts"]:
            hours = list(stats["hourly_counts"].keys())
            counts = list(stats["hourly_counts"].values())
            fig = go.Figure()
            fig.add_trace(
                go.Scatter(
                    x=hours,
                    y=counts,
                    mode="lines+markers",
                    fill="tozeroy",
                    line=dict(color="#6366f1", width=2),
                    fillcolor="rgba(99,102,241,0.1)",
                )
            )
            fig.update_layout(
                height=300,
                margin=dict(l=0, r=0, t=10, b=0),
                xaxis_title="Time",
                yaxis_title="Events",
                template="plotly_white",
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No event data available. Connect to DynamoDB to see live activity.")

    with chart_col2:
        st.subheader("Events by type")
        if stats["type_counts"]:
            types = list(stats["type_counts"].keys())
            vals = list(stats["type_counts"].values())
            fig = px.bar(x=vals, y=types, orientation="h", color_discrete_sequence=["#6366f1"])
            fig.update_layout(
                height=300,
                margin=dict(l=0, r=0, t=10, b=0),
                xaxis_title="Count",
                yaxis_title="",
                template="plotly_white",
                showlegend=False,
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No event type data available.")

    st.divider()

    # ── Recent events table ──
    st.subheader("Recent events")
    if events:
        import pandas as pd

        display_events = [e for e in events if e.get("EventId") != "CHECKPOINT"]
        display_events.sort(key=lambda e: e.get("EventTime", e.get("eventTime", "")), reverse=True)

        rows = []
        for e in display_events[:25]:
            rows.append(
                {
                    "Event ID": str(e.get("EventId", ""))[:12],
                    "Event Name": e.get("EventName", e.get("eventName", "—")),
                    "Event Time": str(e.get("EventTime", e.get("eventTime", "—")))[:19],
                    "Processed": e.get("Processed", "—"),
                    "Database": e.get("DatabaseName", e.get("databaseName", "—")),
                    "Table": e.get("TableName", e.get("tableName", "—")),
                }
            )
        df = pd.DataFrame(rows)
        st.dataframe(
            df,
            use_container_width=True,
            hide_index=True,
            column_config={
                "Processed": st.column_config.TextColumn(
                    "Status",
                    help="Y = processed, N = failed",
                    width="small",
                ),
            },
        )
    else:
        st.info(
            "No events found. This could mean:\n"
            "- AWS credentials are not configured (`aws configure`)\n"
            "- The DynamoDB table doesn't exist yet (deploy the CDK stack first)\n"
            "- No events have been processed yet"
        )

    # ── Attention needed ──
    failed_events = [e for e in events if e.get("Processed") == "N"]
    if failed_events:
        st.divider()
        st.subheader("⚠️ Attention needed")
        for e in failed_events[:5]:
            with st.expander(
                f"**{e.get('EventName', 'Unknown')}** — {str(e.get('EventTime', ''))[:19]}", expanded=False
            ):
                st.json(e)


# ══════════════════════════════════════════════════════════════════════════════
# PAGE: Events
# ══════════════════════════════════════════════════════════════════════════════
elif page == "Events":
    st.title("Events")

    # ── Filters ──
    filter_col1, filter_col2, filter_col3 = st.columns([2, 1, 1])
    with filter_col1:
        search = st.text_input("Search events", placeholder="Filter by event name, database, or table...")
    with filter_col2:
        status_filter = st.selectbox("Status", ["All", "Processed (Y)", "Failed (N)", "Pending"])
    with filter_col3:
        if st.button("🔄 Refresh", use_container_width=True):
            st.cache_data.clear()
            st.rerun()

    st.divider()

    # ── Filter events ──
    display_events = [e for e in events if e.get("EventId") != "CHECKPOINT"]

    if search:
        search_lower = search.lower()
        display_events = [
            e
            for e in display_events
            if search_lower in str(e.get("EventName", "")).lower()
            or search_lower in str(e.get("DatabaseName", e.get("databaseName", ""))).lower()
            or search_lower in str(e.get("TableName", e.get("tableName", ""))).lower()
        ]

    if status_filter == "Processed (Y)":
        display_events = [e for e in display_events if e.get("Processed") == "Y"]
    elif status_filter == "Failed (N)":
        display_events = [e for e in display_events if e.get("Processed") == "N"]
    elif status_filter == "Pending":
        display_events = [e for e in display_events if e.get("Processed") not in ("Y", "N")]

    display_events.sort(key=lambda e: e.get("EventTime", e.get("eventTime", "")), reverse=True)

    # ── Summary metrics ──
    mcol1, mcol2, mcol3 = st.columns(3)
    with mcol1:
        st.metric("Showing", f"{len(display_events):,} events")
    with mcol2:
        processed_count = sum(1 for e in display_events if e.get("Processed") == "Y")
        st.metric("Processed", f"{processed_count:,}")
    with mcol3:
        failed_count = sum(1 for e in display_events if e.get("Processed") == "N")
        st.metric("Failed", f"{failed_count:,}")

    # ── Events table ──
    if display_events:
        import pandas as pd

        rows = []
        for e in display_events:
            rows.append(
                {
                    "Event ID": str(e.get("EventId", "")),
                    "Event Name": e.get("EventName", e.get("eventName", "—")),
                    "Event Time": str(e.get("EventTime", e.get("eventTime", "—")))[:19],
                    "Processed": e.get("Processed", "—"),
                    "Database": e.get("DatabaseName", e.get("databaseName", "—")),
                    "Table": e.get("TableName", e.get("tableName", "—")),
                    "Source": e.get("EventSource", e.get("eventSource", "—")),
                }
            )
        df = pd.DataFrame(rows)
        st.dataframe(df, use_container_width=True, hide_index=True, height=500)

        # ── Event detail expander ──
        st.subheader("Event detail")
        st.caption("Click on an event below to see its full DynamoDB record.")
        for e in display_events[:10]:
            label = f"{e.get('EventName', 'Unknown')} | {str(e.get('EventTime', ''))[:19]} | {e.get('Processed', '?')}"
            with st.expander(label):
                st.json(e)
    else:
        st.info("No events match the current filters.")


# ══════════════════════════════════════════════════════════════════════════════
# PAGE: Configuration
# ══════════════════════════════════════════════════════════════════════════════
elif page == "Configuration":
    st.title("Configuration")

    st.caption(f"Reading from: `{config_path}`")

    # ── Row 1: AWS Connection + Operations ──
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("AWS Connection")
        st.text_input("Source region", value=cfg["source_region"], disabled=True, key="src_region")
        st.text_input("Target region", value=cfg["destination_region"], disabled=True, key="tgt_region")

        if HAS_BOTO:
            try:
                sts = boto3.client("sts", region_name=cfg["source_region"])
                identity = sts.get_caller_identity()
                st.success(f"Connected — Account: {identity['Account']}")
            except Exception as exc:
                st.warning(f"Not connected: {exc}")
        else:
            st.warning("boto3 not installed. Run `pip install boto3`.")

    with col2:
        st.subheader("Operations")
        st.toggle("Sync Glue catalog", value=cfg["sync_glue_catalog"], disabled=True, key="op_glue")
        st.toggle("Sync Lake Formation permissions", value=cfg["sync_lf_permissions"], disabled=True, key="op_lf")
        st.toggle(
            "Remap S3 locations for target region", value=cfg["update_table_s3_location"], disabled=True, key="op_s3"
        )
        st.toggle(
            "Rewrite Iceberg metadata files", value=cfg["rewrite_iceberg_metadata"], disabled=True, key="op_iceberg"
        )
        st.toggle(
            "Delete extra objects in target", value=cfg["delete_target_catalog_objects"], disabled=True, key="op_delete"
        )
        st.caption("Edit `config/glue_config.conf` to change these settings, then redeploy.")

    st.divider()

    # ── Row 2: Databases + S3 Bucket Mapping ──
    col3, col4 = st.columns(2)

    with col3:
        st.subheader("Database List")
        if cfg["database_list"]:
            db_tags = " &nbsp; ".join([f"`{db}`" for db in cfg["database_list"]])
            st.markdown(db_tags, unsafe_allow_html=True)
        else:
            st.info("No databases configured.")

    with col4:
        st.subheader("S3 Bucket Mapping")
        if cfg["s3_bucket_mapping"]:
            for source, target in cfg["s3_bucket_mapping"].items():
                st.text(f"{source}  →  {target}")
        else:
            st.info("No bucket mappings configured.")

    st.divider()

    # ── Row 3: Schedule + Lambda + Storage ──
    col5, col6, col7 = st.columns(3)

    with col5:
        st.subheader("Sync Schedule")
        st.metric("CloudTrail lookup window", f"{cfg['cloudtrail_lookup_hours']}h")
        st.caption(
            "EventBridge triggers the CloudTrail pull Lambda on a schedule. Adjust the interval in the CDK stack."
        )

    with col6:
        st.subheader("Lambda Configuration")
        lambda_runtime = cfg.get("lambda_runtime", "python3.13")
        dlq_name = cfg.get("dlq_name", "lfDRDeadLetterQueue")
        st.text(f"Runtime: {lambda_runtime}")
        st.text(f"DynamoDB table: {DYNAMODB_TABLE_NAME}")
        st.text(f"Dead letter queue: {dlq_name}")
        st.caption("Lambda settings are managed via CDK. Redeploy to update.")

    with col7:
        st.subheader("Storage Paths")
        st.text(f"Backup bucket: {cfg['backup_file_bucket']}")
        st.text(f"Backup folder: {cfg['backup_file_folder']}")
        st.text(f"LF bucket: {cfg['lf_storage_bucket']}")
        st.text(f"LF folder: {cfg['lf_storage_folder']}")

    st.divider()

    # ── Config file preview ──
    st.subheader("Config File Preview")
    if os.path.exists(config_path):
        with open(config_path) as f:
            config_text = f.read()
        st.code(config_text, language="ini")
    else:
        st.warning(f"Config file not found at `{config_path}`")

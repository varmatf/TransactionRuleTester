import io
from datetime import timedelta
import pandas as pd
import streamlit as st

# -----------------------------
# Page configuration
# -----------------------------
st.set_page_config(
    page_title="Transaction Rule Tester",
    page_icon="💳",
    layout="wide",
    initial_sidebar_state="expanded",
)

# -----------------------------
# Initialize session state
# -----------------------------
def _init_state():
    ss = st.session_state
    ss.setdefault("current_step", 1)
    ss.setdefault("excel_file", None)
    ss.setdefault("sheet_name", "")
    ss.setdefault("df", None)

    ss.setdefault("mapping_labels", ["User_ID", "TimeStamp", "Amount", "TransactionID", "Filter1", "Filter2", "Filter3", "Filter4"])
    ss.setdefault("mapping", {k: "" for k in ss["mapping_labels"]})

    ss.setdefault("filters", {})
    ss.setdefault("min_txn", 1)
    ss.setdefault("amount_threshold", 5000.0)
    ss.setdefault("duration_value", 7)
    ss.setdefault("duration_unit", "days")

    ss.setdefault("alert_df", pd.DataFrame())
    ss.setdefault("alert_txn_df", pd.DataFrame())
    ss.setdefault("summary_data", {})
    ss.setdefault("output_bytes", None)

_init_state()
ss = st.session_state

# -----------------------------
# Rule Execution Logic
# -----------------------------
def run_rule_and_build_output():
    df = ss["df"].copy()
    user_col = ss["mapping"]["User_ID"]
    time_col = ss["mapping"]["TimeStamp"]
    amount_col = ss["mapping"]["Amount"]
    txn_col = ss["mapping"]["TransactionID"]

    if not all([user_col, time_col, amount_col, txn_col]):
        st.error("Please complete required mappings: User_ID, TimeStamp, Amount, TransactionID.")
        return False

    try:
        df[txn_col] = df[txn_col].astype(str)
    except Exception as e:
        st.error(f"Failed to cast TransactionID to string: {e}")
        return False

    # Apply filters
    for col_name, values in ss["filters"].items():
        if values:
            df = df[df[col_name].isin(values)].copy()

    df[time_col] = pd.to_datetime(df[time_col], errors='coerce')
    df = df.dropna(subset=[time_col])
    df = df.sort_values([user_col, time_col])

    min_txn = int(ss["min_txn"])
    threshold = float(ss["amount_threshold"])
    duration = int(ss["duration_value"])
    unit = ss["duration_unit"]

    rule_params = {
        "Min Transactions": min_txn,
        "Amount Threshold": threshold,
        "Duration": duration,
        "Duration Unit": unit
    }

    if unit == "minutes":
        time_delta = timedelta(minutes=duration)
    elif unit == "hours":
        time_delta = timedelta(hours=duration)
    else:
        time_delta = timedelta(days=duration)

    alerts = []
    alert_transactions = []  # store contributing txns
    alert_id = 1

    for customer_id, group in df.groupby(user_col):
        group = group.reset_index(drop=True)
        i = 0
        while i < len(group):
            start_time = group.loc[i, time_col]
            window = []
            j = i
            while j < len(group) and (group.loc[j, time_col] - start_time) <= time_delta:
                row = group.loc[j]
                window.append(row.to_dict())
                total_amount = sum(tx[amount_col] for tx in window)

                if len(window) >= min_txn and total_amount >= threshold:
                    last_tx_time = window[-1][time_col]
                    last_tx = group[group[time_col] == last_tx_time].iloc[-1]

                    alerts.append({
                        'alert_id': alert_id,
                        'customer_id': customer_id,
                        'trigger_start': start_time,
                        'trigger_end': last_tx[time_col],
                        'trigger_transaction_id': last_tx[txn_col],
                        'trigger_transaction_amount': last_tx[amount_col],
                        'transaction_count': len(window),
                        'total_amount': total_amount
                    })

                    # store contributing transactions for this alert
                    for tx in window:
                        tx_copy = tx.copy()
                        tx_copy["alert_id"] = alert_id
                        alert_transactions.append(tx_copy)

                    alert_id += 1
                    i = j + 1
                    break
                j += 1
            else:
                i += 1

    alert_df = pd.DataFrame(alerts)
    alert_txn_df = pd.DataFrame(alert_transactions)

    ss["alert_df"] = alert_df
    ss["alert_txn_df"] = alert_txn_df

    summary_data = {
        "Mapped Columns": ss["mapping"].copy(),
        "Rule Parameters": rule_params,
        "Filter Selections": ss["filters"].copy()
    }
    ss["summary_data"] = summary_data

    # Create Excel output
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        alert_df.to_excel(writer, sheet_name="AlertResults", index=False)
        if not alert_txn_df.empty:
            alert_txn_df.to_excel(writer, sheet_name="AlertTransactions", index=False)

        rows = []
        for section, content in summary_data.items():
            rows.append([section, ""])
            if isinstance(content, dict):
                for key, value in content.items():
                    rows.append([key, str(value)])
            rows.append(["", ""])
        summary_df = pd.DataFrame(rows, columns=["Parameter", "Value"])
        summary_df.to_excel(writer, sheet_name="Summary", index=False)
    output.seek(0)
    ss["output_bytes"] = output.getvalue()
    return True

# -----------------------------
# Sidebar Navigation (Fixed)
# -----------------------------
with st.sidebar:
    st.title("Wizard")

    current_step = ss.get("current_step", 1)

    step = st.radio(
        "Steps",
        [1, 2],
        format_func=lambda x: "1) Data & Setup" if x == 1 else "2) Parameters & Run",
        index=current_step - 1,
        key="step_radio",
    )

    st.markdown("---")
    c1, c2 = st.columns(2)

    with c1:
        if st.button("⬅ Back", disabled=(current_step == 1), use_container_width=True):
            ss["current_step"] = 1
            st.rerun()

    with c2:
        disable_next = False
        if current_step == 1:
            need = ["User_ID", "TimeStamp", "Amount", "TransactionID"]
            if ss["df"] is None or ss["df"].empty or any(not ss["mapping"].get(n) for n in need):
                disable_next = True
        if st.button("Next ➡", disabled=(current_step == 2 or disable_next), use_container_width=True):
            ss["current_step"] = 2
            st.rerun()

    step = ss.get("current_step", 1)

# -----------------------------
# STEP 1 — Data & Setup
# -----------------------------
if step == 1:
    st.title("Step 1 — Data & Setup")

    with st.container():
        st.subheader("Upload Excel & Select Sheet")
        uploaded = st.file_uploader("Upload a single Excel file (.xlsx)", type=["xlsx"])
        if uploaded:
            try:
                ss["excel_file"] = pd.ExcelFile(uploaded, engine="openpyxl")
                sheet = st.selectbox("Sheet", ss["excel_file"].sheet_names, key="sheet_name")
                if sheet:
                    ss["df"] = ss["excel_file"].parse(sheet, engine="openpyxl")
                    st.success(f"Loaded `{sheet}` — {len(ss['df'])} rows × {len(ss['df'].columns)} columns.")
                    with st.expander("Preview (top 50 rows)"):
                        st.dataframe(ss["df"].head(50), use_container_width=True)
            except Exception as e:
                st.error(f"Failed to read Excel: {e}")

    st.markdown("---")

    left, right = st.columns([1, 1], gap="large")

    with left:
        st.subheader("Map Columns")
        if ss["df"] is None:
            st.info("Upload and select a sheet first.")
        else:
            cols = [""] + ss["df"].columns.tolist()
            need = ["User_ID", "TimeStamp", "Amount", "TransactionID"]
            opt = ["Filter1", "Filter2", "Filter3", "Filter4"]

            st.caption("**Required**: User_ID, TimeStamp, Amount, TransactionID")
            for label in need:
                ss["mapping"][label] = st.selectbox(
                    label,
                    options=cols,
                    index=cols.index(ss["mapping"][label]) if ss["mapping"][label] in cols else 0,
                    key=f"map_{label}"
                )
            with st.expander("Optional filters (Filter1–4)"):
                for label in opt:
                    ss["mapping"][label] = st.selectbox(
                        label,
                        options=cols,
                        index=cols.index(ss["mapping"][label]) if ss["mapping"][label] in cols else 0,
                        key=f"map_{label}"
                    )

            missing = [m for m in need if not ss["mapping"].get(m)]
            if missing:
                st.warning(f"Missing required mappings: {', '.join(missing)}")
            else:
                st.success("Required mappings complete.")

    with right:
        st.subheader("Filter Values (optional)")
        if ss["df"] is None:
            st.info("Upload and select a sheet first.")
        else:
            mapped = [ss["mapping"][f"Filter{i}"] for i in range(1, 5)]
            filter_cols = [c for c in mapped if c and c in ss["df"].columns]

            if not filter_cols:
                st.info("No filter columns mapped. You can proceed without filters.")
            else:
                grid = st.columns(2)
                for idx, col in enumerate(filter_cols):
                    with grid[idx % 2]:
                        values = sorted([v for v in ss["df"][col].dropna().unique().tolist()])
                        st.markdown(f"**{col}**")
                        select_all = st.checkbox(f"Select all ({col})", key=f"all_{col}")
                        if select_all:
                            ss["filters"][col] = values
                        else:
                            default = ss["filters"].get(col, [])
                            ss["filters"][col] = st.multiselect(
                                f"Values to retain in `{col}`",
                                options=values,
                                default=default,
                                key=f"ms_{col}",
                            )

            with st.expander("Current selections"):
                st.json(ss["filters"])

# -----------------------------
# STEP 2 — Parameters & Results
# -----------------------------
elif step == 2:
    st.title("Step 2 — Parameters, Run & Results")

    c1, c2 = st.columns([1, 1], gap="large")

    with c1:
        st.subheader("Parameters")
        ss["min_txn"] = st.number_input("Min Transactions", min_value=1, value=int(ss["min_txn"]), step=1)
        ss["amount_threshold"] = st.number_input("Amount Threshold", min_value=0.0, value=float(ss["amount_threshold"]), step=100.0, format="%.2f")
        ss["duration_value"] = st.number_input("Duration", min_value=1, value=int(ss["duration_value"]), step=1)
        ss["duration_unit"] = st.selectbox("Duration Unit", ["minutes", "hours", "days"], index=["minutes", "hours", "days"].index(ss["duration_unit"]))

        run = st.button("▶ Run Rule", type="primary", use_container_width=True)
        if run:
            if ss["df"] is None or ss["df"].empty:
                st.warning("Please complete Step 1 first.")
            else:
                ok = run_rule_and_build_output()
                if ok:
                    st.success("Rule executed successfully. See results below.")

    with c2:
        st.subheader("Summary")
        mapping_display = {k: (v if v else "-") for k, v in ss["mapping"].items()}
        st.write("**Mapped Columns**")
        st.json(mapping_display)

        st.write("**Filter Selections (retain)**")
        if ss["filters"]:
            st.json({k: (f"{len(v)} selected" if v else "(all)") for k, v in ss["filters"].items()})
        else:
            st.info("No filter selections (keeping all).")

        st.write("**Parameters**")
        st.json({
            "Min Transactions": ss["min_txn"],
            "Amount Threshold": ss["amount_threshold"],
            "Duration": f"{ss['duration_value']} {ss['duration_unit']}"
        })

    st.markdown("---")
    st.subheader("Results")

    if isinstance(ss.get("alert_df"), pd.DataFrame) and not ss["alert_df"].empty:
        st.dataframe(ss["alert_df"], use_container_width=True, height=420)
        if isinstance(ss.get("alert_txn_df"), pd.DataFrame) and not ss["alert_txn_df"].empty:
            with st.expander("View Alert-Contributing Transactions"):
                st.dataframe(ss["alert_txn_df"], use_container_width=True, height=400)
    else:
        st.info("Run the rule to see results here.")

    if ss.get("output_bytes"):
        st.download_button(
            label="⬇ Download Excel (Alerts + Transactions + Summary)",
            data=ss["output_bytes"],
            file_name="AlertResults.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True
        )

st.markdown(
    "<hr style='margin-top:2rem;margin-bottom:0.5rem'/>"
    "<div style='color:#777'>Built with Streamlit · Transaction Rule Tester</div>",
    unsafe_allow_html=True,
)

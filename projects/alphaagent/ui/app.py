"""
AlphaAgent — Streamlit demo UI.

Run:
    streamlit run ui/app.py --server.port 8501

Talks exclusively to the FastAPI service at $ALPHAAGENT_API_URL (default
http://localhost:8000). All business logic lives behind the API — the UI is
deliberately thin so "what works in the API demo works in the UI."

Tabs:
  1. Ask — natural-language query with generated SQL + grounded answer
  2. Portfolio Explorer — NAV, returns, and risk charts
  3. Data Quality — latest DQ check summary
  4. Lineage — pipeline + mart lineage graph
  5. Agent Eval — scorecard with per-question pass/fail
"""
from __future__ import annotations

import os
from pathlib import Path

import httpx
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

API_URL = os.environ.get("ALPHAAGENT_API_URL", "http://localhost:8000")
API_KEY = os.environ.get("ALPHAAGENT_API_KEY")
HEADERS = {"X-API-Key": API_KEY} if API_KEY else {}
EVALS_DIR = Path(__file__).resolve().parent.parent / "evals"

st.set_page_config(page_title="AlphaAgent", page_icon="📈", layout="wide")


# ---------------------------------------------------------------------------
# API client helpers
# ---------------------------------------------------------------------------

def api_get(path: str, **params):
    try:
        r = httpx.get(f"{API_URL}{path}", params=params, headers=HEADERS, timeout=15.0)
        r.raise_for_status()
        return r.json()
    except httpx.HTTPError as e:
        st.error(f"API error: {e}")
        return None


def api_post(path: str, body: dict):
    try:
        r = httpx.post(f"{API_URL}{path}", json=body, headers=HEADERS, timeout=60.0)
        r.raise_for_status()
        return r.json()
    except httpx.HTTPError as e:
        st.error(f"API error: {e}")
        return None


@st.cache_data(ttl=30)
def list_portfolios() -> list[str]:
    data = api_get("/v1/portfolios")
    return (data or {}).get("portfolios", [])


@st.cache_data(ttl=60)
def get_perf(pid: str, from_d: str | None, to_d: str | None) -> pd.DataFrame:
    data = api_get(f"/v1/portfolio/{pid}/performance", **{"from": from_d, "to": to_d})
    if not data:
        return pd.DataFrame()
    return pd.DataFrame(data.get("points", []))


@st.cache_data(ttl=60)
def get_risk(pid: str, from_d: str | None, to_d: str | None) -> pd.DataFrame:
    data = api_get(f"/v1/portfolio/{pid}/risk", **{"from": from_d, "to": to_d})
    if not data:
        return pd.DataFrame()
    return pd.DataFrame(data.get("points", []))


# ---------------------------------------------------------------------------
# Sidebar: connection status + global context
# ---------------------------------------------------------------------------

with st.sidebar:
    st.title("AlphaAgent")
    st.caption("Multi-agent NLQ + portfolio analytics demo")
    st.write(f"**API:** `{API_URL}`")
    health = api_get("/v1/health")
    if health:
        st.success(f"Status: **{health['status']}** · v{health['version']}")
        st.json(health.get("checks", {}), expanded=False)
    else:
        st.error("API unreachable")

    st.divider()
    st.markdown(
        "**Stack:** LangGraph · dbt · Postgres · Redpanda · OpenLineage · "
        "Great Expectations · FastAPI · Streamlit"
    )
    st.markdown("[GitHub repo ↗](#) · [Substack post ↗](#)")


tab_ask, tab_explore, tab_dq, tab_lin, tab_eval = st.tabs(
    ["🤖 Ask", "📊 Portfolio Explorer", "🧪 Data Quality", "🕸 Lineage", "🎯 Agent Eval"]
)


# ---------------------------------------------------------------------------
# Tab 1: Ask — natural language query
# ---------------------------------------------------------------------------

with tab_ask:
    st.header("Ask in plain English")
    st.caption(
        "The multi-agent pipeline (planner → sql_writer → critic → executor → "
        "explainer) converts your question into safe read-only SQL against the "
        "dbt marts, executes it, and returns a grounded answer with citations."
    )

    examples = [
        "What is the YTD return for portfolio P-001?",
        "Which 5 portfolios have the highest Sharpe ratio over the last 30 days?",
        "How does MSFT compare to AAPL over the last year?",
        "Which sectors contributed most to P-001's YTD return?",
    ]
    cols = st.columns(len(examples))
    chosen = None
    for i, ex in enumerate(examples):
        if cols[i].button(ex, key=f"ex_{i}", use_container_width=True):
            chosen = ex

    question = st.text_input(
        "Your question",
        value=chosen or "What is the YTD return for portfolio P-001?",
        placeholder="Try: 'Which 5 portfolios had the highest 30-day volatility?'",
    )
    go_clicked = st.button("Ask", type="primary")

    if go_clicked and question:
        with st.spinner("Planning → writing SQL → validating → executing → explaining..."):
            resp = api_post("/v1/ask", {"question": question})

        if resp:
            # Top metrics row
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Status", resp["final_status"])
            c2.metric("Rows", resp["row_count"])
            c3.metric("LLM cost", f"${resp['llm_cost_usd']:.4f}")
            c4.metric("Latency", f"{resp['execution_ms']} ms")

            st.subheader("Answer")
            st.markdown(resp.get("answer") or "_(no answer)_")
            if resp.get("citations"):
                st.caption("Grounded in columns: " + ", ".join(resp["citations"]))

            st.subheader("Generated SQL")
            st.code(resp.get("generated_sql") or "", language="sql")
            if not resp.get("sql_valid"):
                st.error(
                    "Critic rejected this SQL: "
                    + ("; ".join(resp.get("sql_errors", [])) or "unknown reason")
                )

            rows = resp.get("result_rows") or []
            if rows:
                df = pd.DataFrame(rows)
                st.subheader("Result")
                st.dataframe(df, use_container_width=True)
                # Chart hint from explainer
                cs = resp.get("chart_spec") or {}
                if cs.get("type") == "bar" and cs.get("x") in df.columns and cs.get("y") in df.columns:
                    st.plotly_chart(px.bar(df, x=cs["x"], y=cs["y"]), use_container_width=True)
                elif cs.get("type") == "line" and cs.get("x") in df.columns and cs.get("y") in df.columns:
                    st.plotly_chart(px.line(df, x=cs["x"], y=cs["y"]), use_container_width=True)


# ---------------------------------------------------------------------------
# Tab 2: Portfolio Explorer
# ---------------------------------------------------------------------------

with tab_explore:
    st.header("Portfolio Explorer")

    portfolios = list_portfolios()
    if not portfolios:
        st.info("No portfolios returned. Seed the database: `make seed && make transform`.")
    else:
        cols = st.columns([2, 1, 1])
        pid = cols[0].selectbox("Portfolio", portfolios, index=0)
        from_d = cols[1].date_input("From", value=None, key="from_perf")
        to_d = cols[2].date_input("To", value=None, key="to_perf")
        from_str = from_d.isoformat() if from_d else None
        to_str = to_d.isoformat() if to_d else None

        perf = get_perf(pid, from_str, to_str)
        risk = get_risk(pid, from_str, to_str)

        if perf.empty:
            st.warning("No performance data for that portfolio/window.")
        else:
            perf["as_of_date"] = pd.to_datetime(perf["as_of_date"])
            latest = perf.iloc[-1]
            k1, k2, k3, k4 = st.columns(4)
            k1.metric("NAV", f"${latest['nav']:,.0f}")
            k2.metric("MTD return", f"{(latest['mtd_return'] or 0) * 100:.2f}%")
            k3.metric("YTD return", f"{(latest['ytd_return'] or 0) * 100:.2f}%")
            if not risk.empty:
                rlatest = risk.iloc[-1]
                k4.metric(
                    "Sharpe (30d)",
                    f"{rlatest['sharpe_30d']:.2f}" if rlatest["sharpe_30d"] is not None else "—",
                )

            # NAV line
            st.plotly_chart(
                px.line(perf, x="as_of_date", y="nav", title=f"{pid} — NAV"),
                use_container_width=True,
            )
            # Cumulative return overlay
            if "ytd_return" in perf.columns:
                fig = go.Figure()
                fig.add_trace(go.Scatter(
                    x=perf["as_of_date"], y=perf["ytd_return"] * 100,
                    name="YTD %", mode="lines",
                ))
                fig.add_trace(go.Scatter(
                    x=perf["as_of_date"], y=perf["mtd_return"] * 100,
                    name="MTD %", mode="lines",
                ))
                fig.update_layout(title="Cumulative return", yaxis_title="%")
                st.plotly_chart(fig, use_container_width=True)

            if not risk.empty:
                risk["as_of_date"] = pd.to_datetime(risk["as_of_date"])
                st.plotly_chart(
                    px.line(risk, x="as_of_date", y="volatility_30d_annualized",
                            title="Annualized volatility (30d rolling)"),
                    use_container_width=True,
                )
                st.plotly_chart(
                    px.line(risk, x="as_of_date", y="sharpe_30d",
                            title="Sharpe ratio (30d rolling)"),
                    use_container_width=True,
                )

            with st.expander("Raw performance data"):
                st.dataframe(perf, use_container_width=True)


# ---------------------------------------------------------------------------
# Tab 3: Data Quality
# ---------------------------------------------------------------------------

with tab_dq:
    st.header("Data Quality Monitor")
    st.caption(
        "Great Expectations suites run after every dbt build. Failures halt "
        "the pipeline; warnings surface here for engineer review."
    )
    data = api_get("/v1/dq/summary")
    checks = (data or {}).get("checks", [])
    if not checks:
        st.info(data.get("note", "No DQ results yet.") if data else "DQ endpoint unreachable.")
    else:
        df = pd.DataFrame(checks)
        pass_n = (df["status"] == "pass").sum()
        fail_n = (df["status"] == "fail").sum()
        warn_n = (df["status"] == "warn").sum() if "warn" in df["status"].values else 0
        c1, c2, c3 = st.columns(3)
        c1.metric("✅ Passing", pass_n)
        c2.metric("⚠️ Warnings", warn_n)
        c3.metric("❌ Failing", fail_n)
        st.dataframe(df, use_container_width=True)


# ---------------------------------------------------------------------------
# Tab 4: Lineage
# ---------------------------------------------------------------------------

with tab_lin:
    st.header("Data Lineage")
    st.caption(
        "Column-level lineage is emitted by dbt + Airflow via OpenLineage into "
        "Marquez. A static fallback graph is shown here for offline demos."
    )
    data = api_get("/v1/lineage")
    if data:
        if data.get("marquez_url"):
            st.markdown(f"🔗 [Open Marquez]({data['marquez_url']}) for live column-level lineage.")
        if data.get("note"):
            st.info(data["note"])

        nodes = data.get("nodes", [])
        edges = data.get("edges", [])
        st.subheader("Nodes")
        st.dataframe(pd.DataFrame(nodes), use_container_width=True)
        st.subheader("Edges")
        st.dataframe(pd.DataFrame(edges), use_container_width=True)

        # Simple Sankey-ish layout
        labels = [n["name"] for n in nodes]
        idx = {n["name"]: i for i, n in enumerate(nodes)}
        sources = [idx[e["source"]] for e in edges if e["source"] in idx and e["target"] in idx]
        targets = [idx[e["target"]] for e in edges if e["source"] in idx and e["target"] in idx]
        values = [1] * len(sources)
        fig = go.Figure(data=[go.Sankey(
            node=dict(label=labels, pad=15, thickness=20),
            link=dict(source=sources, target=targets, value=values),
        )])
        fig.update_layout(title="Pipeline flow", height=500)
        st.plotly_chart(fig, use_container_width=True)


# ---------------------------------------------------------------------------
# Tab 5: Agent eval scorecard + recent queries
# ---------------------------------------------------------------------------

with tab_eval:
    st.header("Agent Eval Scorecard")
    st.caption(
        "Regression suite: 30 golden Q&A pairs covering factual, analytical, "
        "comparative, risk, and attribution questions across easy/medium/hard. "
        "CI blocks merges if match-accuracy drops below threshold."
    )

    scorecard = EVALS_DIR / "scorecard.md"
    if scorecard.exists():
        st.markdown(scorecard.read_text())
    else:
        st.info("No scorecard yet — run `make eval` to generate one.")

    st.divider()
    st.subheader("Recent agent queries")
    data = api_get("/v1/agent/queries", limit=30)
    if data and data.get("entries"):
        df = pd.DataFrame(data["entries"])
        # Success-rate tile
        ok = (df["final_status"] == "success").sum()
        st.metric("Success rate (last 30)", f"{ok}/{len(df)} ({ok/len(df)*100:.0f}%)")
        st.dataframe(df, use_container_width=True)
    else:
        st.info("No agent query log entries yet.")

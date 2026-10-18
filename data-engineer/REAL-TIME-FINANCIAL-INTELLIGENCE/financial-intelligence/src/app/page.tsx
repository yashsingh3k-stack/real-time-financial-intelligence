"use client";

import { useEffect, useState } from "react";

type DashboardData = {
  generated_at: string;
  source: string;
  kpis: {
    total_transactions: number;
    unique_customers: number;
    total_amount: number;
    average_amount: number;
    maximum_amount: number;
  };
  transaction_types: Record<string, number>;
  cities: { name: string; count: number }[];
  channels: { name: string; count: number }[];
  anomalies: Record<string, number>;
};

const numberFormat = new Intl.NumberFormat("en-IN");
const currencyFormat = new Intl.NumberFormat("en-IN", {
  style: "currency",
  currency: "INR",
  maximumFractionDigits: 0,
});

function compactCurrency(value: number) {
  if (value >= 1e7) return `₹${(value / 1e7).toFixed(2)} Cr`;
  if (value >= 1e5) return `₹${(value / 1e5).toFixed(2)} L`;
  return currencyFormat.format(value);
}

function percentage(value: number, total: number) {
  return total ? (value / total) * 100 : 0;
}

function BarRow({
  label,
  count,
  total,
}: {
  label: string;
  count: number;
  total: number;
}) {
  const width = percentage(count, total);

  return (
    <div className="bar-row">
      <div className="bar-label">
        <span>{label}</span>
        <span className="bar-value">{numberFormat.format(count)}</span>
      </div>
      <div className="bar-track">
        <div className="bar-fill" style={{ width: `${width}%` }} />
      </div>
      <div className="bar-percent">{width.toFixed(1)}%</div>
    </div>
  );
}

export default function Home() {
  const [data, setData] = useState<DashboardData | null>(null);
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    let active = true;

    async function loadDashboard() {
      try {
        const response = await fetch("/api/analytics", {
          cache: "no-store",
        });
        if (!response.ok) {
          throw new Error(`Analytics API returned HTTP ${response.status}`);
        }
        const result: DashboardData = await response.json();
        if (active) setData(result);
      } catch (err) {
        if (active) {
          setError(
            err instanceof Error ? err.message : "Unable to load analytics."
          );
        }
      } finally {
        if (active) setLoading(false);
      }
    }

    loadDashboard();
    return () => {
      active = false;
    };
  }, []);

  if (loading) {
    return (
      <main className="page-shell">
        <div className="loading-card">Loading financial analytics…</div>
      </main>
    );
  }

  if (error || !data) {
    return (
      <main className="page-shell">
        <div className="error-card">
          <h1>Dashboard data unavailable</h1>
          <p>{error || "No analytics data was returned."}</p>
          <button onClick={() => window.location.reload()}>Try again</button>
        </div>
      </main>
    );
  }

  const totalTypes = Object.values(data.transaction_types).reduce(
    (sum, count) => sum + count,
    0
  );
  const totalAnomalyFlags = Object.values(data.anomalies).reduce(
    (sum, count) => sum + count,
    0
  );

  return (
    <main className="page-shell">
      <header className="topbar">
        <a className="brand" href="#">
          <span className="brand-mark">F</span>
          <span>FINPULSE<span className="brand-dot">.</span></span>
        </a>
        <div className="topbar-right">
          <span className="live-indicator" />
          <span>Analytics snapshot</span>
          <span className="environment-tag">SPARK + HDFS</span>
        </div>
      </header>

      <section className="hero">
        <div>
          <p className="eyebrow">FINANCIAL INTELLIGENCE PLATFORM</p>
          <h1>Transaction overview<span>.</span></h1>
          <p className="hero-copy">
            A consolidated view of transaction activity, customer reach, and
            rule-based anomaly signals across your financial data pipeline.
          </p>
        </div>
        <div className="snapshot">
          <span className="snapshot-label">DATA SNAPSHOT</span>
          <strong>
            {new Date(data.generated_at).toLocaleString("en-IN", {
              dateStyle: "medium",
              timeStyle: "short",
            })}
          </strong>
          <span className="snapshot-source">{data.source}</span>
        </div>
      </section>

      <section className="section-block">
        <div className="section-heading">
          <div>
            <p className="eyebrow">01 / PERFORMANCE</p>
            <h2>Key metrics</h2>
          </div>
          <span className="section-note">Based on exported transaction data</span>
        </div>

        <div className="kpi-grid">
          <article className="kpi-card">
            <div className="kpi-top">
              <span>Total transactions</span>
              <span className="kpi-icon">↗</span>
            </div>
            <strong className="kpi-number">
              {numberFormat.format(data.kpis.total_transactions)}
            </strong>
            <span className="kpi-foot">Processed transaction records</span>
          </article>
          <article className="kpi-card">
            <div className="kpi-top">
              <span>Unique customers</span>
              <span className="kpi-icon">◎</span>
            </div>
            <strong className="kpi-number">
              {numberFormat.format(data.kpis.unique_customers)}
            </strong>
            <span className="kpi-foot">Distinct customer identifiers</span>
          </article>
          <article className="kpi-card kpi-highlight">
            <div className="kpi-top">
              <span>Total transaction value</span>
              <span className="kpi-icon">₹</span>
            </div>
            <strong className="kpi-number">
              {compactCurrency(data.kpis.total_amount)}
            </strong>
            <span className="kpi-foot">Aggregate amount across records</span>
          </article>
          <article className="kpi-card">
            <div className="kpi-top">
              <span>Average transaction</span>
              <span className="kpi-icon">≈</span>
            </div>
            <strong className="kpi-number">
              {currencyFormat.format(data.kpis.average_amount)}
            </strong>
            <span className="kpi-foot">
              Maximum: {currencyFormat.format(data.kpis.maximum_amount)}
            </span>
          </article>
        </div>
      </section>

      <section className="analytics-grid section-block">
        <article className="panel">
          <div className="panel-heading">
            <div>
              <p className="eyebrow">02 / TRANSACTION MIX</p>
              <h2>Purchases & refunds</h2>
            </div>
            <span className="panel-symbol">◒</span>
          </div>

          <div className="mix-total">
            <strong>{numberFormat.format(totalTypes)}</strong>
            <span>classified transactions</span>
          </div>
          <div className="stacked-bar">
            {Object.entries(data.transaction_types).map(([type, count]) => (
              <div
                key={type}
                className={`stack-segment ${type === "refund" ? "refund" : "purchase"}`}
                style={{ width: `${percentage(count, totalTypes)}%` }}
                title={`${type}: ${numberFormat.format(count)}`}
              />
            ))}
          </div>
          <div className="legend-list">
            {Object.entries(data.transaction_types).map(([type, count]) => (
              <div className="legend-item" key={type}>
                <span className={`legend-dot ${type}`} />
                <span className="legend-name">
                  {type.charAt(0).toUpperCase() + type.slice(1)}
                </span>
                <strong>{numberFormat.format(count)}</strong>
                <span className="legend-pct">
                  {percentage(count, totalTypes).toFixed(1)}%
                </span>
              </div>
            ))}
          </div>
        </article>

        <article className="panel">
          <div className="panel-heading">
            <div>
              <p className="eyebrow">03 / GEOGRAPHY</p>
              <h2>Transactions by city</h2>
            </div>
            <span className="panel-symbol">⌖</span>
          </div>
          <div className="bar-list">
            {data.cities.map((city) => (
              <BarRow
                key={city.name}
                label={city.name}
                count={city.count}
                total={data.kpis.total_transactions}
              />
            ))}
          </div>
        </article>

        <article className="panel">
          <div className="panel-heading">
            <div>
              <p className="eyebrow">04 / CHANNELS</p>
              <h2>Transaction channels</h2>
            </div>
            <span className="panel-symbol">⌘</span>
          </div>
          <div className="channel-list">
            {data.channels.map((channel, index) => (
              <div className="channel-card" key={channel.name}>
                <div className={`channel-icon channel-${index}`}>
                  {channel.name === "mobile"
                    ? "▣"
                    : channel.name === "web"
                      ? "⌘"
                      : "▤"}
                </div>
                <div className="channel-info">
                  <strong>{channel.name.toUpperCase()}</strong>
                  <span>{percentage(channel.count, data.kpis.total_transactions).toFixed(1)}% of all transactions</span>
                </div>
                <strong className="channel-count">
                  {numberFormat.format(channel.count)}
                </strong>
              </div>
            ))}
          </div>
          <p className="panel-footnote">
            Distribution across recorded transaction channels.
          </p>
        </article>
      </section>

      <section className="section-block anomaly-section">
        <div className="section-heading">
          <div>
            <p className="eyebrow">05 / RISK SIGNALS</p>
            <h2>Anomaly detection</h2>
            <p className="subheading">
              Counts produced by the pipeline&apos;s three detection rules.
            </p>
          </div>
          <div className="risk-total">
            <span>Total rule flags</span>
            <strong>{numberFormat.format(totalAnomalyFlags)}</strong>
            <small>Flags may overlap; not unique transactions.</small>
          </div>
        </div>

        <div className="anomaly-grid">
          <article className="anomaly-card">
            <div className="anomaly-icon amber">↗</div>
            <span className="anomaly-label">HIGH-VALUE TRANSACTIONS</span>
            <strong>{numberFormat.format(data.anomalies.high_value_transactions ?? 0)}</strong>
            <p>Transaction amount at or above ₹45,000.</p>
          </article>
          <article className="anomaly-card">
            <div className="anomaly-icon violet">◉</div>
            <span className="anomaly-label">HIGH-ACTIVITY CUSTOMERS</span>
            <strong>{numberFormat.format(data.anomalies.high_activity_customers ?? 0)}</strong>
            <p>Customers meeting the high-activity threshold.</p>
          </article>
          <article className="anomaly-card">
            <div className="anomaly-icon rose">⌁</div>
            <span className="anomaly-label">CUSTOMER-RELATIVE AMOUNTS</span>
            <strong>{numberFormat.format(data.anomalies.customer_relative_high_amounts ?? 0)}</strong>
            <p>Amounts above customer-specific statistical thresholds.</p>
          </article>
        </div>
      </section>

      <footer className="footer">
        <div className="brand footer-brand">
          <span className="brand-mark">F</span>
          <span>FINPULSE<span className="brand-dot">.</span></span>
        </div>
        <span>Financial Intelligence · Data engineering analytics</span>
        <span>Source: Spark Structured Streaming / HDFS Parquet</span>
      </footer>
    </main>
  );
}

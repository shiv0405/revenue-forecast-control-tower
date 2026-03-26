type DashboardSnapshot = {
  title: string;
  summary: {
    accounts_in_scope: number;
    current_quarter_bookings_usd: number;
    quarterly_target_usd: number;
    attainment_pct: number;
    weighted_pipeline_usd: number;
    projected_renewal_risk_usd: number;
    forecast_gap_usd: number;
  };
  regions: Array<Record<string, string | number>>;
  pipeline_risk: Array<Record<string, string | number>>;
  alerts: Array<Record<string, string | number>>;
  scenarios: Array<Record<string, string | number | string[]>>;
};

function currency(value: number): string {
  return new Intl.NumberFormat("en-US", { style: "currency", currency: "USD", maximumFractionDigits: 0 }).format(value);
}

function metric(label: string, value: string): string {
  return `<div class="metric"><div class="metric-label">${label}</div><div class="metric-value">${value}</div></div>`;
}

async function load(): Promise<void> {
  const response = await fetch("./data/dashboard_snapshot.json");
  const snapshot = (await response.json()) as DashboardSnapshot;

  const summary = snapshot.summary;
  document.getElementById("status")!.textContent = "Snapshot loaded";
  document.getElementById("metrics")!.innerHTML = [
    metric("Accounts", String(summary.accounts_in_scope)),
    metric("Bookings", currency(summary.current_quarter_bookings_usd)),
    metric("Attainment", `${summary.attainment_pct.toFixed(1)}%`),
    metric("Forecast Gap", currency(summary.forecast_gap_usd)),
  ].join("");

  document.getElementById("regions")!.innerHTML = `
    <table>
      <thead>
        <tr><th>Region</th><th>Bookings</th><th>Target</th><th>Projected Attainment</th></tr>
      </thead>
      <tbody>
        ${snapshot.regions.map((row) => `
          <tr>
            <td>${row.region_code}</td>
            <td>${currency(Number(row.bookings_usd))}</td>
            <td>${currency(Number(row.target_usd))}</td>
            <td>${Number(row.projected_attainment_pct).toFixed(1)}%</td>
          </tr>
        `).join("")}
      </tbody>
    </table>
  `;

  document.getElementById("alerts")!.innerHTML = snapshot.alerts.map((alert) => `
    <div class="alert ${String(alert.severity)}">
      <strong>${String(alert.account_id)}</strong>
      <p>${String(alert.message)}</p>
      <div>${String(alert.recommended_action)}</div>
    </div>
  `).join("");

  document.getElementById("pipeline")!.innerHTML = `
    <table>
      <thead>
        <tr><th>Account</th><th>Region</th><th>Pipeline</th><th>Risk</th><th>Action</th></tr>
      </thead>
      <tbody>
        ${snapshot.pipeline_risk.map((row) => `
          <tr>
            <td>${row.account_name}</td>
            <td>${row.region_code}</td>
            <td>${currency(Number(row.weighted_pipeline_usd))}</td>
            <td>${Number(row.risk_score).toFixed(1)}</td>
            <td>${row.recommended_action}</td>
          </tr>
        `).join("")}
      </tbody>
    </table>
  `;

  document.getElementById("scenarios")!.innerHTML = snapshot.scenarios.map((scenario) => `
    <div class="scenario">
      <h3>${String(scenario.scenario_name)}</h3>
      <p>Revenue uplift: ${currency(Number(scenario.revenue_uplift_usd))}</p>
      <p>Attainment: ${Number(scenario.attainment_pct).toFixed(1)}%</p>
      <ul>${(scenario.notes as string[]).map((item) => `<li>${item}</li>`).join("")}</ul>
    </div>
  `).join("");
}

load().catch((error: Error) => {
  document.getElementById("status")!.textContent = `Load failed: ${error.message}`;
});

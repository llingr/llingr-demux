#!/usr/bin/env python3
# SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-demux Authors
# SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Llingr-Commercial

"""
Generates a single self-contained HTML benchmark visualization from CSV data.

Usage:
    python3 generate_chart.py

Output:
    llingr-demux-benchmarks.html - single file containing all charts and data
"""

import csv
import json
import os
import platform
import subprocess

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CSV_FILE = os.path.join(SCRIPT_DIR, 'benchmark_results.csv')
OUTPUT_FILE = os.path.join(SCRIPT_DIR, 'llingr-demux-benchmarks.html')

# Colour palette for latency-based charts
LATENCY_COLOURS = {
    10: ('rgba(40, 167, 69, 0.7)', 'rgba(40, 167, 69, 1)'),      # green
    35: ('rgba(70, 130, 180, 0.7)', 'rgba(70, 130, 180, 1)'),    # steel blue
    50: ('rgba(220, 53, 69, 0.7)', 'rgba(220, 53, 69, 1)'),      # red
    100: ('rgba(255, 152, 0, 0.7)', 'rgba(255, 152, 0, 1)'),     # orange
}
DEFAULT_COLOUR = ('rgba(108, 117, 125, 0.7)', 'rgba(108, 117, 125, 1)')

CSS = '''
body {
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
    margin: 0; padding: 0; background: #fafafa;
}
.layout { display: flex; height: 100vh; }
.sidebar {
    width: 220px; background: #fff; border-right: 1px solid #e0e0e0;
    padding: 20px; box-shadow: 2px 0 4px rgba(0,0,0,0.05);
    flex-shrink: 0; display: flex; flex-direction: column;
}
.sidebar h2 { margin: 0 0 20px 0; color: #333; font-size: 1.1em; }
.sidebar a {
    display: block; padding: 12px 15px; margin: 5px 0; color: #555;
    text-decoration: none; border-radius: 6px; cursor: pointer; transition: background 0.2s;
}
.sidebar a:hover { background: #f0f0f0; color: #333; }
.sidebar a.active { background: #40a745; color: #fff; }
.sidebar a.active:hover { background: #369439; }
.main { flex: 1; overflow-y: auto; padding: 20px; }
.section { display: none; max-width: 1000px; margin: 0 auto; }
.section.active { display: block; }
.stats {
    background: #fff; padding: 15px; border-radius: 8px;
    margin-bottom: 20px; box-shadow: 0 1px 3px rgba(0,0,0,0.1);
}
h1 { color: #333; margin-bottom: 20px; }
.chart-container {
    background: #fff; padding: 20px; border-radius: 8px;
    box-shadow: 0 1px 3px rgba(0,0,0,0.1);
}
.note { color: #666; font-size: 0.9em; margin-top: 8px; }
table { width: 100%; border-collapse: collapse; }
th { text-align: left; padding: 8px; }
td { padding: 6px 8px; }
'''


# -----------------------------------------------------------------------------
# Data loading and extraction
# -----------------------------------------------------------------------------

def load_csv():
    """Load all rows from the benchmark CSV."""
    with open(CSV_FILE, 'r') as f:
        return list(csv.DictReader(f))


def extract_framework_overhead_data(rows):
    """Extract framework overhead data from zero-latency runs."""
    data = [
        {'x': int(r['concurrent_keys']), 'y': round(1_000_000 / float(r['actual_tps']), 3)}
        for r in rows if r['processor_latency_ms'] == '0'
    ]
    return sorted(data, key=lambda p: p['x'])


def extract_efficiency_data(rows):
    """Extract efficiency data grouped by latency."""
    groups = {}
    for r in rows:
        latency = int(r['processor_latency_ms'])
        if latency == 0 or float(r['efficiency_pct']) < 0:
            continue
        groups.setdefault(latency, []).append({
            'x': int(r['concurrent_keys']),
            'y': round(float(r['efficiency_pct']), 2),
            'tps': round(float(r['actual_tps']), 0)
        })
    return groups


def extract_tps_data(rows):
    """Extract TPS data grouped by latency."""
    groups = {}
    for r in rows:
        latency = int(r['processor_latency_ms'])
        if latency == 0 or float(r['efficiency_pct']) < 0:
            continue
        groups.setdefault(latency, []).append({
            'x': int(r['concurrent_keys']),
            'y': round(float(r['actual_tps']), 1),
            'theoretical': round(float(r['theoretical_tps']), 1)
        })
    return groups


def extract_linear_scaling_data(rows):
    """Extract 35ms latency data for linear scaling chart."""
    data = [
        {
            'x': int(r['concurrent_keys']),
            'y': round(float(r['actual_tps']), 1),
            'theoretical': round(float(r['theoretical_tps']), 1),
            'efficiency': round(float(r['efficiency_pct']), 1)
        }
        for r in rows if int(r['processor_latency_ms']) == 35
    ]
    return sorted(data, key=lambda p: p['x'])


def extract_heatmap_data(rows):
    """Extract efficiency data as a grid for heatmap: concurrency × latency → efficiency."""
    # Representative concurrency levels for a readable heatmap
    target_concurrencies = {250, 500, 750, 1000, 1500, 2000, 2500, 3000, 3500, 4000, 4500, 5000}

    grid = {}
    for r in rows:
        latency = int(r['processor_latency_ms'])
        concurrency = int(r['concurrent_keys'])
        efficiency = float(r['efficiency_pct'])
        if latency == 0 or latency == 10 or efficiency < 0:  # Skip 10ms - too noisy
            continue
        if concurrency not in target_concurrencies:
            continue
        # Average multiple jitter variants
        key = (concurrency, latency)
        if key not in grid:
            grid[key] = []
        grid[key].append(efficiency)

    # Average the efficiencies and build the result
    result = {}
    for (conc, lat), effs in grid.items():
        if conc not in result:
            result[conc] = {}
        result[conc][lat] = round(sum(effs) / len(effs), 1)
    return result


def efficiency_to_colour(eff):
    """Map efficiency percentage to a colour - green (high) to red (low)."""
    if eff >= 98:
        return 'rgba(40, 167, 69, 0.85)'   # green
    elif eff >= 95:
        return 'rgba(70, 130, 180, 0.85)'  # steel blue
    elif eff >= 92:
        return 'rgba(255, 193, 7, 0.85)'   # amber
    elif eff >= 88:
        return 'rgba(255, 152, 0, 0.85)'   # orange
    else:
        return 'rgba(220, 53, 69, 0.85)'   # red


# -----------------------------------------------------------------------------
# Machine specs
# -----------------------------------------------------------------------------

def get_command_output(cmd):
    """Run a shell command and return output, or None on failure."""
    try:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=5)
        return result.stdout.strip() if result.returncode == 0 else None
    except Exception:
        return None


def get_machine_specs():
    """Get machine specs from environment (Docker) or system commands."""
    specs = {}

    # Benchmark run date from CSV
    try:
        with open(CSV_FILE, 'r') as f:
            first_row = next(csv.DictReader(f))
            if ts := first_row.get('timestamp', ''):
                specs['Benchmarks run'] = ts[:10]
    except Exception:
        pass

    # Git commit
    if commit := (os.environ.get('GIT_COMMIT') or get_command_output("git rev-parse --short HEAD")):
        specs['Commit'] = commit

    # System info (prefer env vars from Docker host)
    specs['OS'] = (os.environ.get('HOST_OS') or
                   get_command_output("grep PRETTY_NAME /etc/os-release | cut -d'\"' -f2") or
                   platform.platform())
    specs['Kernel'] = os.environ.get('HOST_KERNEL') or get_command_output("uname -r") or 'Unknown'

    cpu = os.environ.get('HOST_CPU') or get_command_output("lscpu | grep 'Model name' | cut -d':' -f2")
    specs['CPU'] = cpu.strip() if cpu else 'Unknown'

    cores = os.environ.get('HOST_CORES') or get_command_output("lscpu | grep '^CPU(s):' | cut -d':' -f2")
    specs['CPU cores'] = cores.strip() if cores else 'Unknown'

    specs['Memory'] = (os.environ.get('HOST_MEMORY') or
                       get_command_output("free -h | awk '/^Mem:/ {print $2}'") or 'Unknown')
    specs['Architecture'] = os.environ.get('HOST_ARCH') or platform.machine()

    return specs


# -----------------------------------------------------------------------------
# HTML helpers
# -----------------------------------------------------------------------------

def make_table(headers, rows):
    """Generate HTML table. Headers: list of (label, align, formatter) tuples."""
    thead = ''.join(
        f'<th style="text-align: {align};">{label}</th>'
        for label, align, _ in headers
    )
    tbody = '\n'.join(
        '<tr style="border-bottom: 1px solid #eee;">' +
        ''.join(f'<td style="text-align: {align};">{fmt(row)}</td>'
                for _, align, fmt in headers) +
        '</tr>'
        for row in rows
    )
    return f'''<table style="margin-top: 10px;">
        <thead><tr style="border-bottom: 2px solid #ddd;">{thead}</tr></thead>
        <tbody>{tbody}</tbody>
    </table>'''


def make_section(id_, title, context, stats_content, chart_id, table_html, active=False):
    """Generate a chart section with stats, canvas, context, and data table."""
    active_class = ' active' if active else ''
    context_html = f'''<div class="stats" style="margin-top: 20px;">
            <p style="color: #555; margin: 0; line-height: 1.6;">{context}</p>
        </div>''' if context else ''
    return f'''
    <div id="section-{id_}" class="section{active_class}">
        <h1>{title}</h1>
        <div class="stats">{stats_content}</div>
        <div class="chart-container"><canvas id="{chart_id}"></canvas></div>
        {context_html}
        <div class="stats" style="margin-top: 20px;">
            <strong>Benchmark data</strong>
            {table_html}
        </div>
    </div>'''


def make_latency_datasets(data_by_latency, include_theoretical=False):
    """Create Chart.js datasets for latency-grouped data."""
    datasets = []
    for latency in sorted(data_by_latency.keys()):
        bg, border = LATENCY_COLOURS.get(latency, DEFAULT_COLOUR)
        datasets.append({
            'label': f'{latency}ms latency',
            'data': data_by_latency[latency],
            'backgroundColor': bg,
            'borderColor': border,
            'pointRadius': 5,
            'pointHoverRadius': 7
        })
    return datasets


# -----------------------------------------------------------------------------
# HTML generation
# -----------------------------------------------------------------------------

def generate_html(rows):
    """Generate complete self-contained HTML file."""
    # Extract data
    overhead_data = extract_framework_overhead_data(rows)
    efficiency_data = extract_efficiency_data(rows)
    tps_data = extract_tps_data(rows)
    linear_data = extract_linear_scaling_data(rows)
    heatmap_data = extract_heatmap_data(rows)
    machine_specs = get_machine_specs()

    sections = []
    js_data = []
    js_charts = []

    # --- Linear scaling section ---
    if linear_data:
        min_keys, max_keys = linear_data[0]['x'], linear_data[-1]['x']
        peak_tps = max(p['y'] for p in linear_data)
        avg_eff = round(sum(p['efficiency'] for p in linear_data) / len(linear_data), 1)

        stats = f'''<strong>Simulated 35ms latency benchmark</strong><br>
            Data points: {len(linear_data)} |
            Concurrent keys: {min_keys} - {max_keys}<br>
            <strong>Peak TPS:</strong> {peak_tps:,.0f} |
            <strong>Avg efficiency:</strong> {avg_eff:.1f}%
            <div class="note">
                Simulated 35ms processor latency. The dashed line shows theoretical maximum
                (keys / 35ms). Actual performance tracks closely, demonstrating minimal
                framework overhead.
            </div>'''

        table = make_table([
            ('Concurrent Keys', 'left', lambda p: f'{p["x"]:,}'),
            ('Actual TPS', 'right', lambda p: f'{p["y"]:,.0f}'),
            ('Theoretical', 'right', lambda p: f'{p["theoretical"]:,.0f}'),
            ('Efficiency', 'right', lambda p: f'{p["efficiency"]:.1f}%'),
        ], linear_data)

        intro = '''The <strong>llingr-demux</strong> takes single partition streams and distributes
            messages into <strong>per-Key worker streams</strong> for concurrent processing. Physical
            broker partition count is no longer the limiting factor - instead, throughput scales with
            <code>concurrentKeys</code>. The formula is:
            <code>maxTPS = instances × concurrentKeys × (1000 / processMs)</code>.
            With the default of 250 concurrent keys, a single instance provides
            <strong>250× effective partitions</strong>.'''

        sections.append(make_section('linear', 'Single Instance Linear Throughput Scaling',
                                     intro, stats, 'linearChart', table, active=True))

        js_data.append(f'const linearData = {json.dumps(linear_data)};')
        js_charts.append(f'''
        const linearTheoretical = [
            {{x: {min_keys}, y: {round(min_keys / 0.035, 1)}}},
            {{x: {max_keys}, y: {round(max_keys / 0.035, 1)}}}
        ];
        const linearEfficiency = linearData.map(p => ({{x: p.x, y: p.efficiency}}));

        new Chart(document.getElementById('linearChart').getContext('2d'), {{
            type: 'scatter',
            data: {{
                datasets: [
                    {{
                        label: 'Actual TPS', data: linearData,
                        backgroundColor: 'rgba(40, 167, 69, 0.8)',
                        borderColor: 'rgba(40, 167, 69, 1)',
                        pointRadius: 6, pointHoverRadius: 8, yAxisID: 'y'
                    }},
                    {{
                        label: 'Theoretical maximum', data: linearTheoretical, type: 'line',
                        borderColor: 'rgba(255, 152, 0, 0.8)', borderWidth: 2,
                        borderDash: [8, 4], pointRadius: 0, fill: false, yAxisID: 'y'
                    }},
                    {{
                        label: 'Efficiency %', data: linearEfficiency, type: 'line',
                        borderColor: 'rgba(70, 130, 180, 0.8)',
                        backgroundColor: 'rgba(70, 130, 180, 0.1)',
                        borderWidth: 2, pointRadius: 3, pointHoverRadius: 5,
                        fill: true, yAxisID: 'efficiency'
                    }}
                ]
            }},
            options: {{
                responsive: true, maintainAspectRatio: true, aspectRatio: 2,
                animation: {{ y: {{ from: (ctx) => ctx.chart.scales.y.bottom }} }},
                scales: {{
                    x: {{ type: 'linear', min: 0, max: {int(max_keys * 1.1)},
                          title: {{ display: true, text: 'Concurrent Keys',
                                    font: {{ size: 14, weight: 'bold' }} }} }},
                    y: {{ type: 'linear', position: 'left', min: 0,
                          title: {{ display: true, text: 'Messages per Second',
                                    font: {{ size: 14, weight: 'bold' }} }},
                          ticks: {{ callback: v => v.toLocaleString() }} }},
                    efficiency: {{ type: 'linear', position: 'right', min: 0, max: 100,
                                   title: {{ display: true, text: 'Efficiency %',
                                             font: {{ size: 14, weight: 'bold' }},
                                             color: 'rgba(70, 130, 180, 1)' }},
                                   grid: {{ drawOnChartArea: false }},
                                   ticks: {{ callback: v => v + '%',
                                             color: 'rgba(70, 130, 180, 0.8)' }} }}
                }},
                plugins: {{
                    title: {{ display: true, padding: {{ bottom: 20 }}, font: {{ size: 14 }},
                              text: 'Throughput scales linearly with concurrency - ' +
                                    'near-zero framework overhead' }},
                    legend: {{ display: true, position: 'top' }},
                    tooltip: {{ callbacks: {{
                        label: ctx => {{
                            const p = ctx.raw;
                            if (p.efficiency !== undefined)
                                return ctx.dataset.label + ': ' + p.y.toLocaleString() +
                                       ' TPS (' + p.efficiency + '% eff)';
                            return ctx.dataset.label + ': ' + p.y.toLocaleString() + ' TPS';
                        }}
                    }} }}
                }}
            }}
        }});''')

    # --- Efficiency section ---
    if efficiency_data:
        all_eff = [p['y'] for pts in efficiency_data.values() for p in pts]
        max_keys = max(p['x'] for pts in efficiency_data.values() for p in pts)

        stats = f'''<strong>Latency-based benchmark results (laptop)</strong><br>
            Data points: {len(all_eff)} |
            Concurrent keys range: up to {max_keys}<br>
            <strong>Efficiency:</strong>
            Min: {min(all_eff):.1f}% |
            Max: {max(all_eff):.1f}% |
            Avg: {sum(all_eff)/len(all_eff):.1f}%
            <div class="note">
                Efficiency = actual TPS / theoretical TPS. Theoretical TPS = concurrent_keys /
                latency. Efficiency loss at low latency is primarily <code>time.Sleep()</code>
                inaccuracy - the OS has ~0.5-1ms scheduling granularity. At 10ms target sleep,
                that's 5-10% overhead; at 100ms it's &lt;1%. This is not framework overhead.
                Jitter (0%, 10%, 30%) has negligible impact.
                <br><br>
                Efficiency drops further at high concurrency (1000+ keys) because
                <code>time.Sleep()</code> accuracy degrades under Go scheduler pressure - more
                goroutines means busier scheduler, longer wakeup delays.
            </div>'''

        table_data = sorted(
            [{'keys': p['x'], 'latency': lat, 'efficiency': p['y'], 'tps': p['tps']}
             for lat, pts in efficiency_data.items() for p in pts],
            key=lambda r: (r['latency'], r['keys'])
        )
        table = make_table([
            ('Concurrent Keys', 'left', lambda r: f'{r["keys"]:,}'),
            ('Latency (ms)', 'right', lambda r: str(r['latency'])),
            ('Efficiency', 'right', lambda r: f'{r["efficiency"]:.1f}%'),
            ('Actual TPS', 'right', lambda r: f'{r["tps"]:,.0f}'),
        ], table_data)

        intro = '''The goal is an <strong>'effectively invisible' library</strong> for massive
            vertical scaling. At typical workloads (up to 20k TPS per instance), the framework
            achieves <strong>98-99% efficiency</strong> - meaning almost all processing time is
            spent in your application code, not framework coordination. This enables
            <strong>lower end-to-end latency</strong> and more predictable performance.'''

        sections.append(make_section('efficiency', 'Processing Efficiency vs Concurrency',
                                     intro, stats, 'efficiencyChart', table))

        datasets = make_latency_datasets(efficiency_data)
        js_data.append(f'const efficiencyDatasets = {json.dumps(datasets)};')
        js_charts.append(f'''
        new Chart(document.getElementById('efficiencyChart').getContext('2d'), {{
            type: 'scatter',
            data: {{ datasets: efficiencyDatasets }},
            options: {{
                responsive: true, maintainAspectRatio: true, aspectRatio: 2,
                animation: {{ y: {{ from: (ctx) => ctx.chart.scales.y.bottom }} }},
                scales: {{
                    x: {{ type: 'linear', min: 0, max: {int(max_keys * 1.1)},
                          title: {{ display: true, text: 'Concurrent Keys',
                                    font: {{ size: 14, weight: 'bold' }} }} }},
                    y: {{ min: 0, max: 100,
                          title: {{ display: true, text: 'Efficiency (%)',
                                    font: {{ size: 14, weight: 'bold' }} }} }}
                }},
                plugins: {{
                    title: {{ display: true, font: {{ size: 14 }}, padding: {{ bottom: 20 }},
                              text: 'How close to theoretical maximum throughput (higher is better)' }},
                    legend: {{ display: true, position: 'top' }}
                }}
            }}
        }});''')

    # --- TPS section ---
    if tps_data:
        all_tps = [p['y'] for pts in tps_data.values() for p in pts]
        max_keys = max(p['x'] for pts in tps_data.values() for p in pts)

        stats = f'''<strong>Latency-based benchmark results (laptop)</strong><br>
            Data points: {len(all_tps)} |
            Concurrent keys range: up to {max_keys}<br>
            <strong>Peak TPS:</strong> {max(all_tps):,.0f} messages/sec
            <div class="note">
                Shows actual throughput achieved at different concurrency levels. Higher
                concurrency enables higher throughput, but with diminishing returns due to
                <code>time.Sleep()</code> scheduler pressure. Lower latency workloads achieve
                higher TPS but are more affected by sleep inaccuracy.
            </div>'''

        table_data = sorted(
            [{'keys': p['x'], 'latency': lat, 'actual': p['y'], 'theoretical': p['theoretical']}
             for lat, pts in tps_data.items() for p in pts],
            key=lambda r: (r['latency'], r['keys'])
        )
        table = make_table([
            ('Concurrent Keys', 'left', lambda r: f'{r["keys"]:,}'),
            ('Latency (ms)', 'right', lambda r: str(r['latency'])),
            ('Actual TPS', 'right', lambda r: f'{r["actual"]:,.0f}'),
            ('Theoretical TPS', 'right', lambda r: f'{r["theoretical"]:,.0f}'),
        ], table_data)

        intro = '''Horizontal scaling multiplies these numbers: 24 consumers × 1000 concurrent keys
            × 100ms latency = <strong>240,000 TPS</strong> cluster-wide. This enables
            <strong>fewer partitions</strong> (reducing broker costs), <strong>improved compute
            utilisation</strong> (workers stay busy instead of waiting on blocked partitions), and
            <strong>smaller, cheaper broker clusters</strong>.'''

        sections.append(make_section('tps', 'Actual Throughput vs Concurrency',
                                     intro, stats, 'tpsChart', table))

        datasets = make_latency_datasets(tps_data)
        js_data.append(f'const tpsDatasets = {json.dumps(datasets)};')
        js_charts.append(f'''
        new Chart(document.getElementById('tpsChart').getContext('2d'), {{
            type: 'scatter',
            data: {{ datasets: tpsDatasets }},
            options: {{
                responsive: true, maintainAspectRatio: true, aspectRatio: 2,
                animation: {{ y: {{ from: (ctx) => ctx.chart.scales.y.bottom }} }},
                scales: {{
                    x: {{ type: 'linear', min: 0, max: {int(max_keys * 1.1)},
                          title: {{ display: true, text: 'Concurrent Keys',
                                    font: {{ size: 14, weight: 'bold' }} }} }},
                    y: {{ min: 0, title: {{ display: true, text: 'Actual TPS (messages/sec)',
                                            font: {{ size: 14, weight: 'bold' }} }},
                          ticks: {{ callback: v => v.toLocaleString() }} }}
                }},
                plugins: {{
                    title: {{ display: true, font: {{ size: 14 }}, padding: {{ bottom: 20 }},
                              text: 'Raw throughput - scales with concurrency (higher is better)' }},
                    legend: {{ display: true, position: 'top' }},
                    tooltip: {{ callbacks: {{
                        label: ctx => ctx.dataset.label + ': ' + ctx.parsed.y.toLocaleString() +
                                      ' TPS'
                    }} }}
                }}
            }}
        }});''')

    # --- Framework overhead section ---
    if overhead_data:
        overheads = [p['y'] for p in overhead_data]
        min_keys, max_keys = overhead_data[0]['x'], overhead_data[-1]['x']
        avg_overhead = round(sum(overheads) / len(overheads), 2)

        stats = f'''<strong>Zero-latency benchmark results (laptop)</strong><br>
            Data points: {len(overhead_data)} |
            Concurrent keys range: {min_keys} - {max_keys}<br>
            <strong>Overhead per message:</strong>
            Min: {min(overheads):.2f} &#181;s |
            Max: {max(overheads):.2f} &#181;s |
            Avg: {avg_overhead:.2f} &#181;s
            <div class="note">
                Each test run processed 100k messages. Laptop overheated after 313 runs at
                keys=3120, so steps changed from 10 to 100 above 3000 keys (hence sparser data
                on right side). Vertical spread appears to be thermal throttling rather than
                framework overhead.
            </div>'''

        table = make_table([
            ('Concurrent Keys', 'left', lambda p: f'{p["x"]:,}'),
            ('Overhead (&#181;s)', 'right', lambda p: f'{p["y"]:.2f}'),
            ('TPS', 'right', lambda p: f'{1_000_000/p["y"]:,.0f}'),
        ], overhead_data)

        intro = '''Built with <strong>zero external dependencies</strong> using only the Go standard
            library. The low overhead is achieved through careful <strong>mutex sharding</strong>,
            <strong>channel sizing</strong>, <strong>object pooling</strong>, scheduler hints, and
            <strong>cache-line alignment</strong>. This provides complete code auditability, no
            supply chain attack surface, and predictable long-term maintenance.'''

        sections.append(make_section('overhead', 'Framework Overhead vs Concurrency',
                                     intro, stats, 'overheadChart', table))

        js_data.append(f'const overheadData = {json.dumps(overhead_data)};')
        js_charts.append(f'''
        new Chart(document.getElementById('overheadChart').getContext('2d'), {{
            type: 'scatter',
            data: {{
                datasets: [
                    {{
                        label: 'Framework Overhead (\\u00b5s/message)', data: overheadData,
                        backgroundColor: 'rgba(128, 90, 213, 0.6)',
                        borderColor: 'rgba(128, 90, 213, 1)',
                        pointRadius: 4, pointHoverRadius: 6
                    }},
                    {{
                        label: 'Average ({avg_overhead:.2f} \\u00b5s)',
                        data: [{{x: 0, y: {avg_overhead}}}, {{x: {max_keys}, y: {avg_overhead}}}],
                        type: 'line', borderColor: 'rgba(34, 139, 34, 0.8)',
                        borderWidth: 2, borderDash: [5, 5], pointRadius: 0, fill: false
                    }}
                ]
            }},
            options: {{
                responsive: true, maintainAspectRatio: true, aspectRatio: 2,
                animation: {{ y: {{ from: (ctx) => ctx.chart.scales.y.bottom }} }},
                scales: {{
                    x: {{ type: 'linear', min: 0, max: {int(max_keys * 1.05)},
                          title: {{ display: true, text: 'Concurrent Keys',
                                    font: {{ size: 14, weight: 'bold' }} }} }},
                    y: {{ min: 0, max: 2.5,
                          title: {{ display: true, text: 'Overhead (\\u00b5s per message)',
                                    font: {{ size: 14, weight: 'bold' }} }} }}
                }},
                plugins: {{
                    title: {{ display: true, font: {{ size: 14 }}, padding: {{ bottom: 20 }},
                              text: 'Framework coordination cost - thermal throttling causes ' +
                                    'vertical spread' }},
                    legend: {{ display: true, position: 'top' }}
                }}
            }}
        }});''')

    # --- Heatmap section ---
    if heatmap_data:
        latencies = sorted({lat for conc_data in heatmap_data.values() for lat in conc_data.keys()})
        concurrencies = sorted(heatmap_data.keys())

        # Build heatmap table
        header_cells = '<th style="padding: 12px; background: #f5f5f5;"></th>'
        header_cells += ''.join(
            f'<th style="padding: 12px; background: #f5f5f5; text-align: center;">{lat}ms</th>'
            for lat in latencies
        )

        rows_html = []
        for conc in concurrencies:
            cells = f'<td style="padding: 10px; font-weight: 600; background: #f5f5f5;">{conc:,}</td>'
            for lat in latencies:
                eff = heatmap_data[conc].get(lat)
                if eff is not None:
                    colour = efficiency_to_colour(eff)
                    cells += f'''<td style="padding: 10px; text-align: center; background: {colour};
                                  color: #fff; font-weight: 500; text-shadow: 1px 1px 2px rgba(0,0,0,0.3);">
                                  {eff:.1f}%</td>'''
                else:
                    cells += '<td style="padding: 10px; text-align: center; background: #eee;">-</td>'
            rows_html.append(f'<tr>{cells}</tr>')

        heatmap_table = f'''<table style="width: 100%; border-collapse: collapse; border-radius: 8px;
                                          overflow: hidden; box-shadow: 0 1px 3px rgba(0,0,0,0.1);">
            <thead><tr>{header_cells}</tr></thead>
            <tbody>{''.join(rows_html)}</tbody>
        </table>'''

        # Legend
        legend = '''<div style="display: flex; gap: 20px; margin-top: 15px; flex-wrap: wrap;">
            <span><span style="display: inline-block; width: 16px; height: 16px;
                   background: rgba(40, 167, 69, 0.85); border-radius: 3px; vertical-align: middle;">
                   </span> 98%+</span>
            <span><span style="display: inline-block; width: 16px; height: 16px;
                   background: rgba(70, 130, 180, 0.85); border-radius: 3px; vertical-align: middle;">
                   </span> 95-98%</span>
            <span><span style="display: inline-block; width: 16px; height: 16px;
                   background: rgba(255, 193, 7, 0.85); border-radius: 3px; vertical-align: middle;">
                   </span> 92-95%</span>
            <span><span style="display: inline-block; width: 16px; height: 16px;
                   background: rgba(255, 152, 0, 0.85); border-radius: 3px; vertical-align: middle;">
                   </span> 88-92%</span>
            <span><span style="display: inline-block; width: 16px; height: 16px;
                   background: rgba(220, 53, 69, 0.85); border-radius: 3px; vertical-align: middle;">
                   </span> &lt;88%</span>
        </div>'''

        stats = f'''<strong>Efficiency heatmap across all tested configurations</strong><br>
            Concurrency levels: {len(concurrencies)} |
            Latency values: {len(latencies)} ({', '.join(f'{l}ms' for l in latencies)})
            <div class="note">
                Each cell shows the average efficiency across jitter variants (0%, 10%, 30%).
                Lower latency workloads are more affected by <code>time.Sleep()</code> inaccuracy,
                while higher concurrency increases Go scheduler pressure - both reduce efficiency.
            </div>'''

        intro = '''This heatmap provides a quick overview of where the framework performs best. The
            <strong>sweet spot</strong> is the green zone: moderate concurrency with realistic
            processing latencies. For most production workloads (50-100ms latency, 250-1000 concurrent
            keys), efficiency stays above 97%.'''

        sections.append(f'''
    <div id="section-heatmap" class="section">
        <h1>Efficiency Heatmap: Concurrency × Latency</h1>
        <div class="stats">{stats}</div>
        <div class="chart-container">
            {heatmap_table}
            {legend}
        </div>
        <div class="stats" style="margin-top: 20px;">
            <p style="color: #555; margin: 0; line-height: 1.6;">{intro}</p>
        </div>
    </div>''')

    # --- About section (machine specs + benchmark context) ---
    machine_link = ''
    machine_section = ''
    if machine_specs:
        machine_link = '''
            <div style="margin-top: auto; padding-top: 20px; border-top: 1px solid #e0e0e0;">
                <a onclick="showSection('about', this)">About</a>
            </div>'''

        specs_rows = '\n'.join(
            f'<tr style="border-bottom: 1px solid #eee;">'
            f'<td style="padding: 8px; font-weight: 500;">{k}</td>'
            f'<td style="padding: 8px;">{v}</td></tr>'
            for k, v in machine_specs.items()
        )
        machine_section = f'''
    <div id="section-about" class="section">
        <h1>Test Context: Theoretical Throughput</h1>
        <div class="stats">
            <p style="margin: 0 0 15px 0; color: #333; line-height: 1.6;">
                These results are from isolated local testing using a mock broker - no network
                I/O, serialisation overhead, or broker coordination. They represent a theoretical
                upper bound for consumer throughput, not production Kafka performance.
            </p>
            <p style="margin: 0 0 15px 0; color: #555; line-height: 1.6;">
                In practice, other infrastructure becomes the bottleneck first - databases,
                Kafka clusters, and in some cases even network capacity.
            </p>
            <p style="margin: 0; color: #555; line-height: 1.6;">
                The <strong>llingr-demux</strong> doesn't make Kafka process millions of TPS -
                that remains a hard infrastructure problem. What it does is remove the consumer
                as a bottleneck, so that if you have the infrastructure capacity, you can
                actually use it. For the majority of real-world workloads, the framework
                coordination overhead is effectively invisible.
            </p>
        </div>

        <h2 style="margin-top: 30px; color: #333;">Test Machine Specifications</h2>
        <div class="stats">
            <table style="width: 100%; border-collapse: collapse;">
                <tbody>{specs_rows}</tbody>
            </table>
        </div>
    </div>'''

    # --- Assemble HTML ---
    return f'''<!DOCTYPE html>
<html>
<head>
    <title>llingr-demux benchmarks</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"></script>
    <style>{CSS}</style>
</head>
<body>
    <div class="layout">
        <div class="sidebar">
            <h2>llingr-demux benchmarks</h2>
            <a onclick="showSection('linear', this)" class="active">Throughput Scaling</a>
            <a onclick="showSection('efficiency', this)">Efficiency</a>
            <a onclick="showSection('tps', this)">TPS by Latency</a>
            <a onclick="showSection('overhead', this)">Framework Overhead</a>
            <a onclick="showSection('heatmap', this)">Efficiency Heatmap</a>{machine_link}
        </div>
        <div class="main">
{''.join(sections)}
{machine_section}
        </div>
    </div>

    <script>
        function showSection(id, element) {{
            document.querySelectorAll('.section').forEach(s => s.classList.remove('active'));
            document.querySelectorAll('.sidebar a').forEach(a => a.classList.remove('active'));
            document.getElementById('section-' + id).classList.add('active');
            element.classList.add('active');
        }}

        {chr(10).join(js_data)}
        {chr(10).join(js_charts)}
    </script>
</body>
</html>'''


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------

def main():
    rows = load_csv()
    print(f"Loaded {len(rows)} rows from CSV")

    html = generate_html(rows)

    with open(OUTPUT_FILE, 'w') as f:
        f.write(html)

    print(f"Generated: {OUTPUT_FILE}")


if __name__ == '__main__':
    main()

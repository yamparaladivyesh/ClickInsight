
import { useEffect, useState } from 'react'
import { useNavigate } from 'react-router-dom'
import {
  Activity, ArrowLeft, BarChart3, Route, RefreshCw, ShoppingCart,
  Target, Users, Compass, MousePointerClick, TrendingUp,
  CircleAlert, Layers3, ArrowRight, Repeat2, Clock3, Globe2,
  Smartphone, ShoppingBag
} from 'lucide-react'
import {
  ResponsiveContainer, FunnelChart, Funnel, Tooltip, LabelList, Cell,
  BarChart, Bar, XAxis, YAxis, CartesianGrid, AreaChart, Area,
  PieChart, Pie, Legend, LineChart, Line
} from 'recharts'
import './Analytics.css'

const API = 'http://127.0.0.1:8000'
const COLORS = ['#59d9cb', '#75aaff', '#b29bff', '#f3b86a', '#f28fba', '#6ec5ee', '#9bdc91']

const arr = value => Array.isArray(value) ? value : []
const rows = (data, key) => arr(data?.[key]).filter(r => r && typeof r === 'object')

function value(row, keys, fallback = null) {
  for (const key of keys) {
    if (row?.[key] !== undefined && row[key] !== null && row[key] !== '') return row[key]
  }
  return fallback
}
const num = v => {
  if (v === null || v === undefined || v === '') return null
  const n = Number(v)
  return Number.isFinite(n) ? n : null
}
const fmt = v => num(v) === null ? '—' : Number(v).toLocaleString('en-IN', { maximumFractionDigits: 1 })
const pct = v => num(v) === null ? '—' : `${(Math.abs(Number(v)) <= 1 ? Number(v) * 100 : Number(v)).toFixed(1)}%`
const label = v => String(v ?? '').replaceAll('_', ' ').replace(/\b\w/g, c => c.toUpperCase())
const cleanName = v => {
  if (v === null || v === undefined || String(v).trim() === '') return ''
  const s = String(v).trim()
  if (/^(unknown|null|none|nan|undefined)$/i.test(s)) return ''
  return label(s)
}

function Metric({ title, value: metricValue, note, icon: Icon, tone = '' }) {
  return <article className="metric-card">
    <div className="metric-top"><span>{title}</span><i className={tone}><Icon size={17} /></i></div>
    <strong>{metricValue}</strong><small>{note}</small>
  </article>
}

function Panel({ title, subtitle, children, wide = false }) {
  return <section className={`chart-panel ${wide ? 'wide-panel' : ''}`}>
    <div className="chart-heading"><div><h3>{title}</h3><p>{subtitle}</p></div></div>
    {children}
  </section>
}

function Empty({ children = 'This metric is not available in the saved results.' }) {
  return <div className="empty-chart"><BarChart3 size={25} /><span>{children}</span></div>
}

function Tip({ active, payload, label: chartLabel }) {
  if (!active || !payload?.length) return null
  return <div className="custom-tooltip">
    <b>{label(chartLabel ?? payload[0]?.name)}</b>
    {payload.map((p, i) => <div key={i}><span>{label(p.name ?? p.dataKey)}</span><strong>{fmt(p.value)}</strong></div>)}
  </div>
}

function RankChart({ data, nameKeys, countKeys, color = '#59d9cb', limit = 8, nameWidth = 125 }) {
  const chartData = data.map(row => ({
    name: cleanName(value(row, nameKeys)),
    value: num(value(row, countKeys)),
    rate: num(value(row, ['purchase_session_rate'])),
  })).filter(r => r.name && r.value !== null)
    .sort((a, b) => b.value - a.value).slice(0, limit)

  if (!chartData.length) return <Empty>No valid labelled records are available for this chart.</Empty>
  return <ResponsiveContainer width="100%" height={Math.max(220, chartData.length * 42)}>
    <BarChart data={chartData} layout="vertical" margin={{ top: 5, right: 25, left: 8, bottom: 5 }}>
      <CartesianGrid stroke="#263449" strokeDasharray="3 3" horizontal={false} />
      <XAxis type="number" stroke="#8293ac" tick={{ fontSize: 10 }} />
      <YAxis type="category" dataKey="name" width={nameWidth} stroke="#8293ac" tick={{ fontSize: 10 }} />
      <Tooltip content={<Tip />} cursor={{ fill: '#1a293a' }} />
      <Bar dataKey="value" name="Sessions" fill={color} radius={[0, 5, 5, 0]} maxBarSize={24} />
    </BarChart>
  </ResponsiveContainer>
}

function TransitionFlow({ data }) {
  const edges = data.map(row => ({
    from: cleanName(value(row, ['from_event'])),
    to: cleanName(value(row, ['to_event'])),
    count: num(value(row, ['transition_count'])),
    probability: num(value(row, ['transition_probability'])),
  })).filter(e => e.from && e.to && e.count !== null)
    .sort((a, b) => b.count - a.count).slice(0, 12)

  if (!edges.length) return <Empty>No labelled event transitions are available.</Empty>

  const sources = [...new Set(edges.map(e => e.from))]
  const targets = [...new Set(edges.map(e => e.to))]
  const width = 850
  const height = Math.max(300, Math.max(sources.length, targets.length) * 49 + 50)
  const pos = {}
  sources.forEach((name, i) => { pos[`s:${name}`] = { x: 10, y: 38 + i * (height - 65) / Math.max(sources.length - 1, 1) } })
  targets.forEach((name, i) => { pos[`t:${name}`] = { x: 690, y: 38 + i * (height - 65) / Math.max(targets.length - 1, 1) } })

  return <div className="flow-scroll">
    <svg className="flow-svg" viewBox={`0 0 ${width} ${height}`} role="img" aria-label="Most frequent event transitions">
      <text x="10" y="17" className="flow-caption">CURRENT EVENT</text>
      <text x="690" y="17" className="flow-caption">NEXT EVENT</text>
      {edges.map((e, i) => {
        const a = pos[`s:${e.from}`], b = pos[`t:${e.to}`]
        const mid = (a.x + 140 + b.x) / 2
        return <g key={`${e.from}-${e.to}-${i}`}>
          <path d={`M ${a.x + 140} ${a.y} C ${mid} ${a.y}, ${mid} ${b.y}, ${b.x} ${b.y}`}
            fill="none" stroke={COLORS[i % COLORS.length]} strokeOpacity=".5"
            strokeWidth={Math.max(1.5, Math.min(8, Math.sqrt(e.count) / 8))} />
          <title>{`${e.from} → ${e.to}: ${fmt(e.count)} transitions; probability ${pct(e.probability)}`}</title>
        </g>
      })}
      {sources.map(name => {
        const p = pos[`s:${name}`]
        return <g key={`s-${name}`}>
          <rect x={p.x} y={p.y - 13} width="140" height="26" rx="6" fill="#15263a" stroke="#34516a" />
          <text x={p.x + 70} y={p.y + 4} textAnchor="middle" className="flow-node">{name.slice(0, 19)}</text>
        </g>
      })}
      {targets.map(name => {
        const p = pos[`t:${name}`]
        return <g key={`t-${name}`}>
          <rect x={p.x} y={p.y - 13} width="150" height="26" rx="6" fill="#15312f" stroke="#35756d" />
          <text x={p.x + 75} y={p.y + 4} textAnchor="middle" className="flow-node">{name.slice(0, 20)}</text>
        </g>
      })}
    </svg>
  </div>
}

export default function Analytics() {
  const navigate = useNavigate()
  const [data, setData] = useState(null)
  const [tab, setTab] = useState('')
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState('')

  async function loadResults() {
    setLoading(true)
    setError('')
    try {
      const response = await fetch(`${API}/api/results`)
      if (!response.ok) throw new Error(`HTTP ${response.status}`)
      setData(await response.json())
    } catch (e) {
      setError(`Could not load saved results. Check that the backend is running. ${e.message}`)
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => { loadResults() }, [])

  const funnelRaw = rows(data, 'funnel_summary')
  const timesRaw = rows(data, 'funnel_time_metrics')
  const shortcutsRaw = rows(data, 'funnel_shortcuts')
  const transitions = rows(data, 'journey_transitions')
  const journeys = rows(data, 'common_journeys')
  const segments = rows(data, 'journey_segments')
  const lengths = rows(data, 'journey_length_distribution')
  const entries = rows(data, 'entry_events')
  const exits = rows(data, 'exit_events')
  const repeated = rows(data, 'repeated_behavior')

  const summary = Array.isArray(data?.journey_summary) ? data.journey_summary[0] || {} : data?.journey_summary || {}

  const stages = funnelRaw.map((r, i) => ({
    name: cleanName(value(r, ['stage', 'stage_name', 'event', 'event_name', 'name'], `Stage ${i + 1}`)),
    value: num(value(r, ['count', 'sessions', 'session_count', 'event_count', 'stage_count', 'total'])),
  })).filter(s => s.name && s.value !== null)

  const stageInsights = stages.map((stage, i) => {
    const previous = stages[i - 1]
    const conversion = previous?.value > 0 ? stage.value / previous.value * 100 : null
    return { ...stage, conversion, drop: conversion === null ? null : Math.max(0, 100 - conversion) }
  })

  const timeData = timesRaw.map(r => ({
    name: cleanName(value(r, ['metric', 'stage', 'name', 'event', 'stage_name'])),
    median: num(value(r, ['median_minutes', 'median_time_minutes', 'median_time', 'median'])),
    p90: num(value(r, ['p90_minutes', 'p90_time_minutes', 'p90'])),
    value: num(value(r, ['median_minutes', 'median_time_minutes', 'median_time', 'average_minutes', 'mean_minutes', 'value', 'minutes'])),
  })).filter(r => r.name && (r.median !== null || r.p90 !== null || r.value !== null))

  const shortcutData = shortcutsRaw.map(r => ({
    name: cleanName(value(r, ['path', 'shortcut', 'journey', 'name', 'from_event'])),
    count: num(value(r, ['count', 'sessions', 'session_count', 'shortcut_count', 'frequency'])),
  })).filter(r => r.name && r.count !== null)

  const lengthData = lengths.map(r => ({
    name: String(value(r, ['event_count', 'journey_length', 'length', 'events', 'bucket', 'label'], '')),
    sessions: num(value(r, ['session_count', 'count', 'sessions', 'frequency', 'value'])),
    purchaseRate: num(value(r, ['purchase_session_rate'])),
  })).filter(r => r.name && r.sessions !== null)

  const bySegment = type => segments.filter(r => String(r.segment_type ?? '').toLowerCase() === type)
  const devices = bySegment('device')
  const sources = bySegment('source')
  const countries = bySegment('country')
  const totalSessions = value(summary, ['session_count', 'total_sessions', 'total_session_count']) ?? stages[0]?.value
  const purchaseSessions = value(summary, ['purchase_sessions', 'sessions_with_purchase'])
  const purchaseRate = value(summary, ['purchase_session_rate', 'purchase_rate'])
  const meanEvents = value(summary, ['mean_events_per_session', 'average_events_per_session'])
  const repeatSummary = repeated[0] || {}

  return <main className="analytics-page">
    <header className="dashboard-nav">
      <a className="brand" href="/"><span className="brand-icon"><Activity size={22} /></span><span>CLICK<span className="accent">INSIGHT</span></span></a>
      <div className="nav-actions"><span className="saved-label"><i /> SAVED ANALYSIS</span><button className="back-button" onClick={() => navigate('/')}><ArrowLeft size={16} /> Home</button></div>
    </header>

    <section className="dashboard-content">
      <div className="dashboard-title">
        <span className="section-kicker">CLICKSTREAM INTELLIGENCE PLATFORM</span>
        <h1>Analytics <span>Workspace</span></h1>
        <p>Explore conversion bottlenecks, event transitions, and customer behaviour using saved pipeline results.</p>
      </div>

      <nav className="analysis-tabs">
        <button className={tab === 'funnel' ? 'active' : ''} onClick={() => setTab('funnel')}><BarChart3 size={17} /> Funnel Analysis</button>
        <button className={tab === 'journey' ? 'active' : ''} onClick={() => setTab('journey')}><Route size={17} /> User Journey Analysis</button>
      </nav>

      {loading && <div className="page-state"><span className="spinner" /> Loading saved results…</div>}
      {error && <div className="error-state"><CircleAlert size={22} /><div><b>Unable to load results</b><p>{error}</p><button onClick={loadResults}>Try again</button></div></div>}

      {!loading && !error && !tab && <div className="dashboard-empty">
        <div className="empty-icon"><Compass size={30} /></div>
        <h2>Select an analysis above ↑</h2>
        <p>Choose an analysis to explore the patterns in your saved clickstream results.</p>
        <div className="module-preview">
          <button onClick={() => setTab('funnel')}><BarChart3 size={22} /><span><b>Funnel Analysis</b><small>Conversion and drop-off patterns</small></span><ArrowRight size={17} /></button>
          <button onClick={() => setTab('journey')}><Route size={22} /><span><b>User Journey Analysis</b><small>Event flow and behavioural patterns</small></span><ArrowRight size={17} /></button>
        </div>
      </div>}

      {!loading && !error && tab === 'funnel' && <div className="dashboard-panel">
        <div className="panel-heading"><div><span className="section-kicker">CONVERSION PERFORMANCE</span><h2>Funnel Analysis</h2><p>Understand stage conversion, loss points, and the time required to progress.</p></div><span className="saved-badge">Saved results</span></div>

        <div className="metric-grid">
          <Metric title="Product Views" value={fmt(stages[0]?.value)} note="Funnel entry stage" icon={MousePointerClick} tone="blue" />
          <Metric title="Added to Cart" value={fmt(stages[1]?.value)} note="Cart stage" icon={ShoppingCart} />
          <Metric title="Checkout" value={fmt(stages[2]?.value)} note="Checkout stage" icon={Layers3} tone="purple" />
          <Metric title="Purchases" value={fmt(stages[3]?.value)} note="Final funnel stage" icon={Target} tone="green" />
        </div>

        <div className="chart-grid">
          <Panel title="Conversion funnel" subtitle="Observed counts at each purchase stage." wide>
            {stages.length ? <ResponsiveContainer width="100%" height={330}>
              <FunnelChart>
                <Tooltip formatter={v => fmt(v)} />
                <Funnel dataKey="value" data={stages} isAnimationActive>
                  {stages.map((s, i) => <Cell key={s.name} fill={COLORS[i % COLORS.length]} />)}
                  <LabelList dataKey="name" position="right" fill="#e3edf9" stroke="none" />
                </Funnel>
              </FunnelChart>
            </ResponsiveContainer> : <Empty>No funnel stages were returned.</Empty>}
          </Panel>

          <Panel title="Stage conversion health" subtitle="Share of sessions reaching each stage from the previous stage.">
            {stageInsights.length > 1 ? <div className="conversion-list">
              {stageInsights.map((s, i) => <div className="conversion-row" key={s.name}>
                <div className="conversion-label"><span>{i ? `${stages[i - 1].name} → ${s.name}` : s.name}</span><b>{i ? pct(s.conversion) : 'Baseline'}</b></div>
                <div className="conversion-track"><i style={{ width: `${Math.max(0, Math.min(100, i ? s.conversion ?? 0 : 100))}%` }} /></div>
                {i > 0 && <small>{pct(s.drop)} did not reach this stage from the previous stage</small>}
              </div>)}
            </div> : <Empty>At least two funnel stages are needed.</Empty>}
          </Panel>

          <Panel title="Where sessions drop off" subtitle="Count difference between adjacent stages.">
            {stageInsights.length > 1 ? <ResponsiveContainer width="100%" height={280}>
              <BarChart data={stageInsights.slice(1).map((s, i) => ({ name: s.name, drop: Math.max(0, stageInsights[i].value - s.value) }))} margin={{ top: 8, right: 12, left: 0, bottom: 22 }}>
                <CartesianGrid stroke="#263449" strokeDasharray="3 3" />
                <XAxis dataKey="name" stroke="#8293ac" tick={{ fontSize: 10 }} />
                <YAxis stroke="#8293ac" tick={{ fontSize: 10 }} />
                <Tooltip content={<Tip />} />
                <Bar dataKey="drop" name="Stage count difference" fill="#f3b86a" radius={[5, 5, 0, 0]} maxBarSize={42} />
              </BarChart>
            </ResponsiveContainer> : <Empty>Drop-off needs multiple stages.</Empty>}
          </Panel>

          <Panel title="Time between funnel events" subtitle="Timing metrics supplied by the saved pipeline.">
            {timeData.length ? <ResponsiveContainer width="100%" height={280}>
              <LineChart data={timeData} margin={{ top: 8, right: 12, left: 0, bottom: 25 }}>
                <CartesianGrid stroke="#263449" strokeDasharray="3 3" />
                <XAxis dataKey="name" stroke="#8293ac" tick={{ fontSize: 10 }} angle={-12} textAnchor="end" />
                <YAxis stroke="#8293ac" tick={{ fontSize: 10 }} />
                <Tooltip content={<Tip />} />
                <Line dataKey="value" name="Time (minutes)" stroke="#75aaff" strokeWidth={3} dot={{ r: 4, fill: '#75aaff' }} activeDot={{ r: 6 }} />
              </LineChart>
            </ResponsiveContainer> : <Empty>No compatible timing metrics are available.</Empty>}
          </Panel>

          <Panel title="Frequently observed shortcut paths" subtitle="Paths reported by the funnel pipeline." wide>
            {shortcutData.length ? <RankChart data={shortcutsRaw} nameKeys={['path', 'shortcut', 'journey', 'name', 'from_event']} countKeys={['count', 'sessions', 'session_count', 'shortcut_count', 'frequency']} color="#b29bff" /> : <Empty>No shortcut-path data is available.</Empty>}
          </Panel>
        </div>
        <p className="analysis-note">Drop-off differences describe count differences between adjacent stages. They are not necessarily unique-user losses unless the underlying stages use consistent session-level definitions.</p>
      </div>}

      {!loading && !error && tab === 'journey' && <div className="dashboard-panel">
        <div className="panel-heading"><div><span className="section-kicker">BEHAVIOUR & PATH DISCOVERY</span><h2>User Journey Analysis</h2><p>Explore event transitions, common paths, engagement depth, and differences between customer segments.</p></div><span className="saved-badge">Saved results</span></div>

        <div className="metric-grid">
          <Metric title="Sessions Analysed" value={fmt(totalSessions)} note="Observed sessions" icon={Users} tone="blue" />
          <Metric title="Purchase Sessions" value={fmt(purchaseSessions)} note="Sessions containing purchase" icon={Target} tone="green" />
          <Metric title="Mean Journey Length" value={meanEvents == null ? '—' : Number(meanEvents).toFixed(1)} note="Events per session on average" icon={Route} tone="purple" />
          <Metric title="Purchase Session Rate" value={pct(purchaseRate)} note="Observed purchase-session share" icon={TrendingUp} />
        </div>

        <div className="chart-grid">
          <Panel title="How events connect" subtitle="The most frequent consecutive event transitions." wide>
            <TransitionFlow data={transitions} />
          </Panel>

          <Panel title="Most common journey patterns" subtitle="Frequent complete paths observed across sessions." wide>
            {journeys.length ? <div className="journey-rank-list">
              {journeys.slice(0, 8).map((r, i) => {
                const path = value(r, ['journey_path', 'journey', 'path', 'sequence', 'events', 'name'])
                const count = num(value(r, ['session_count', 'count', 'sessions', 'frequency']))
                const pathText = Array.isArray(path) ? path.map(cleanName).filter(Boolean).join(' → ') : String(path ?? '').split(' -> ').map(cleanName).filter(Boolean).join(' → ')
                if (!pathText) return null
                return <div className="journey-rank" key={i}><span className="rank-number">{String(i + 1).padStart(2, '0')}</span><div className="journey-rank-body"><b>{pathText}</b>{count !== null && <small>{fmt(count)} observed sessions</small>}</div><ArrowRight size={15} /></div>
              })}
            </div> : <Empty>No common journey patterns were returned.</Empty>}
          </Panel>

          <Panel title="Journey length distribution" subtitle="How session counts vary with the number of events.">
            {lengthData.length ? <ResponsiveContainer width="100%" height={285}>
              <AreaChart data={lengthData} margin={{ top: 10, right: 10, left: 0, bottom: 10 }}>
                <defs><linearGradient id="journeyArea" x1="0" y1="0" x2="0" y2="1"><stop offset="0%" stopColor="#59d9cb" stopOpacity=".34" /><stop offset="95%" stopColor="#59d9cb" stopOpacity=".02" /></linearGradient></defs>
                <CartesianGrid stroke="#263449" strokeDasharray="3 3" />
                <XAxis dataKey="name" stroke="#8293ac" tick={{ fontSize: 10 }} />
                <YAxis stroke="#8293ac" tick={{ fontSize: 10 }} />
                <Tooltip content={<Tip />} />
                <Area type="monotone" dataKey="sessions" name="Sessions" stroke="#59d9cb" fill="url(#journeyArea)" strokeWidth={2.5} />
              </AreaChart>
            </ResponsiveContainer> : <Empty>No journey-length distribution is available.</Empty>}
          </Panel>

          <Panel title="Purchase rate by journey length" subtitle="Observed purchase-session share for each journey length.">
            {lengthData.some(r => r.purchaseRate !== null) ? <ResponsiveContainer width="100%" height={285}>
              <BarChart data={lengthData.filter(r => r.purchaseRate !== null)} margin={{ top: 10, right: 10, left: 0, bottom: 10 }}>
                <CartesianGrid stroke="#263449" strokeDasharray="3 3" />
                <XAxis dataKey="name" stroke="#8293ac" tick={{ fontSize: 10 }} />
                <YAxis stroke="#8293ac" tick={{ fontSize: 10 }} tickFormatter={v => `${(v * 100).toFixed(0)}%`} />
                <Tooltip formatter={v => pct(v)} />
                <Bar dataKey="purchaseRate" name="Purchase-session rate" fill="#75aaff" radius={[5, 5, 0, 0]} />
              </BarChart>
            </ResponsiveContainer> : <Empty>Purchase-rate data is not available by journey length.</Empty>}
          </Panel>

          <Panel title="Journey entry points" subtitle="Events where sessions most often begin.">
            <RankChart data={entries} nameKeys={['event_type']} countKeys={['session_count']} color="#75aaff" />
          </Panel>

          <Panel title="Journey exit points" subtitle="Events where sessions most often end.">
            <RankChart data={exits} nameKeys={['event_type']} countKeys={['session_count']} color="#f3b86a" />
          </Panel>

          <Panel title="Repeated interaction behaviour" subtitle="Aggregate indicators of repeated actions within sessions." wide>
            {Object.keys(repeatSummary).length ? <div className="repeat-metrics">
              <div className="repeat-card"><Repeat2 size={22} /><span>Sessions with consecutive repeats</span><strong>{fmt(repeatSummary.sessions_with_consecutive_repeats)}</strong><small>{pct(num(repeatSummary.sessions_with_consecutive_repeats) / Math.max(1, num(repeatSummary.session_count)))} of observed sessions</small></div>
              <div className="repeat-card"><MousePointerClick size={22} /><span>Sessions with repeated event types</span><strong>{fmt(repeatSummary.sessions_with_repeated_event_types)}</strong><small>{pct(num(repeatSummary.sessions_with_repeated_event_types) / Math.max(1, num(repeatSummary.session_count)))} of observed sessions</small></div>
              <div className="repeat-card"><Activity size={22} /><span>Mean consecutive repeats per session</span><strong>{fmt(repeatSummary.mean_consecutive_repeat_count_per_session)}</strong><small>Average repeated adjacent events</small></div>
              <div className="repeat-card"><Layers3 size={22} /><span>Mean repeated event types per session</span><strong>{fmt(repeatSummary.mean_repeated_event_type_count_per_session)}</strong><small>Average extra occurrences of event types</small></div>
            </div> : <Empty>No repeated-behaviour aggregate metrics are available.</Empty>}
          </Panel>

          <Panel title="Device behaviour" subtitle="Compare session volume and purchase rate by device.">
            {devices.length ? <div className="segment-chart">
              <RankChart data={devices} nameKeys={['segment_value']} countKeys={['session_count']} color="#59d9cb" />
              <div className="segment-rate-list">{devices.filter(r => cleanName(r.segment_value)).sort((a, b) => (num(b.purchase_session_rate) ?? 0) - (num(a.purchase_session_rate) ?? 0)).slice(0, 6).map((r, i) => <div key={i}><span>{cleanName(r.segment_value)} purchase rate</span><b>{pct(r.purchase_session_rate)}</b></div>)}</div>
            </div> : <Empty>No device segment data is available.</Empty>}
          </Panel>

          <Panel title="Traffic source performance" subtitle="Compare acquisition-source session volume and purchase rate.">
            {sources.length ? <div className="segment-chart">
              <RankChart data={sources} nameKeys={['segment_value']} countKeys={['session_count']} color="#75aaff" />
              <div className="segment-rate-list">{sources.filter(r => cleanName(r.segment_value)).sort((a, b) => (num(b.purchase_session_rate) ?? 0) - (num(a.purchase_session_rate) ?? 0)).slice(0, 6).map((r, i) => <div key={i}><span>{cleanName(r.segment_value)} purchase rate</span><b>{pct(r.purchase_session_rate)}</b></div>)}</div>
            </div> : <Empty>No traffic-source segment data is available.</Empty>}
          </Panel>

          <Panel title="Country performance" subtitle="Compare session volume and purchase rate by country." wide>
            {countries.length ? <div className="segment-chart">
              <RankChart data={countries} nameKeys={['segment_value']} countKeys={['session_count']} color="#b29bff" limit={10} />
              <div className="segment-rate-list">{countries.filter(r => cleanName(r.segment_value)).sort((a, b) => (num(b.purchase_session_rate) ?? 0) - (num(a.purchase_session_rate) ?? 0)).slice(0, 8).map((r, i) => <div key={i}><span>{cleanName(r.segment_value)} purchase rate</span><b>{pct(r.purchase_session_rate)}</b></div>)}</div>
            </div> : <Empty>No country segment data is available.</Empty>}
          </Panel>
        </div>
        <p className="analysis-note">Purchase-session rate means sessions containing a purchase divided by sessions in that segment. Segments with blank labels are omitted rather than displayed as “Unknown”. Transition probability describes the next observed event conditional on the previous event, not purchase conversion.</p>
      </div>}
    </section>

    <footer className="dashboard-footer">
      <span>CLICKINSIGHT · Clickstream Analytics</span>
      <span>Saved pipeline results</span>
      <button onClick={loadResults} disabled={loading}><RefreshCw size={14} /> {loading ? 'Loading…' : 'Refresh results'}</button>
    </footer>
  </main>
}

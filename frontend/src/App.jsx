
import { useState } from 'react'
import { useNavigate } from 'react-router-dom'
import {
  Activity,
  ArrowRight,
  BarChart3,
  ChartNoAxesCombined,
  MousePointer2,
  Route,
  ShieldCheck,
  Zap,
} from 'lucide-react'
import './App.css'

function App() {
  const [loading, setLoading] = useState(false)
  const navigate = useNavigate()

  async function startAnalysis() {
    setLoading(true)

    try {
      const response = await fetch('http://127.0.0.1:8000/api/analyze', {
        method: 'POST',
      })

      if (!response.ok) {
        throw new Error('Analysis failed')
      }

      navigate('/analytics')
    } catch (error) {
      alert('Could not run analysis. Make sure your FastAPI backend is running.')
    } finally {
      setLoading(false)
    }
  }

  return (
    <main className="home">
      <nav className="navbar">
        <a className="brand" href="/">
          <span className="brand-icon"><Activity size={23} /></span>
          <span>CLICK<span className="accent">INSIGHT</span></span>
        </a>
        <span className="nav-label">E-COMMERCE ANALYTICS</span>
      </nav>

      <section className="hero">
        <div className="eyebrow">
          <span className="status-dot" />
          CLICKSTREAM INTELLIGENCE PLATFORM
        </div>

        <h1>
          Understand every click.
          <br />
          <span className="gradient-text">Improve every journey.</span>
        </h1>

        <p className="hero-description">
          Transform raw clickstream data into actionable insights.
          Discover conversion bottlenecks, understand user behaviour,
          and uncover the paths that lead to purchase.
        </p>

        <button
          className="analyze-button"
          onClick={startAnalysis}
          disabled={loading}
        >
          {loading ? (
            <>
              <span className="spinner" />
              Analyzing your data...
            </>
          ) : (
            <>
              <Zap size={19} />
              Run Analysis
              <ArrowRight size={18} />
            </>
          )}
        </button>

        <div className="hero-meta">
          <span><ShieldCheck size={15} /> Data-driven insights</span>
          <span><Activity size={15} /> Funnel & journey analytics</span>
        </div>
      </section>

      <section className="analysis-section">
        <div className="section-heading">
          <div>
            <span className="section-kicker">EXPLORE YOUR DATA</span>
            <h2>Analytics that move you forward</h2>
          </div>
          <span className="section-count">02 ANALYSIS MODULES</span>
        </div>

        <div className="analysis-grid">
          <article className="analysis-card funnel-card">
            <div className="card-top">
              <div className="module-icon blue-icon">
                <BarChart3 size={25} />
              </div>
              <span className="module-number">01</span>
            </div>

            <h3>Funnel Analysis</h3>
            <p>
              Track the conversion funnel from product discovery to purchase.
              Identify drop-offs, measure stage conversion, and find
              opportunities to improve performance.
            </p>

            <div className="mini-funnel" aria-label="Conversion funnel illustration">
              <div className="funnel-stage stage-one"><span>Product views</span><i /></div>
              <div className="funnel-stage stage-two"><span>Add to cart</span><i /></div>
              <div className="funnel-stage stage-three"><span>Checkout</span><i /></div>
              <div className="funnel-stage stage-four"><span>Purchase</span><i /></div>
            </div>

            <div className="card-footer">
              <span><ChartNoAxesCombined size={16} /> Conversion performance</span>
              <ArrowRight size={18} />
            </div>
          </article>

          <article className="analysis-card journey-card">
            <div className="card-top">
              <div className="module-icon teal-icon">
                <Route size={25} />
              </div>
              <span className="module-number">02</span>
            </div>

            <h3>User Journey Analysis</h3>
            <p>
              Explore the paths customers take through your store.
              Understand event transitions, common journeys, entry points,
              and behaviours associated with purchases.
            </p>

            <div className="journey-visual" aria-label="User journey illustration">
              <div className="journey-node"><MousePointer2 size={17} /><span>Visit</span></div>
              <div className="journey-line"><i /></div>
              <div className="journey-node"><BarChart3 size={17} /><span>Explore</span></div>
              <div className="journey-line"><i /></div>
              <div className="journey-node purchase-node"><Zap size={17} /><span>Purchase</span></div>
            </div>

            <div className="card-footer">
              <span><Route size={16} /> Behaviour & pathways</span>
              <ArrowRight size={18} />
            </div>
          </article>
        </div>
      </section>

      <footer className="footer">
        <a className="brand footer-brand" href="/">
          <span className="brand-icon"><Activity size={18} /></span>
          <span>CLICK<span className="accent">INSIGHT</span></span>
        </a>
        <span>Turning clickstreams into clarity.</span>
        <span>© 2026 CLICKINSIGHT</span>
      </footer>
    </main>
  )
}

export default App

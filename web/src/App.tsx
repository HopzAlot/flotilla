import { useState } from 'react'
import './App.css'
import './styles/shared.css'
import { killNode, startNode } from './api'
import { useFleetStream } from './hooks/useFleetStream'
import ShipCard from './components/ShipCard'
import CargoPanel from './components/CargoPanel'
import TreasurePanel from './components/TreasurePanel'
import ShipsLog from './components/ShipsLog'
import Waterline from './components/Waterline'

function App() {
  const { nodes, events } = useFleetStream()
  const [busy, setBusy] = useState(false)

  async function handleKill(id: string) {
    setBusy(true)
    try {
      await killNode(id)
    } finally {
      setBusy(false)
    }
  }
  async function handleStart(id: string) {
    setBusy(true)
    try {
      await startNode(id)
    } finally {
      setBusy(false)
    }
  }

  const aliveIds = nodes.filter((n) => n.alive).map((n) => n.id)
  const captain = nodes.find((n) => n.alive && n.state === 'Leader')

  return (
    <div className="app">
      <header className="app__header">
        <svg className="compass-rose" viewBox="0 0 100 100" aria-hidden="true">
          <circle cx="50" cy="50" r="46" fill="none" stroke="currentColor" strokeWidth="0.75" />
          <circle cx="50" cy="50" r="2.5" fill="currentColor" />
          <g stroke="currentColor" strokeWidth="1">
            <line x1="50" y1="4" x2="50" y2="96" />
            <line x1="4" y1="50" x2="96" y2="50" />
          </g>
          <g stroke="currentColor" strokeWidth="0.5">
            <line x1="15.5" y1="15.5" x2="84.5" y2="84.5" />
            <line x1="15.5" y1="84.5" x2="84.5" y2="15.5" />
          </g>
          <path d="M50 4 L45 30 L50 22 L55 30 Z" fill="currentColor" />
        </svg>
        <h1>⛵ Flotilla</h1>
        <p className="app__tagline">
          A Raft cluster you can actually sink. Watch consensus survive it anyway.
        </p>
        <div className="app__status">
          {captain ? (
            <span className="pill pill--leader">Captain: {captain.id}</span>
          ) : (
            <span className="pill pill--candidate">No Captain, the sea decides</span>
          )}
        </div>
      </header>

      <section className="fleet-deck">
        <div className="fleet">
          {nodes.map((n) => (
            <ShipCard key={n.id} node={n} onKill={handleKill} onStart={handleStart} busy={busy} />
          ))}
          {nodes.length === 0 && <p className="fleet__loading">Launching the fleet…</p>}
        </div>
        <Waterline />
      </section>

      <main className="app__main">
        <div className="app__panels">
          <CargoPanel aliveIds={aliveIds} />
          <TreasurePanel aliveIds={aliveIds} />
        </div>
        <ShipsLog events={events} />
      </main>

      <footer className="app__footer">
        <p>
          Real Go processes, real <code>kill -9</code>, real elections. No mocks below deck.
        </p>
      </footer>
    </div>
  )
}

export default App

import { useEffect, useRef, useState } from 'react'
import './App.css'
import { fetchEvents, fetchNodes, killNode, startNode } from './api'
import ShipCard from './components/ShipCard'
import CargoPanel from './components/CargoPanel'
import TreasurePanel from './components/TreasurePanel'
import ShipsLog from './components/ShipsLog'
import type { FleetEvent, NodeStatus } from './types'

function App() {
  const [nodes, setNodes] = useState<NodeStatus[]>([])
  const [events, setEvents] = useState<FleetEvent[]>([])
  const [busy, setBusy] = useState(false)
  const lastSeq = useRef(0)

  useEffect(() => {
    let cancelled = false

    async function pollNodes() {
      try {
        const n = await fetchNodes()
        if (!cancelled) setNodes(n)
      } catch {
        /* orchestrator not reachable yet — just retry next tick */
      }
    }
    async function pollEvents() {
      try {
        const fresh = await fetchEvents(lastSeq.current)
        if (fresh.length && !cancelled) {
          lastSeq.current = fresh[fresh.length - 1].seq
          setEvents((prev) => [...prev, ...fresh].slice(-60))
        }
      } catch {
        /* same */
      }
    }

    pollNodes()
    pollEvents()
    const t1 = setInterval(pollNodes, 500)
    const t2 = setInterval(pollEvents, 700)
    return () => {
      cancelled = true
      clearInterval(t1)
      clearInterval(t2)
    }
  }, [])

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
        <h1>⛵ Flotilla</h1>
        <p className="app__tagline">
          A Raft cluster you can actually sink. Watch consensus survive it anyway.
        </p>
        <div className="app__status">
          {captain ? (
            <span className="pill pill--leader">Captain: {captain.id}</span>
          ) : (
            <span className="pill pill--candidate">No Captain — the sea decides</span>
          )}
        </div>
      </header>

      <section className="fleet">
        {nodes.map((n) => (
          <ShipCard key={n.id} node={n} onKill={handleKill} onStart={handleStart} busy={busy} />
        ))}
        {nodes.length === 0 && <p className="fleet__loading">Launching the fleet…</p>}
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

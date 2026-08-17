import type { FleetEvent } from '../types'

function timeLabel(ts: number): string {
  const d = new Date(ts)
  return d.toLocaleTimeString([], { hour12: false })
}

export default function ShipsLog({ events }: { events: FleetEvent[] }) {
  const ordered = [...events].reverse()
  return (
    <section className="panel panel--log">
      <h2 className="panel__title">📜 Ship's Log</h2>
      <div className="log">
        {ordered.length === 0 && <p className="log__empty">All quiet on the water…</p>}
        {ordered.map((e, i) => (
          <div className="log__line" key={e.seq} style={{ animationDelay: `${Math.min(i, 6) * 20}ms` }}>
            <span className="log__time mono">{timeLabel(e.ts)}</span>
            <span className="log__msg">{e.message}</span>
          </div>
        ))}
      </div>
    </section>
  )
}

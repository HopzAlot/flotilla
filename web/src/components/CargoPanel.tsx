import { useEffect, useState } from 'react'
import '../styles/shared.css'
import { submit } from '../api'
import { KNOWN_NODES, idForAddr } from '../types'
import type { SubmitReply } from '../types'

interface Props {
  aliveIds: string[]
  captainId?: string
}

export default function CargoPanel({ aliveIds, captainId }: Props) {
  const [op, setOp] = useState<'PUT' | 'DELETE'>('PUT')
  const [key, setKey] = useState('treasure')
  const [value, setValue] = useState('doubloons')
  const [target, setTarget] = useState(KNOWN_NODES[0].id)
  const [result, setResult] = useState<SubmitReply | null>(null)
  const [loading, setLoading] = useState(false)

  // if the ship we were talking to just sank, don't leave the dropdown
  // pointed at a corpse, follow the fleet to whoever's alive
  useEffect(() => {
    if (aliveIds.includes(target)) return
    setTarget(captainId ?? aliveIds[0] ?? target)
  }, [aliveIds, captainId, target])

  async function send(toId: string) {
    if (!key) return
    setLoading(true)
    try {
      const reply = await submit(toId, op, key, value)
      setResult(reply)
      if (reply.Success) setTarget(toId)
    } finally {
      setLoading(false)
    }
  }

  const redirectId = result?.LeaderAddr ? idForAddr(result.LeaderAddr) : undefined

  return (
    <section className="panel">
      <h2 className="panel__title">📦 Load Cargo</h2>
      <p className="panel__sub">Writes only count once a majority of the fleet has it in the hold.</p>

      <div className="panel__row">
        <select value={op} onChange={(e) => setOp(e.target.value as 'PUT' | 'DELETE')}>
          <option value="PUT">PUT</option>
          <option value="DELETE">DELETE</option>
        </select>
        <input placeholder="key" value={key} onChange={(e) => setKey(e.target.value)} />
        {op === 'PUT' && (
          <input placeholder="value" value={value} onChange={(e) => setValue(e.target.value)} />
        )}
      </div>

      <div className="panel__row">
        <span className="panel__label">deliver to</span>
        <select value={target} onChange={(e) => setTarget(e.target.value)}>
          {KNOWN_NODES.map((n) => (
            <option key={n.id} value={n.id} disabled={!aliveIds.includes(n.id)}>
              {n.id} {aliveIds.includes(n.id) ? '' : '(sunk)'}
            </option>
          ))}
        </select>
        <button className="btn btn--accent" disabled={loading} onClick={() => send(target)}>
          {loading ? 'Rowing over…' : 'Send it'}
        </button>
      </div>

      {result && (
        <div className={`result ${result.Success ? 'result--ok' : 'result--warn'}`}>
          {result.Success ? (
            <>✅ Cargo confirmed by the fleet, safe in the hold.</>
          ) : result.Error ? (
            <>
              🌊 Couldn't reach that ship, it's sunk.{' '}
              {captainId ? (
                <button className="btn btn--link" onClick={() => send(captainId)}>
                  Try the Captain, {captainId} →
                </button>
              ) : (
                'No Captain known right now.'
              )}
            </>
          ) : (
            <>
              🧭 Wrong ship. {target} isn't Captain.{' '}
              {redirectId ? (
                <button className="btn btn--link" onClick={() => send(redirectId)}>
                  Follow to {redirectId} →
                </button>
              ) : (
                'No Captain known right now.'
              )}
            </>
          )}
        </div>
      )}
    </section>
  )
}

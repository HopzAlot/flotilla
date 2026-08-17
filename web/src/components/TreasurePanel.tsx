import { useState } from 'react'
import { get } from '../api'
import { KNOWN_NODES, idForAddr } from '../types'
import type { GetReply } from '../types'

interface Props {
  aliveIds: string[]
}

export default function TreasurePanel({ aliveIds }: Props) {
  const [key, setKey] = useState('treasure')
  const [target, setTarget] = useState(KNOWN_NODES[0].id)
  const [result, setResult] = useState<GetReply | null>(null)
  const [loading, setLoading] = useState(false)

  async function fetchKey(fromId: string) {
    if (!key) return
    setLoading(true)
    try {
      const reply = await get(fromId, key)
      setResult(reply)
      if (reply.Success) setTarget(fromId)
    } finally {
      setLoading(false)
    }
  }

  const redirectId = result?.LeaderAddr ? idForAddr(result.LeaderAddr) : undefined

  return (
    <section className="panel">
      <h2 className="panel__title">🔭 Fetch Treasure</h2>
      <p className="panel__sub">
        A linearizable read: the ship must prove — right now — it still commands a majority before it answers.
      </p>

      <div className="panel__row">
        <input placeholder="key" value={key} onChange={(e) => setKey(e.target.value)} />
        <span className="panel__label">ask</span>
        <select value={target} onChange={(e) => setTarget(e.target.value)}>
          {KNOWN_NODES.map((n) => (
            <option key={n.id} value={n.id} disabled={!aliveIds.includes(n.id)}>
              {n.id} {aliveIds.includes(n.id) ? '' : '(sunk)'}
            </option>
          ))}
        </select>
        <button className="btn btn--accent" disabled={loading} onClick={() => fetchKey(target)}>
          {loading ? 'Consulting the crew…' : 'Fetch'}
        </button>
      </div>

      {result && (
        <div className={`result ${result.Success && result.Found ? 'result--ok' : result.Success ? 'result--warn' : 'result--warn'}`}>
          {result.Error ? (
            <>🌊 Couldn't reach that ship — try another.</>
          ) : result.Success && result.Found ? (
            <>
              💰 <strong>{key}</strong> = <span className="mono">{result.Value}</span>
            </>
          ) : result.Success ? (
            <>🗺️ No such treasure buried under "{key}".</>
          ) : (
            <>
              🧭 {target} won't vouch for a majority right now.{' '}
              {redirectId ? (
                <button className="btn btn--link" onClick={() => fetchKey(redirectId)}>
                  Ask {redirectId} instead →
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

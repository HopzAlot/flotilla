import type { NodeStatus } from '../types'

const ROLE_LABEL: Record<string, string> = {
  Leader: 'Captain',
  Follower: 'Deckhand',
  Candidate: 'Mutineer',
}

const ROLE_GLYPH: Record<string, string> = {
  Leader: '⛵',
  Follower: '🚣',
  Candidate: '🏴',
}

interface Props {
  node: NodeStatus
  onKill: (id: string) => void
  onStart: (id: string) => void
  busy: boolean
}

export default function ShipCard({ node, onKill, onStart, busy }: Props) {
  const role = node.alive ? node.state || 'Follower' : 'Sunk'
  const roleClass = node.alive ? role.toLowerCase() : 'sunk'

  return (
    <div className={`ship-card ship-card--${roleClass}`}>
      <div className="ship-card__glyph" aria-hidden="true">
        {node.alive ? ROLE_GLYPH[role] ?? '🚣' : '💀'}
      </div>

      <div className="ship-card__body">
        <div className="ship-card__head">
          <h3 className="ship-card__id">{node.id}</h3>
          <span className={`pill pill--${roleClass}`}>
            {node.alive ? ROLE_LABEL[role] ?? role : 'Sunk'}
          </span>
        </div>
        <p className="ship-card__addr mono">{node.addr}</p>

        {node.alive ? (
          <dl className="ship-card__stats mono">
            <div>
              <dt>term</dt>
              <dd>{node.term}</dd>
            </div>
            <div>
              <dt>commit</dt>
              <dd>{node.commitIndex}</dd>
            </div>
            <div>
              <dt>applied</dt>
              <dd>{node.lastApplied}</dd>
            </div>
            <div>
              <dt>log</dt>
              <dd>{node.logLength}</dd>
            </div>
          </dl>
        ) : (
          <p className="ship-card__epitaph">Resting on the seabed. No cargo lost — only what was never confirmed.</p>
        )}

        <button
          className={node.alive ? 'btn btn--danger' : 'btn btn--success'}
          disabled={busy}
          onClick={() => (node.alive ? onKill(node.id) : onStart(node.id))}
        >
          {node.alive ? '💣 Sink it' : '⚓ Raise it'}
        </button>
      </div>
    </div>
  )
}

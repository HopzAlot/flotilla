import '../styles/shared.css'

const REPO_URL = 'https://github.com/HopzAlot/flotilla'

export default function AboutPanel() {
  return (
    <section className="panel">
      <h2 className="panel__title">🗺️ What is Flotilla?</h2>
      <p className="panel__sub">
        A distributed key-value store built on Raft consensus, written from scratch in Go, no{' '}
        <code>hashicorp/raft</code>, no consensus library. It was built as a learning project: the
        goal was to actually understand why each safety rule in Raft exists, not just ship
        something that passes a demo.
      </p>
      <p className="panel__sub">
        Every ship above is a real OS process (an actual <code>flotillanode</code> binary), reachable
        over plain HTTP and JSON, running the identical state machine driven by the identical
        replicated log. Leader election, heartbeats, and commit-index advancement are all
        hand-built, and so is what goes beyond the usual tutorial scope: log compaction and
        snapshotting so the log doesn't grow forever, and the ReadIndex protocol so a read is
        provably as fresh as the cluster's most recent committed write, not silently stale on a
        partitioned leader.
      </p>
      <p className="panel__sub">
        Writes only come back successful once a majority has replicated them, not after the local
        append, which is what makes the failover demo meaningful. Sink the Captain mid-write with{' '}
        <code>Sink it</code> and watch the fleet re-elect a new one and keep every acknowledged
        write. Sink two out of three and watch it correctly refuse to elect anyone at all, no
        matter how many terms it burns through, because a lone ship can never prove it speaks for
        a majority. Nothing here is mocked: the same chaos-testing script that backs this dashboard
        does real <code>kill -9</code> on real leader processes.
      </p>
      <a className="btn btn--accent" href={REPO_URL} target="_blank" rel="noopener noreferrer">
        View source on GitHub
      </a>
    </section>
  )
}

import { useEffect, useRef, useState } from 'react'
import { fetchEvents, fetchNodes } from '../api'
import type { FleetEvent, NodeStatus } from '../types'

// Polls the orchestrator for live cluster state and the Ship's Log feed.
// Isolated from App so the component tree stays about layout, not fetching.
export function useFleetStream() {
  const [nodes, setNodes] = useState<NodeStatus[]>([])
  const [events, setEvents] = useState<FleetEvent[]>([])
  const lastSeq = useRef(0)

  useEffect(() => {
    let cancelled = false

    async function pollNodes() {
      try {
        const n = await fetchNodes()
        if (!cancelled) setNodes(n)
      } catch {
        /* orchestrator not reachable yet, just retry next tick */
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

  return { nodes, events }
}

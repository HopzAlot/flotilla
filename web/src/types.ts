export type NodeState = 'Leader' | 'Follower' | 'Candidate' | ''

export interface NodeStatus {
  id: string
  addr: string
  alive: boolean
  state: NodeState
  term: number
  commitIndex: number
  lastApplied: number
  logLength: number
  leaderId: string
}

export interface FleetEvent {
  seq: number
  ts: number
  message: string
}

export interface SubmitReply {
  Success: boolean
  LeaderAddr?: string
  Error?: string
}

export interface GetReply {
  Success: boolean
  Value?: string
  Found?: boolean
  LeaderAddr?: string
  Error?: string
}

// Mirrors fleetSize/nodes in cmd/orchestrator/main.go, nodeN on 8000+N.
const FLEET_SIZE = 10

export const KNOWN_NODES: { id: string; addr: string }[] = Array.from(
  { length: FLEET_SIZE },
  (_, i) => ({ id: `node${i + 1}`, addr: `localhost:${8000 + i + 1}` }),
)

export function idForAddr(addr: string): string | undefined {
  return KNOWN_NODES.find((n) => n.addr === addr)?.id
}

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

export const KNOWN_NODES: { id: string; addr: string }[] = [
  { id: 'node1', addr: 'localhost:8001' },
  { id: 'node2', addr: 'localhost:8002' },
  { id: 'node3', addr: 'localhost:8003' },
]

export function idForAddr(addr: string): string | undefined {
  return KNOWN_NODES.find((n) => n.addr === addr)?.id
}

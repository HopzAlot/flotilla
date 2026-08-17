import type { FleetEvent, GetReply, NodeStatus, SubmitReply } from './types'

export async function fetchNodes(): Promise<NodeStatus[]> {
  const res = await fetch('/api/nodes')
  return res.json()
}

export async function fetchEvents(after: number): Promise<FleetEvent[]> {
  const res = await fetch(`/api/events?after=${after}`)
  return res.json()
}

export async function killNode(id: string): Promise<void> {
  await fetch(`/api/nodes/${id}/kill`, { method: 'POST' })
}

export async function startNode(id: string): Promise<void> {
  await fetch(`/api/nodes/${id}/start`, { method: 'POST' })
}

export async function submit(
  id: string,
  op: 'PUT' | 'DELETE',
  key: string,
  value: string,
): Promise<SubmitReply> {
  const res = await fetch(`/api/nodes/${id}/submit`, {
    method: 'POST',
    body: JSON.stringify({ Op: op, Key: key, Value: value }),
  })
  return res.json()
}

export async function get(id: string, key: string): Promise<GetReply> {
  const res = await fetch(`/api/nodes/${id}/get?key=${encodeURIComponent(key)}`)
  return res.json()
}

import { TypedStore } from './TypedStore.js'
import { Map2Storage } from '../cache/Map2Storage.js'
import type { SocketClient } from '../socket/SocketClient.js'
import type { Subscription } from '../subscription/index.js'
import type { MapStoreEvents, Map2SubscriptionStatus } from '../types/events.js'
import type { RoomMap2Data } from '../types/game.js'
import type { Logger } from '../logger.js'

function canonicalize(data: RoomMap2Data): string {
  const sortedKeys = Object.keys(data).sort()
  const obj: Record<string, [number, number][] | null> = {}
  for (const k of sortedKeys) {
    const v = data[k]
    obj[k] = v ? [...v].sort((a, b) => a[0] - b[0] || a[1] - b[1]) : null
  }
  return JSON.stringify(obj)
}

export interface Map2Subscription extends Subscription {
  /** Current subscription status — updated in place when waitlist entry is promoted. */
  readonly status: () => Map2SubscriptionStatus
  /** Last data the library has for this room (memory only; synchronous). */
  readonly cachedData: () => RoomMap2Data | null
  /** Register a handler called whenever status changes. Returns a disposable. */
  onStatusChange(handler: (status: Map2SubscriptionStatus) => void): Subscription
}

export interface MapStoreOptions {
  /** Max simultaneous WebSocket roomMap2 subscriptions. Default 500. */
  maxSubscriptions?: number
}

interface ActiveEntry {
  room: string
  shard: string | null
  refCount: number
  socketSub: Subscription
  listenerSub: Subscription
  /** The unprefixed channel's subscription and listener, opened alongside whenever a shard is given. */
  legacySubs: Subscription[]
  /** Canonical hash of the last data we emitted/stored. null until first message or cache-warm init. */
  lastHash: string | null
}

interface WaitlistEntry {
  key: string
  room: string
  shard: string | null
  refCount: number
}

interface KeyState {
  status: Map2SubscriptionStatus
  statusHandlers: Set<(status: Map2SubscriptionStatus) => void>
}

export class MapStore extends TypedStore<MapStoreEvents> {
  private readonly socket: SocketClient
  private readonly storage: Map2Storage
  private readonly maxSubscriptions: number
  private readonly active = new Map<string, ActiveEntry>()
  private readonly waitlist: WaitlistEntry[] = []
  private readonly keyStates = new Map<string, KeyState>()
  private warnedAboutWaitlist = false

  constructor(socket: SocketClient, storage: Map2Storage, opts: MapStoreOptions = {}, logger?: Logger) {
    super(logger)
    this.socket = socket
    this.storage = storage
    this.maxSubscriptions = opts.maxSubscriptions ?? 500
    void this.socket.on('connected', () => this.onReconnect())
  }

  map2data(room: string, shard: string | null): RoomMap2Data | null {
    return this.storage.getMemory(room, shard)
  }

  subscribeMap2(room: string, shard: string | null): Map2Subscription {
    const mapKey = `${room}/${shard}`

    // Case 1: already active — increment refCount and reuse the open WS sub
    const activeEntry = this.active.get(mapKey)
    if (activeEntry) {
      activeEntry.refCount++
      this.logger.log('subscribeMap2', room, shard, `(active, refs: ${activeEntry.refCount})`)
      const keyState = this.getOrCreateKeyState(mapKey, 'active')
      this.emitWarmStart(room, shard)
      return this.makeSubscription(room, shard, mapKey, keyState)
    }

    // Case 2: already on waitlist — increment refCount
    const waitEntry = this.waitlist.find(e => e.key === mapKey)
    if (waitEntry) {
      waitEntry.refCount++
      this.logger.log('subscribeMap2', room, shard, `(pending, refs: ${waitEntry.refCount})`)
      const keyState = this.getOrCreateKeyState(mapKey, 'pending')
      this.emitWarmStart(room, shard)
      return this.makeSubscription(room, shard, mapKey, keyState)
    }

    // Case 3: new subscription
    if (this.active.size < this.maxSubscriptions) {
      this.activateKey(room, shard, mapKey, 1)
      this.logger.log('subscribeMap2', room, shard, '(new active)')
      const keyState = this.getOrCreateKeyState(mapKey, 'active')
      this.emit('room:map2state', { room, shard, status: 'active' })
      this.emitWarmStart(room, shard)
      return this.makeSubscription(room, shard, mapKey, keyState)
    }

    // Limit reached — enqueue on waitlist
    this.waitlist.push({ key: mapKey, room, shard, refCount: 1 })
    this.logger.log('subscribeMap2', room, shard, '(new pending)')
    const keyState = this.getOrCreateKeyState(mapKey, 'pending')
    this.emit('room:map2state', { room, shard, status: 'pending' })
    this.emitWarmStart(room, shard)
    if (!this.warnedAboutWaitlist) {
      this.logger.log(
        `subscription limit (${this.maxSubscriptions}) reached — `,
        'some rooms are on a waitlist and will be promoted as slots free up.'
      )
      this.warnedAboutWaitlist = true
    }
    return this.makeSubscription(room, shard, mapKey, keyState)
  }

  private activateKey(room: string, shard: string | null, mapKey: string, refCount: number): void {
    const channel = shard ? `roomMap2:${shard}/${room}` : `roomMap2:${room}`
    const socketSub = this.socket.subscribe(channel)
    // A screeps-launcher private server reports a shard via /api/version but serves map tiles only
    // on roomMap2:<room> (@screeps/backend socket/map.js), as RoomStore found for room:<room>. Also
    // subscribe the unprefixed form whenever a shard is given; a server that shard-prefixes its
    // channels never publishes on it, which costs one idle subscription per room there.
    const legacyChannel = shard ? `roomMap2:${room}` : null
    const cached = this.storage.getMemory(room, shard)
    const entry: ActiveEntry = {
      room,
      shard,
      refCount,
      socketSub,
      listenerSub: undefined as unknown as Subscription,
      legacySubs: [],
      lastHash: cached ? canonicalize(cached) : null,
    }
    const handleUpdate = (data: unknown) => {
      const next = data as RoomMap2Data
      const nextHash = canonicalize(next)
      if (entry.lastHash === nextHash) return
      entry.lastHash = nextHash
      void this.storage.put(room, shard, next)
      this.emit('room:map2update', { room, shard, data: next, source: 'live' })
    }
    entry.listenerSub = this.socket.on(channel, handleUpdate)
    if (legacyChannel) {
      entry.legacySubs = [this.socket.subscribe(legacyChannel), this.socket.on(legacyChannel, handleUpdate)]
    }
    this.active.set(mapKey, entry)
  }

  private promoteNext(): void {
    const next = this.waitlist.shift()
    if (!next) return

    this.activateKey(next.room, next.shard, next.key, next.refCount)
    this.logger.log('promoteNext', next.room, next.shard, '(promoted from waitlist)')

    const keyState = this.keyStates.get(next.key)
    if (keyState) {
      keyState.status = 'active'
      keyState.statusHandlers.forEach(h => h('active'))
    }

    this.emit('room:map2state', { room: next.room, shard: next.shard, status: 'active' })
  }

  private getOrCreateKeyState(mapKey: string, status: Map2SubscriptionStatus): KeyState {
    let state = this.keyStates.get(mapKey)
    if (!state) {
      state = { status, statusHandlers: new Set() }
      this.keyStates.set(mapKey, state)
    }
    return state
  }

  private makeSubscription(room: string, shard: string | null, mapKey: string, keyState: KeyState): Map2Subscription {
    let disposed = false
    return {
      status: () => keyState.status,
      cachedData: () => this.storage.getMemory(room, shard),
      onStatusChange: (handler) => {
        keyState.statusHandlers.add(handler)
        return { dispose: () => { keyState.statusHandlers.delete(handler) } }
      },
      dispose: () => {
        if (disposed) return
        disposed = true
        this.disposeSubscription(room, shard, mapKey)
      },
    }
  }

  private disposeSubscription(room: string, shard: string | null, mapKey: string): void {
    // Check waitlist first — no WS sub to close, no promotion needed
    const waitIdx = this.waitlist.findIndex(e => e.key === mapKey)
    if (waitIdx >= 0) {
      const wait = this.waitlist[waitIdx]
      wait.refCount--
      this.logger.log('unsubscribeMap2', room, shard, `(pending, refs: ${wait.refCount})`)
      if (wait.refCount <= 0) {
        this.waitlist.splice(waitIdx, 1)
        this.keyStates.delete(mapKey)
        this.logger.log('unsubscribeMap2', room, shard, '(removed from waitlist)')
      }
      return
    }

    const active = this.active.get(mapKey)
    if (!active) return

    active.refCount--
    this.logger.log('unsubscribeMap2', room, shard, `(active, refs: ${active.refCount})`)
    if (active.refCount <= 0) {
      active.socketSub.dispose()
      active.listenerSub.dispose()
      for (const sub of active.legacySubs) sub.dispose()
      this.active.delete(mapKey)
      this.keyStates.delete(mapKey)
      this.logger.log('unsubscribeMap2', room, shard, '(deactivated, promoting next)')
      this.promoteNext()
    }
  }

  private emitWarmStart(room: string, shard: string | null): void {
    const mapKey = `${room}/${shard}`
    const cached = this.storage.getMemory(room, shard)
    if (cached) {
      queueMicrotask(() => {
        if (this.isSubscribed(mapKey)) {
          this.emit('room:map2update', { room, shard, data: cached, source: 'cache' })
        }
      })
    } else {
      void this.storage.get(room, shard).then(data => {
        if (data && this.isSubscribed(mapKey)) {
          this.emit('room:map2update', { room, shard, data, source: 'cache' })
        }
      })
    }
  }

  private onReconnect(): void {
    this.logger.log('onReconnect —', this.active.size, 'active,', this.waitlist.length, 'pending')
    for (const entry of this.active.values()) {
      // After a resubscribe, the server resends initial state. Clear the dedup hash so the first
      // message after reconnect always fires an emit — even if the payload happens to match.
      entry.lastHash = null
      this.emit('room:map2state', { room: entry.room, shard: entry.shard, status: 'active' })
    }
    for (const entry of this.waitlist) {
      this.emit('room:map2state', { room: entry.room, shard: entry.shard, status: 'pending' })
    }
  }

  private isSubscribed(mapKey: string): boolean {
    return this.active.has(mapKey) || this.waitlist.some(e => e.key === mapKey)
  }
}

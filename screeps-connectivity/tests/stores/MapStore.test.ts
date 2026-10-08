import { describe, it, expect, vi } from 'vitest'
import { MapStore } from '../../src/stores/MapStore.js'
import { Map2Storage } from '../../src/cache/Map2Storage.js'
import type { RoomMap2Data } from '../../src/types/game.js'

function makeStore(maxEntries = 10000) {
  const socket = {
    subscribe: vi.fn().mockReturnValue({ dispose: vi.fn() }),
    on: vi.fn().mockReturnValue({ dispose: vi.fn() }),
  } as unknown as import('../../src/socket/SocketClient.js').SocketClient

  const storage = new Map2Storage({ adapter: null, namespace: 'test', maxEntries })
  const store = new MapStore(socket, storage)
  return { store, socket, storage }
}

describe('MapStore', () => {
  it('subscribeMap2() calls socket.subscribe with the correct channel (with shard)', () => {
    const { store, socket } = makeStore()
    store.subscribeMap2('W7N7', 'shard0')
    expect(socket.subscribe).toHaveBeenCalledWith('roomMap2:shard0/W7N7')
  })

  it('subscribeMap2() omits shard prefix when shard is null (private server)', () => {
    const { store, socket } = makeStore()
    store.subscribeMap2('E9N3', null)
    expect(socket.subscribe).toHaveBeenCalledWith('roomMap2:E9N3')
    // Nothing to fall back from: exactly one subscription.
    expect(socket.subscribe).toHaveBeenCalledOnce()
  })

  it('subscribeMap2() with a shard also subscribes the unprefixed channel a private server publishes on', () => {
    // A screeps-launcher server reports a shard via /api/version but serves only roomMap2:<room>
    // (@screeps/backend socket/map.js matches /^roomMap2:([a-zA-Z0-9_-]+)$/), as with room:<room>.
    const { store, socket } = makeStore()
    store.subscribeMap2('W7N7', 'shard')
    expect((socket.subscribe as ReturnType<typeof vi.fn>).mock.calls.map(c => c[0])).toEqual(['roomMap2:shard/W7N7', 'roomMap2:W7N7'])
  })

  it('processes data arriving on the unprefixed channel exactly like the prefixed one', () => {
    const { store, socket } = makeStore()
    const events: Array<{ room: string; shard: string | null; source: string }> = []
    store.on('room:map2update', e => events.push(e))
    store.subscribeMap2('W7N7', 'shard')
    const legacy = (socket.on as ReturnType<typeof vi.fn>).mock.calls.find(c => c[0] === 'roomMap2:W7N7')
    expect(legacy).toBeDefined()
    const data: RoomMap2Data = { w: [[1, 2]] }
    legacy![1](data)
    expect(events).toEqual([{ room: 'W7N7', shard: 'shard', data, source: 'live' }])
    expect(store.map2data('W7N7', 'shard')).toEqual(data)
  })

  it('subscribeMap2() returns a Subscription with dispose()', () => {
    const { store } = makeStore()
    const sub = store.subscribeMap2('W7N7', 'shard0')
    expect(typeof sub.dispose).toBe('function')
  })

  it('map2data() returns null before any data arrives', () => {
    const { store } = makeStore()
    expect(store.map2data('W7N7', 'shard0')).toBeNull()
  })

  it('emits room:map2update with source:live on new data', () => {
    const { store, socket } = makeStore()

    let handler!: (data: unknown) => void
    ;(socket.on as ReturnType<typeof vi.fn>).mockImplementation((_ch: string, cb: (data: unknown) => void) => {
      handler = cb
      return { dispose: vi.fn() }
    })

    const listener = vi.fn()
    store.on('room:map2update', listener)
    store.subscribeMap2('W7N7', 'shard0')

    const data: RoomMap2Data = { s: [[10, 20]], c: [[25, 25]] }
    handler(data)

    expect(listener).toHaveBeenCalledOnce()
    expect(listener).toHaveBeenCalledWith(expect.objectContaining({
      room: 'W7N7',
      shard: 'shard0',
      source: 'live',
      data,
    }))
  })

  it('diff detection: identical successive messages do NOT emit room:map2update again', () => {
    const { store, socket } = makeStore()

    let handler!: (data: unknown) => void
    ;(socket.on as ReturnType<typeof vi.fn>).mockImplementation((_ch: string, cb: (data: unknown) => void) => {
      handler = cb
      return { dispose: vi.fn() }
    })

    const listener = vi.fn()
    store.on('room:map2update', listener)
    store.subscribeMap2('W7N7', 'shard0')

    const data: RoomMap2Data = { s: [[10, 20]] }
    handler(data)
    handler({ s: [[10, 20]] }) // same data, different object reference

    expect(listener).toHaveBeenCalledOnce()
  })

  it('diff detection: changed data DOES emit room:map2update', () => {
    const { store, socket } = makeStore()

    let handler!: (data: unknown) => void
    ;(socket.on as ReturnType<typeof vi.fn>).mockImplementation((_ch: string, cb: (data: unknown) => void) => {
      handler = cb
      return { dispose: vi.fn() }
    })

    const listener = vi.fn()
    store.on('room:map2update', listener)
    store.subscribeMap2('W7N7', 'shard0')

    handler({ s: [[10, 20]] })
    handler({ s: [[10, 21]] }) // different y

    expect(listener).toHaveBeenCalledTimes(2)
  })

  it('diff detection: key ordering in payload does not matter', () => {
    const { store, socket } = makeStore()

    let handler!: (data: unknown) => void
    ;(socket.on as ReturnType<typeof vi.fn>).mockImplementation((_ch: string, cb: (data: unknown) => void) => {
      handler = cb
      return { dispose: vi.fn() }
    })

    const listener = vi.fn()
    store.on('room:map2update', listener)
    store.subscribeMap2('W7N7', 'shard0')

    handler({ s: [[10, 20]], c: [[25, 25]] })
    handler({ c: [[25, 25]], s: [[10, 20]] }) // same data, different key order

    expect(listener).toHaveBeenCalledOnce()
  })

  it('ref-counting: multiple subscribes to same room share one WS subscription', () => {
    const { store, socket } = makeStore()
    store.subscribeMap2('W7N7', 'shard0')
    store.subscribeMap2('W7N7', 'shard0')
    // MapStore opens one socket subscription per channel per key; the second call reuses them
    expect((socket.subscribe as ReturnType<typeof vi.fn>).mock.calls.map(c => c[0])).toEqual(['roomMap2:shard0/W7N7', 'roomMap2:W7N7'])
  })

  it('dispose() when last ref is released, closes the socket subscription', () => {
    const { store, socket } = makeStore()
    const socketDispose = vi.fn()
    ;(socket.subscribe as ReturnType<typeof vi.fn>).mockReturnValue({ dispose: socketDispose })
    ;(socket.on as ReturnType<typeof vi.fn>).mockReturnValue({ dispose: vi.fn() })

    const sub1 = store.subscribeMap2('W7N7', 'shard0')
    const sub2 = store.subscribeMap2('W7N7', 'shard0')

    sub1.dispose()
    expect(socketDispose).not.toHaveBeenCalled()  // refCount still 1

    sub2.dispose()
    expect(socketDispose).toHaveBeenCalledTimes(2)  // last ref gone: the prefixed and the unprefixed channel
  })

  it('map2data() returns data after first message arrives', () => {
    const { store, socket } = makeStore()

    let handler!: (data: unknown) => void
    ;(socket.on as ReturnType<typeof vi.fn>).mockImplementation((_ch: string, cb: (data: unknown) => void) => {
      handler = cb
      return { dispose: vi.fn() }
    })

    store.subscribeMap2('W7N7', 'shard0')
    const data: RoomMap2Data = { c: [[25, 25]] }
    handler(data)

    expect(store.map2data('W7N7', 'shard0')).toEqual(data)
  })

  it('data is retained in storage after dispose (for future cache warm-start)', () => {
    const { store, socket } = makeStore()

    let handler!: (data: unknown) => void
    ;(socket.on as ReturnType<typeof vi.fn>).mockImplementation((_ch: string, cb: (data: unknown) => void) => {
      handler = cb
      return { dispose: vi.fn() }
    })

    const sub = store.subscribeMap2('W7N7', 'shard0')
    handler({ c: [[25, 25]] })
    sub.dispose()

    expect(store.map2data('W7N7', 'shard0')).not.toBeNull()
  })
})

function makeStoreWithLimit(maxSubscriptions: number) {
  const socket = {
    subscribe: vi.fn().mockReturnValue({ dispose: vi.fn() }),
    on: vi.fn().mockReturnValue({ dispose: vi.fn() }),
  } as unknown as import('../../src/socket/SocketClient.js').SocketClient
  const storage = new Map2Storage({ adapter: null, namespace: 'test', maxEntries: 10000 })
  const store = new MapStore(socket, storage, { maxSubscriptions })
  return { store, socket, storage }
}

describe('MapStore — subscription limit + waitlist', () => {
  it('subscribeMap2() returns active subscription when under limit', () => {
    const { store } = makeStore()
    const sub = store.subscribeMap2('W7N7', 'shard0')
    expect(sub.status()).toBe('active')
  })

  it('subscribeMap2() returns pending subscription when at limit', () => {
    const { store } = makeStoreWithLimit(1)
    store.subscribeMap2('W1N1', 'shard0')  // fills the slot
    const sub = store.subscribeMap2('W7N7', 'shard0')
    expect(sub.status()).toBe('pending')
  })

  it('unsubscribing active promotes next waitlist entry (FIFO)', () => {
    const { store } = makeStoreWithLimit(1)
    const sub1 = store.subscribeMap2('W1N1', 'shard0')  // active
    const sub2 = store.subscribeMap2('W7N7', 'shard0')  // pending first
    const sub3 = store.subscribeMap2('W8N8', 'shard0')  // pending second

    expect(sub2.status()).toBe('pending')
    sub1.dispose()                   // frees slot → W7N7 promoted (FIFO)
    expect(sub2.status()).toBe('active')
    expect(sub3.status()).toBe('pending')  // W8N8 still waiting
  })

  it('unsubscribing a waitlist entry does NOT trigger promotion', () => {
    const { store } = makeStoreWithLimit(1)
    const sub1 = store.subscribeMap2('W1N1', 'shard0')  // active
    const sub2 = store.subscribeMap2('W7N7', 'shard0')  // pending
    const sub3 = store.subscribeMap2('W8N8', 'shard0')  // pending

    sub2.dispose()  // remove from waitlist — no slot freed, no promotion
    expect(sub1.status()).toBe('active')
    expect(sub3.status()).toBe('pending')
  })

  it('emits room:map2state active on new subscription under limit', () => {
    const { store } = makeStore()
    const events: string[] = []
    store.on('room:map2state', ({ status }) => events.push(status))
    store.subscribeMap2('W7N7', 'shard0')
    expect(events).toContain('active')
  })

  it('emits room:map2state pending when at limit', () => {
    const { store } = makeStoreWithLimit(1)
    store.subscribeMap2('W1N1', 'shard0')
    const events: Array<{ room: string; status: string }> = []
    store.on('room:map2state', e => events.push(e))
    store.subscribeMap2('W7N7', 'shard0')
    expect(events).toContainEqual(expect.objectContaining({ room: 'W7N7', status: 'pending' }))
  })

  it('emits room:map2state active when waitlist entry is promoted', () => {
    const { store } = makeStoreWithLimit(1)
    const sub1 = store.subscribeMap2('W1N1', 'shard0')
    store.subscribeMap2('W7N7', 'shard0')
    const events: Array<{ room: string; status: string }> = []
    store.on('room:map2state', e => events.push(e))
    sub1.dispose()
    expect(events).toContainEqual(expect.objectContaining({ room: 'W7N7', status: 'active' }))
  })

  it('onStatusChange fires when subscription transitions pending → active', () => {
    const { store } = makeStoreWithLimit(1)
    const sub1 = store.subscribeMap2('W1N1', 'shard0')
    const sub2 = store.subscribeMap2('W7N7', 'shard0')

    const history: string[] = []
    sub2.onStatusChange(s => history.push(s))

    sub1.dispose()
    expect(history).toEqual(['active'])
  })

  it('onStatusChange handler can be disposed independently', () => {
    const { store } = makeStoreWithLimit(1)
    const sub1 = store.subscribeMap2('W1N1', 'shard0')
    const sub2 = store.subscribeMap2('W7N7', 'shard0')

    const handler = vi.fn()
    const handlerSub = sub2.onStatusChange(handler)
    handlerSub.dispose()  // remove handler before promotion

    sub1.dispose()
    expect(handler).not.toHaveBeenCalled()
  })

  it('dispose() is idempotent — second call has no effect', () => {
    const { store } = makeStoreWithLimit(1)
    store.subscribeMap2('W1N1', 'shard0')
    const sub2 = store.subscribeMap2('W7N7', 'shard0')

    sub2.dispose()
    sub2.dispose()  // second call — must not throw or double-decrement

    // Sub1 still active (refCount unchanged)
    expect(store.subscribeMap2('W1N1', 'shard0').status()).toBe('active')
  })

  it('cachedData() returns current memory data synchronously', () => {
    const { store, storage } = makeStore()
    const data: RoomMap2Data = { s: [[10, 20]] }
    void storage.put('W7N7', 'shard0', data)
    const sub = store.subscribeMap2('W7N7', 'shard0')
    expect(sub.cachedData()).toEqual(data)
  })

  it('promotion opens a new WS socket subscription for the promoted room', () => {
    const { store, socket } = makeStoreWithLimit(1)
    const sub1 = store.subscribeMap2('W1N1', 'shard0')   // active, socket.subscribe #1
    store.subscribeMap2('W7N7', 'shard0')                // pending, no socket.subscribe

    expect(socket.subscribe).toHaveBeenCalledTimes(2)  // only W1N1 subscribed so far, prefixed and unprefixed

    sub1.dispose()  // promotes W7N7

    expect((socket.subscribe as ReturnType<typeof vi.fn>).mock.calls.slice(2).map(c => c[0])).toEqual(['roomMap2:shard0/W7N7', 'roomMap2:W7N7'])
  })

  it('live data arrives for promoted room after promotion', () => {
    let capturedHandler: ((data: unknown) => void) | null = null
    const socketMock = {
      subscribe: vi.fn().mockReturnValue({ dispose: vi.fn() }),
      on: vi.fn().mockImplementation((_ch: string, cb: (data: unknown) => void) => {
        capturedHandler = cb
        return { dispose: vi.fn() }
      }),
    } as unknown as import('../../src/socket/SocketClient.js').SocketClient

    const storage = new Map2Storage({ adapter: null, namespace: 'test', maxEntries: 10000 })
    const store = new MapStore(socketMock, storage, { maxSubscriptions: 1 })

    const sub1 = store.subscribeMap2('W1N1', 'shard0')
    store.subscribeMap2('W7N7', 'shard0')

    sub1.dispose()  // promotes W7N7, socket.on registered for W7N7

    const liveEvents: string[] = []
    store.on('room:map2update', ({ room, source }) => {
      if (source === 'live') liveEvents.push(room)
    })

    capturedHandler!({ s: [[10, 20]] })
    expect(liveEvents).toContain('W7N7')
  })
})

describe('MapStore — cache warm-start', () => {
  it('emits source:cache via microtask when memory has data at subscribe time', async () => {
    const { store, storage } = makeStore()

    const data: RoomMap2Data = { s: [[10, 20]] }
    // Populate memory synchronously (put() updates memory before its first await)
    void storage.put('W7N7', 'shard0', data)

    const listener = vi.fn()
    store.on('room:map2update', listener)
    store.subscribeMap2('W7N7', 'shard0')

    // No emission yet — warm-start is deferred to a microtask
    expect(listener).not.toHaveBeenCalled()

    await Promise.resolve()

    expect(listener).toHaveBeenCalledOnce()
    expect(listener).toHaveBeenCalledWith(expect.objectContaining({
      room: 'W7N7',
      shard: 'shard0',
      source: 'cache',
      data,
    }))
  })

  it('warm-start does NOT fire when there is no cached data', async () => {
    const { store } = makeStore()
    const listener = vi.fn()
    store.on('room:map2update', listener)
    store.subscribeMap2('W7N7', 'shard0')

    await Promise.resolve()

    expect(listener).not.toHaveBeenCalled()
  })

  it('warm-start does NOT fire if subscription is disposed before microtask runs', async () => {
    const { store, storage } = makeStore()
    void storage.put('W7N7', 'shard0', { s: [[10, 20]] })

    const listener = vi.fn()
    store.on('room:map2update', listener)
    const sub = store.subscribeMap2('W7N7', 'shard0')
    sub.dispose()  // dispose immediately, before microtask runs

    await Promise.resolve()

    expect(listener).not.toHaveBeenCalled()
  })

  it('warm-start source:cache is emitted even when room is on second subscribe (ref already held)', async () => {
    const { store, socket, storage } = makeStore()
    ;(socket.on as ReturnType<typeof vi.fn>).mockReturnValue({ dispose: vi.fn() })

    const data: RoomMap2Data = { c: [[25, 25]] }
    void storage.put('W7N7', 'shard0', data)

    const listener = vi.fn()
    store.on('room:map2update', listener)

    store.subscribeMap2('W7N7', 'shard0')  // first sub — warm-start scheduled
    await Promise.resolve()  // first warm-start fires
    listener.mockClear()

    store.subscribeMap2('W7N7', 'shard0')  // second sub — another warm-start scheduled
    await Promise.resolve()

    expect(listener).toHaveBeenCalledOnce()
    expect(listener).toHaveBeenCalledWith(expect.objectContaining({ source: 'cache' }))
  })
})

function makeSocketMockWithReconnect() {
  const listeners = new Map<string, (data: unknown) => void>()
  const socket = {
    subscribe: vi.fn().mockReturnValue({ dispose: vi.fn() }),
    on: vi.fn().mockImplementation((channel: string, cb: (data: unknown) => void) => {
      listeners.set(channel, cb)
      return { dispose: vi.fn() }
    }),
    trigger: (channel: string, data: unknown) => listeners.get(channel)?.(data),
  } as unknown as import('../../src/socket/SocketClient.js').SocketClient & { trigger: (ch: string, d: unknown) => void }
  return socket
}

describe('MapStore — reconnect handling', () => {
  it('re-emits room:map2state active for all active subs on connected', () => {
    const socket = makeSocketMockWithReconnect()
    const storage = new Map2Storage({ adapter: null, namespace: 'test', maxEntries: 10000 })
    const store = new MapStore(socket, storage)

    store.subscribeMap2('W1N1', 'shard0')
    store.subscribeMap2('W2N2', 'shard0')

    const events: Array<{ room: string; status: string }> = []
    store.on('room:map2state', e => events.push(e))

    // Simulate reconnect
    socket.trigger('connected', {})

    expect(events).toContainEqual(expect.objectContaining({ room: 'W1N1', status: 'active' }))
    expect(events).toContainEqual(expect.objectContaining({ room: 'W2N2', status: 'active' }))
  })

  it('re-emits room:map2state pending for all waitlist subs on connected', () => {
    const socket = makeSocketMockWithReconnect()
    const storage = new Map2Storage({ adapter: null, namespace: 'test', maxEntries: 10000 })
    const store = new MapStore(socket, storage, { maxSubscriptions: 1 })

    store.subscribeMap2('W1N1', 'shard0')  // active
    store.subscribeMap2('W7N7', 'shard0')  // pending

    const events: Array<{ room: string; status: string }> = []
    store.on('room:map2state', e => events.push(e))

    socket.trigger('connected', {})

    expect(events).toContainEqual(expect.objectContaining({ room: 'W1N1', status: 'active' }))
    expect(events).toContainEqual(expect.objectContaining({ room: 'W7N7', status: 'pending' }))
  })

  it('live data still flows for active subs after reconnect', () => {
    const socket = makeSocketMockWithReconnect()
    const storage = new Map2Storage({ adapter: null, namespace: 'test', maxEntries: 10000 })
    const store = new MapStore(socket, storage)

    store.subscribeMap2('W1N1', 'shard0')

    socket.trigger('connected', {})

    const liveEvents: string[] = []
    store.on('room:map2update', ({ room, source }) => { if (source === 'live') liveEvents.push(room) })

    socket.trigger('roomMap2:shard0/W1N1', { s: [[10, 20]] })
    expect(liveEvents).toContain('W1N1')
  })

  it('emits no events on connected when store has no subscriptions', () => {
    const socket = makeSocketMockWithReconnect()
    const storage = new Map2Storage({ adapter: null, namespace: 'test', maxEntries: 10000 })
    const store = new MapStore(socket, storage)

    const events: unknown[] = []
    store.on('room:map2state', e => events.push(e))

    socket.trigger('connected', {})
    expect(events).toHaveLength(0)
  })

  it('first message after reconnect always emits, even when payload is unchanged', () => {
    const socket = makeSocketMockWithReconnect()
    const storage = new Map2Storage({ adapter: null, namespace: 'test', maxEntries: 10000 })
    const store = new MapStore(socket, storage)

    store.subscribeMap2('W1N1', 'shard0')

    const liveEvents: unknown[] = []
    store.on('room:map2update', e => { if (e.source === 'live') liveEvents.push(e) })

    const payload = { s: [[10, 20]] as [number, number][] }
    socket.trigger('roomMap2:shard0/W1N1', payload)
    expect(liveEvents).toHaveLength(1)

    // Identical payload before reconnect is deduped
    socket.trigger('roomMap2:shard0/W1N1', { s: [[10, 20]] })
    expect(liveEvents).toHaveLength(1)

    // After reconnect the server resends initial state — same payload must emit again
    socket.trigger('connected', {})
    socket.trigger('roomMap2:shard0/W1N1', { s: [[10, 20]] })
    expect(liveEvents).toHaveLength(2)

    // Subsequent identical messages after that are deduped again
    socket.trigger('roomMap2:shard0/W1N1', { s: [[10, 20]] })
    expect(liveEvents).toHaveLength(2)
  })
})

describe('Map2Storage LRU eviction', () => {
  it('evicts least-recently-accessed entry when over maxEntries', () => {
    const storage = new Map2Storage({ adapter: null, namespace: 'test', maxEntries: 2 })
    const data: RoomMap2Data = {}

    storage.put('W1N1', null, data)
    storage.put('W2N2', null, data)
    // Touch W1N1 so W2N2 is oldest
    storage.getMemory('W1N1', null)
    // Adding W3N3 should evict W2N2
    storage.put('W3N3', null, data)

    expect(storage.getMemory('W1N1', null)).not.toBeNull()
    expect(storage.getMemory('W2N2', null)).toBeNull()
    expect(storage.getMemory('W3N3', null)).not.toBeNull()
  })
})

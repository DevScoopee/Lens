import { PrismaClient } from '@prisma/client'
import { Pool } from 'pg'
import { createHash, randomUUID } from 'node:crypto'
import {
  db_query_duration_seconds,
  price_points_duplicate_total,
  price_points_inserted_total,
  price_rejected_total,
} from './metrics'
import { config, type NetworkName } from './config'

// Prisma for schema management + simple queries
const globalForPrisma = globalThis as unknown as { prisma?: PrismaClient }
export const prisma = globalForPrisma.prisma ?? new PrismaClient({
  log: process.env.NODE_ENV === 'development' ? ['warn', 'error'] : ['error'],
})
if (process.env.NODE_ENV !== 'production') globalForPrisma.prisma = prisma

// Raw pg pool for time-series queries (VWAP, aggregates)
// ssl: true with rejectUnauthorized: false is required for Supabase pgbouncer
// (their pooler uses a self-signed cert in the chain — this is safe for Supabase specifically)
export const pgPool = new Pool({
  connectionString: config.db.url,
  ssl: config.db.url.includes('supabase.com') ? { rejectUnauthorized: false } : undefined,
})

// Instrument raw pg queries
const originalQuery = pgPool.query.bind(pgPool)
pgPool.query = (async (...args: any[]) => {
  const end = db_query_duration_seconds.startTimer()
  try {
    return await (originalQuery as any)(...args)
  } finally {
    end()
  }
}) as any

/**
 * `network` is required rather than defaulting to the active one. Ingesters are
 * started per network (see startSDEXIngester(network) and friends), so a single
 * process runs a testnet and a mainnet loop side by side. Tagging rows from the
 * global STELLAR_NETWORK wrote every mainnet price as testnet, silently blending
 * real prices with a test chain's in one table. A required argument is what
 * stops that reappearing the next time a venue is added.
 *
 * Idempotency (#237): a retried batch or two ingester instances overlapping
 * during a deploy must not store the same observation twice — duplicated
 * trades inflate volume and skew the volume-weighted VWAP. The unique index
 * price_point_identity on (network, source, event_id) makes that impossible at
 * the database level, and ON CONFLICT DO NOTHING turns the write into an
 * idempotent insert: the replay is absorbed, not errored.
 *
 * The index only catches rows that HAVE an event id — Postgres treats NULLs as
 * distinct in a unique index — so a null eventId would leave a source entirely
 * outside the guarantee. Rather than store nulls, writes to sources with no
 * natural event id get a key synthesized from the row's own content (see
 * synthesizeEventId). Venues with a natural id — SDEX and AMM trades carry the
 * Horizon trade id / TOID — pass it through unchanged, and snapshot venues
 * without one derive theirs from pool state (see venues/amm.ts and
 * venues/soroswap.ts).
 *
 * INSERT ... RETURNING id reports exactly which rows landed. Prisma's
 * createMany exposes only a count, which cannot tell callers which points were
 * duplicates — and publishing events for those made quiet pools look active.
 * The ids are generated here (rather than relying on a database default) so
 * each input row can be matched to its insertion result by index.
 */
export interface PricePointWrite {
  assetA: string
  assetB: string
  pairKey: string
  source: string
  poolId?: string
  price: number
  baseVolume: number
  counterVolume: number
  ledger: number
  timestamp: Date
  eventId?: string
}

export interface PricePointWriteResult {
  inserted: PricePointWrite[]
  duplicate: PricePointWrite[]
  quarantined: PricePointWrite[]
}

type ResolvedPricePoint = PricePointWrite & { eventId: string }

const PRICE_POINT_COLUMNS = 13

/** Prices outside the configured bounds are never offered to the database. */
export function isPlausiblePrice(price: number): boolean {
  return Number.isFinite(price)
    && price >= config.plausibility.minPrice
    && price <= config.plausibility.maxPrice
}

export async function upsertPricePoints(
  points: PricePointWrite[],
  network: NetworkName
): Promise<PricePointWriteResult> {
  const accepted: ResolvedPricePoint[] = []
  const quarantined: PricePointWrite[] = []
  for (const point of points) {
    if (!isPlausiblePrice(point.price)) {
      quarantined.push(point)
      continue
    }
    accepted.push({
      ...point,
      eventId: point.eventId || synthesizeEventId(network, point),
    })
  }

  for (const point of quarantined) {
    price_rejected_total.inc({
      pair: point.pairKey, network, venue: point.source, reason: 'out_of_bounds',
    })
  }
  if (accepted.length === 0) return { inserted: [], duplicate: [], quarantined }

  const ids = accepted.map(() => randomUUID())
  const values: unknown[] = []
  const tuples = accepted.map((point, i) => {
    const start = i * PRICE_POINT_COLUMNS + 1
    values.push(
      ids[i],
      network,
      point.assetA,
      point.assetB,
      point.pairKey,
      point.source,
      point.poolId ?? null,
      point.price,
      point.baseVolume,
      point.counterVolume,
      point.ledger,
      point.timestamp,
      point.eventId,
    )
    return `($${start}, $${start + 1}, $${start + 2}, $${start + 3}, $${start + 4}, $${start + 5}, $${start + 6}, $${start + 7}, $${start + 8}, $${start + 9}, $${start + 10}, $${start + 11}, $${start + 12})`
  })

  // One statement keeps the batch atomic: no partial write followed by a
  // second lookup that could observe a different database state.
  const result = await pgPool.query<{ id: string }>(
    `INSERT INTO price_points
       (id, network, asset_a, asset_b, pair_key, source, pool_id, price, base_volume, counter_volume, ledger, timestamp, event_id)
     VALUES ${tuples.join(', ')}
     ON CONFLICT (network, source, event_id) DO NOTHING
     RETURNING id`,
    values,
  )

  const insertedIds = new Set(result.rows.map(row => row.id))
  const inserted: PricePointWrite[] = []
  const duplicate: PricePointWrite[] = []
  accepted.forEach((point, i) => {
    if (insertedIds.has(ids[i])) inserted.push(point)
    else duplicate.push(point)
  })

  for (const point of inserted) {
    price_points_inserted_total.inc({ source: point.source, network })
  }
  for (const point of duplicate) {
    price_points_duplicate_total.inc({ source: point.source, network })
  }

  return { inserted, duplicate, quarantined }
}

/**
 * Deterministic identity for rows whose source has no natural event id
 * (#237): a SHA-256 over the row's own content — the network, source, pair,
 * pool, ledger, observation timestamp and amounts — never a clock read or a
 * random value. Re-submitting the same batch re-derives the same key, so the
 * unique index absorbs it.
 *
 * The timestamp used is the one the venue attached to the observation (a
 * trade's ledger close time, a snapshot's poll time), which is part of the
 * row's content, not "now". So two ingester instances that independently poll
 * the same venue at different milliseconds will produce different ids for a
 * snapshot whose timestamp they each stamp at read time — the per-venue keys
 * in venues/amm.ts and venues/soroswap.ts exist precisely to derive from pool
 * STATE (last_modified_ledger / reserves) instead, and sources that only ever
 * flow through here (Aquarius, oracles, future venues) fall back to this
 * content hash, which still makes a replay of the same derived batch
 * idempotent.
 */
export function synthesizeEventId(
  network: NetworkName,
  p: {
    source: string; pairKey: string; poolId?: string
    price: number; baseVolume: number; counterVolume: number
    ledger: number; timestamp: Date
  },
): string {
  const content = [
    network,
    p.source,
    p.pairKey,
    p.poolId ?? '',
    String(p.ledger),
    p.timestamp.toISOString(),
    String(p.price),
    String(p.baseVolume),
    String(p.counterVolume),
  ].join('|')
  return `${p.source}-synthetic-${createHash('sha256').update(content).digest('hex')}`
}

/**
 * The cursor and the ledger it reached, read in one round trip. Ingesters need
 * both: the cursor resumes pagination, and the ledger is the fallback when a
 * trade's TOID can't be parsed (see ingesters/toid.ts).
 */
export interface IndexerState {
  cursor: string | null
  ledger: number | null
}

export async function getIndexerState(id: string, network: NetworkName): Promise<IndexerState> {
  const state = await prisma.indexerState.findUnique({ where: { network_id: { network, id } } })
  return { cursor: state?.lastCursor ?? null, ledger: state?.lastLedger ?? null }
}

export async function setIndexerCursor(id: string, cursor: string, network: NetworkName, ledger?: number): Promise<void> {
  await prisma.indexerState.upsert({
    where: { network_id: { network, id } },
    create: { id, network, lastCursor: cursor, lastLedger: ledger, lastProcessedAt: new Date() },
    update: { lastCursor: cursor, lastLedger: ledger, lastProcessedAt: new Date() },
  })
}

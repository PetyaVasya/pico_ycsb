#!/usr/bin/env tarantool
--
-- Vinyl Workload A: Update-Heavy (modeled after YCSB-A / RocksDB overwrite)
--
-- Mix: 50% point reads, 50% updates on existing keys.
-- Distribution: Zipfian (hot keys accessed much more often).
-- Purpose: stress overlapping runs, fuse filter effectiveness for
-- point lookups, and the overlap-based compaction driver.
--
-- Dataset: ~20 GB (200M rows × ~100 bytes).
--
-- Usage:
--   tarantool vinyl_workload_a.lua [minutes]
--
-- The workload is resumable: restarting continues from existing data.
-- Default runtime is 10 minutes.
--

-------------------------------------------------------------------------------
-- Scale factor:  1 →  2M keys  (~200 MB)
--               10 → 20M keys  (~2 GB)
--              100 → 200M keys (~20 GB)
-------------------------------------------------------------------------------
local SCALE_FACTOR    = tonumber(arg and arg[2]) or 10

-- Default runtime: round(scale / (2 * lg10(scale))) minutes.
-- scale 1 → 1 min, scale 10 → 5 min, scale 100 → 25 min.
local RUNTIME_MINUTES = tonumber(arg and arg[1]) or
                        math.max(1, math.floor(SCALE_FACTOR /
                            math.max(1, 2 * math.log10(SCALE_FACTOR)) + 0.5))
local NUM_KEYS        = 2000000 * SCALE_FACTOR
local VALUE_SIZE      = 90        -- + 8 byte key + ~2 byte overhead ≈ 100 bytes/row
-- At scale 1 the working set is small enough that 8 Zipfian fibers
-- hit the same hot keys constantly; batch=2 keeps conflicts near zero.
local BATCH_SIZE      = tonumber(arg and arg[3]) or
                        math.max(2, math.floor(SCALE_FACTOR *
                            math.log10(SCALE_FACTOR) + 0.5))
local LOAD_BATCH      = 5000      -- statements per transaction during load
local NUM_FIBERS      = 8         -- concurrent workers
local ZIPF_THETA      = 0.99     -- skew: ~1% of keys get ~50% of traffic

-- RAM budget preserves a realistic RAM:DISK ratio.
-- At scale 1 (~200 MB data): ~2 MB RAM.
-- At scale 10 (~2 GB data):  ~20 MB RAM.
local RAM_BUDGET      = 2 * 1024 * 1024 * SCALE_FACTOR

local log   = require('log')
local fiber = require('fiber')
local clock = require('clock')

box.cfg{
    log       = 'picodata.log',
    log_level = 'info',
    checkpoint_count    = 2,
    checkpoint_interval = 600,
    vinyl_cache         = tonumber(os.getenv('VINYL_CACHE')) or 0,
    vinyl_memory        = RAM_BUDGET * 2,
}

-- Schema (idempotent).
if box.space.bench_a == nil then
    local s = box.schema.space.create('bench_a', {
        engine = 'vinyl',
        format = {
            { name = 'id',    type = 'unsigned' },
            { name = 'value', type = 'string'   },
        },
    })
    s:create_index('pk', { parts = { 'id' },  })
    log.info('bench_a: created space')
end

local space = box.space.bench_a

-------------------------------------------------------------------------------
-- Zipfian distribution (O(1) memory, YCSB-style)
--
-- Uses the Euler-Maclaurin approximation for the generalized
-- harmonic number and the YCSB inverse-CDF method.
-------------------------------------------------------------------------------

local zipf_n     = NUM_KEYS
local zipf_theta = ZIPF_THETA
local zipf_alpha = 1.0 / (1.0 - zipf_theta)

-- Approximate H(n, theta) using the integral + correction terms.
local function approx_zeta(n, theta)
    local ns = n ^ (1.0 - theta)
    return (ns - 1.0) / (1.0 - theta) +
           0.5 * (1.0 + n ^ (-theta)) +
           theta / 12.0 * (1.0 - n ^ (-theta - 1.0))
end

local zipf_zeta_n = approx_zeta(zipf_n, zipf_theta)
local zipf_zeta_2 = 1.0 + (0.5 ^ zipf_theta)
local zipf_eta    = (1.0 - (2.0 / zipf_n) ^ (1.0 - zipf_theta)) /
                    (1.0 - zipf_zeta_2 / zipf_zeta_n)

local function zipf_next()
    local u  = math.random()
    local uz = u * zipf_zeta_n
    if uz < 1.0 then
        return 1
    end
    if uz < zipf_zeta_2 then
        return 2
    end
    return 1 + math.floor(zipf_n *
               ((zipf_eta * u - zipf_eta + 1.0) ^ zipf_alpha))
end

-------------------------------------------------------------------------------
-- Random value generator
-------------------------------------------------------------------------------

-- Pre-allocate a single reusable string buffer.
local random_bytes = {}
for i = 1, VALUE_SIZE do random_bytes[i] = 0 end

local function random_value()
    for i = 1, VALUE_SIZE do
        random_bytes[i] = math.random(97, 122) -- a-z
    end
    return string.char(unpack(random_bytes))
end

-------------------------------------------------------------------------------
-- Initial load (if needed)
-------------------------------------------------------------------------------

local function load_data()
    local count = space:count()
    if count >= NUM_KEYS then
        log.info('bench_a: data already loaded (%d rows)', count)
        return
    end
    log.info('bench_a: loading data from key %d to %d ...', count + 1, NUM_KEYS)
    local batch = 0
    local t0 = clock.monotonic()
    box.begin()
    for i = count + 1, NUM_KEYS do
        space:replace({ i, random_value() })
        batch = batch + 1
        if batch >= LOAD_BATCH then
            box.commit()
            box.begin()
            batch = 0
            if i % math.max(100000, math.floor(NUM_KEYS / 20)) == 0 then
                local elapsed = clock.monotonic() - t0
                local rate = i / elapsed
                local eta = (NUM_KEYS - i) / rate
                log.info('bench_a: loaded %dM / %dM (%.0f rows/s, ETA %.0fs)',
                         i / 1e6, NUM_KEYS / 1e6, rate, eta)
            end
        end
    end
    if batch > 0 then
        box.commit()
    end
    local elapsed = clock.monotonic() - t0
    log.info('bench_a: load complete, %d rows in %.1fs', space:count(), elapsed)
    box.snapshot()
end

load_data()

-------------------------------------------------------------------------------
-- Workload fibers
-------------------------------------------------------------------------------

local stats = {
    reads  = 0,
    writes = 0,
    errors = 0,
}
local stop = false

local function worker(id)
    log.info('bench_a: worker %d started', id)
    local local_reads  = 0
    local local_writes = 0
    local local_errors = 0

    while not stop do
        local ok, err = pcall(function()
            box.begin()
            for _ = 1, BATCH_SIZE do
                local key = zipf_next()
                if math.random() < 0.5 then
                    space:get(key)
                    local_reads = local_reads + 1
                else
                    space:replace({ key, random_value() })
                    local_writes = local_writes + 1
                end
            end
            box.commit()
        end)
        if not ok then
            pcall(box.rollback)
            local_errors = local_errors + 1
            if local_errors <= 3 then
                log.warn('bench_a: worker %d error: %s', id, tostring(err))
            end
        end
        fiber.yield()
    end

    stats.reads  = stats.reads  + local_reads
    stats.writes = stats.writes + local_writes
    stats.errors = stats.errors + local_errors
    log.info('bench_a: worker %d done (reads=%d writes=%d errors=%d)',
             id, local_reads, local_writes, local_errors)
end

-------------------------------------------------------------------------------
-- Reporter fiber
-------------------------------------------------------------------------------

local function reporter()
    local prev_reads  = 0
    local prev_writes = 0
    while not stop do
        fiber.sleep(10)
        local r = stats.reads
        local w = stats.writes
        local dr = r - prev_reads
        local dw = w - prev_writes
        prev_reads  = r
        prev_writes = w
        log.info('bench_a: total reads=%d writes=%d (+%d/+%d per 10s) errors=%d',
                 r, w, dr, dw, stats.errors)
        local vs = box.stat.vinyl()
        if vs.index_cache then
            log.info('bench_a: index_cache hit=%d miss=%d evict=%d mem=%d',
                     vs.index_cache.hit, vs.index_cache.miss,
                     vs.index_cache.evict, vs.index_cache.mem_used)
        end
        log.info('bench_a: scheduler dump=%d tasks_completed=%d',
                 vs.scheduler.dump_count, vs.scheduler.tasks_completed)
        if vs.dict then
            log.info('bench_a: dict attempted=%d throttled=%d failed=%d rejected=%d accepted=%d active=%d bytes=%d',
                     vs.dict.train_attempted or 0, vs.dict.train_throttled or 0,
                     vs.dict.train_failed or 0, vs.dict.train_rejected or 0,
                     vs.dict.train_accepted or 0, vs.dict.dicts_active or 0,
                     vs.dict.dicts_bytes or 0)
        end
        local idx = space.index.pk
        if idx then
            local is = idx:stat()
            log.info('bench_a: pk ranges=%d runs=%d bloom hit=%d miss=%d',
                     is.range_count, is.run_count,
                     is.disk.iterator.bloom.hit,
                     is.disk.iterator.bloom.miss)
            log.info('bench_a: pk compaction count=%d in=%d out=%d',
                     is.disk.compaction.count,
                     is.disk.compaction.input.bytes or 0,
                     is.disk.compaction.output.bytes or 0)
        end
    end
end

-------------------------------------------------------------------------------
-- Main
-------------------------------------------------------------------------------

log.info('bench_a: starting %d fibers for %d minutes', NUM_FIBERS, RUNTIME_MINUTES)
math.randomseed(tonumber(clock.realtime64() % 2147483647))

local fibers = {}
for i = 1, NUM_FIBERS do
    fibers[i] = fiber.new(worker, i)
    fibers[i]:set_joinable(true)
    fibers[i]:name('bench_a_' .. i)
end

local rep = fiber.new(reporter)
rep:name('bench_a_rep')

fiber.sleep(RUNTIME_MINUTES * 60)
stop = true

for i = 1, NUM_FIBERS do
    fibers[i]:join()
end

log.info('bench_a: DONE total reads=%d writes=%d errors=%d',
         stats.reads, stats.writes, stats.errors)

local vs = box.stat.vinyl()
local is = space.index.pk:stat()
local total_ops = stats.reads + stats.writes
local elapsed_s = RUNTIME_MINUTES * 60
log.info('=== FINAL REPORT ===')
log.info('Total ops:       %d', total_ops)
log.info('RPS:             %.0f', total_ops / elapsed_s)
log.info('Errors:          %d', stats.errors)
log.info('Ranges:          %d', is.range_count)
log.info('Runs:            %d', is.run_count)
log.info('Disk bytes:      %d', is.disk.bytes or 0)
log.info('Dump count:      %d', vs.scheduler.dump_count or 0)
local cin  = is.disk.compaction.input.bytes or 0
local cout = is.disk.compaction.output.bytes or 0
local din  = is.disk.dump.input.bytes or 0
local dout = is.disk.dump.output.bytes or 0
log.info('Compaction I/O:  in=%d out=%d', cin, cout)
log.info('Dump I/O:        in=%d out=%d', din, dout)
local total_written = cout + dout
log.info('Total written:   %d', total_written)
if din > 0 then
    log.info('Write amplification: %.2f', total_written / din)
end
local live_data = math.max(1, space:count() * 100)
local space_amp = (is.disk.bytes or 1) / live_data
log.info('Space amplification: %.2f (disk_bytes=%d / live_data=%d)',
         space_amp, is.disk.bytes or 0, live_data)
log.info('Bloom hit:       %d', is.disk.iterator.bloom.hit)
log.info('Bloom miss:      %d', is.disk.iterator.bloom.miss)
local get_bytes  = is.get.bytes or 0
local read_bytes = is.disk.iterator.read.bytes or 0
log.info('Get bytes:       %d', get_bytes)
log.info('Disk read bytes: %d', read_bytes)
if get_bytes > 0 then
    log.info('Read amplification: %.2f (disk_read_bytes / get_bytes)',
             read_bytes / get_bytes)
end
log.info('=== END REPORT ===')

os.exit(0)

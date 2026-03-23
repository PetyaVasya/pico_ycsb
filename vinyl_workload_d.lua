#!/usr/bin/env tarantool
--
-- Vinyl Workload D: Append + FIFO read/delete
--
-- Mix: 50% sequential inserts at the end, 50% sequential get + delete
-- from the beginning. One worker fiber type; each statement picks the
-- path at random.
--
-- Dataset: pre-loaded to ~20 GB (200M rows), then a sliding window —
-- new keys at the tail, old keys removed at the head.
--
-- Usage:
--   tarantool workload/d.lua [minutes] [scale_factor]
--
-- Resumable: next_key = loaded + 1; head_key = min(pk) (empty → next_key).
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
local INITIAL_KEYS    = 2000000 * SCALE_FACTOR
local VALUE_SIZE      = 90        -- ~100 bytes per row with key
local BATCH_SIZE      = 200       -- statements per transaction
local LOAD_BATCH      = 5000      -- statements per transaction during load
local NUM_FIBERS      = 8         -- concurrent workers

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
if box.space.bench_d == nil then
    local s = box.schema.space.create('bench_d', {
        engine = 'vinyl',
        format = {
            { name = 'ts',    type = 'unsigned' },
            { name = 'value', type = 'string'   },
        },
    })
    s:create_index('pk', {
        parts = { 'ts' },
        tombstone_threshold = 0.8,
        -- stmt_delete_histogram_max_bins = 100,
        tombstone_compaction_ttl = 180,
        compaction_priority_refresh_interval = 1,
    })
    log.info('bench_d: created space')
end

local space = box.space.bench_d

-------------------------------------------------------------------------------
-- Random value generator
-------------------------------------------------------------------------------

local random_bytes = {}
for i = 1, VALUE_SIZE do random_bytes[i] = 0 end

local function random_value()
    for i = 1, VALUE_SIZE do
        random_bytes[i] = math.random(97, 122)
    end
    return string.char(unpack(random_bytes))
end

-------------------------------------------------------------------------------
-- Initial load (if needed)
-------------------------------------------------------------------------------

local function load_data()
    local count = space:count()
    if count >= INITIAL_KEYS then
        log.info('bench_d: data already loaded (%d rows)', count)
        return count
    end
    log.info('bench_d: loading data from key %d to %d ...', count + 1, INITIAL_KEYS)
    local batch = 0
    local t0 = clock.monotonic()
    box.begin()
    for i = count + 1, INITIAL_KEYS do
        space:replace({ i, random_value() })
        batch = batch + 1
        if batch >= LOAD_BATCH then
            box.commit()
            box.begin()
            batch = 0
            if i % math.max(100000, math.floor(INITIAL_KEYS / 20)) == 0 then
                local elapsed = clock.monotonic() - t0
                local rate = i / elapsed
                local eta = (INITIAL_KEYS - i) / rate
                log.info('bench_d: loaded %dM / %dM (%.0f rows/s, ETA %.0fs)',
                         i / 1e6, INITIAL_KEYS / 1e6, rate, eta)
            end
        end
    end
    box.commit()
    local elapsed = clock.monotonic() - t0
    log.info('bench_d: load complete, %d rows in %.1fs', space:count(), elapsed)
    box.snapshot()
    return INITIAL_KEYS
end

local loaded = load_data()

-------------------------------------------------------------------------------
-- Shared state
-------------------------------------------------------------------------------

local next_key = loaded + 1
local head_key
do
    local it = space.index.pk:min()
    head_key = it and it[1] or next_key
end

local stats = {
    inserts     = 0,
    reads       = 0,
    read_miss   = 0,
    deletes     = 0,
    errors      = 0,
}
local stop = false

-------------------------------------------------------------------------------
-- Workload fibers (same layout as workload A: one txn, BATCH_SIZE ops)
-------------------------------------------------------------------------------

local function worker(id)
    log.info('bench_d: worker %d started', id)
    local local_inserts     = 0
    local local_reads       = 0
    local local_read_miss   = 0
    local local_deletes     = 0
    local local_errors      = 0

    while not stop do
        local ok, err = pcall(function()
            box.begin()
            for _ = 1, BATCH_SIZE do
                if math.random() < 0.5 then
                    local base = next_key
                    next_key = next_key + 1
                    space:replace({ base, random_value() })
                    local_inserts = local_inserts + 1
                else
                    local h = head_key
                    local t = next_key
                    if h < t then
                        head_key = h + 1
                        local tuple = space:get(h)
                        if tuple == nil then
                            local_read_miss = local_read_miss + 1
                        else
                            local_reads = local_reads + 1
                        end
                        space:delete(h)
                        local_deletes = local_deletes + 1
                    end
                end
            end
            box.commit()
        end)
        if not ok then
            pcall(box.rollback)
            local_errors = local_errors + 1
            if local_errors <= 3 then
                log.warn('bench_d: worker %d error: %s', id, tostring(err))
            end
        end
        fiber.yield()
    end

    stats.inserts   = stats.inserts   + local_inserts
    stats.reads     = stats.reads     + local_reads
    stats.read_miss = stats.read_miss + local_read_miss
    stats.deletes   = stats.deletes   + local_deletes
    stats.errors    = stats.errors    + local_errors
    log.info('bench_d: worker %d done (inserts=%d reads=%d miss=%d deletes=%d errors=%d)',
             id, local_inserts, local_reads, local_read_miss, local_deletes, local_errors)
end

-------------------------------------------------------------------------------
-- Reporter fiber
-------------------------------------------------------------------------------

local function reporter()
    local prev_inserts = 0
    local prev_deletes = 0
    while not stop do
        fiber.sleep(10)
        local ins = stats.inserts
        local del = stats.deletes
        local dins = ins - prev_inserts
        local ddel = del - prev_deletes
        prev_inserts = ins
        prev_deletes = del
        log.info('bench_d: inserts=%d deletes=%d reads=%d (+%d/+%d per 10s) '
                 .. 'read_miss=%d errors=%d head=%d next_key=%d',
                 ins, del, stats.reads, dins, ddel,
                 stats.read_miss, stats.errors, head_key, next_key)
        local vs = box.stat.vinyl()
        if vs.index_cache then
            log.info('bench_d: index_cache hit=%d miss=%d evict=%d mem=%d',
                     vs.index_cache.hit, vs.index_cache.miss,
                     vs.index_cache.evict, vs.index_cache.mem_used)
        end
        log.info('bench_d: scheduler dump=%d compaction_input=%d compaction_output=%d',
                 vs.scheduler.dump_count or 0,
                 vs.scheduler.compaction_input or 0,
                 vs.scheduler.compaction_output or 0)
        if vs.dict then
            log.info('bench_d: dict attempted=%d throttled=%d failed=%d rejected=%d accepted=%d active=%d bytes=%d',
                     vs.dict.train_attempted or 0, vs.dict.train_throttled or 0,
                     vs.dict.train_failed or 0, vs.dict.train_rejected or 0,
                     vs.dict.train_accepted or 0, vs.dict.dicts_active or 0,
                     vs.dict.dicts_bytes or 0)
        end
        local idx = space.index.pk
        if idx then
            local is = idx:stat()
            log.info('bench_d: pk ranges=%d runs=%d run_avg=%d',
                     is.range_count, is.run_count, is.run_avg)
            log.info('bench_d: pk compaction count=%d in=%d out=%d',
                     is.disk.compaction.count,
                     is.disk.compaction.input.bytes or 0,
                     is.disk.compaction.output.bytes or 0)
        end
    end
end

-------------------------------------------------------------------------------
-- Main
-------------------------------------------------------------------------------

log.info('bench_d: starting %d fibers for %d minutes', NUM_FIBERS, RUNTIME_MINUTES)
math.randomseed(tonumber(clock.realtime64() % 2147483647))

local fibers = {}
for i = 1, NUM_FIBERS do
    fibers[i] = fiber.new(worker, i)
    fibers[i]:set_joinable(true)
    fibers[i]:name('bench_d_' .. i)
end

local rep = fiber.new(reporter)
rep:name('bench_d_rep')

fiber.sleep(RUNTIME_MINUTES * 60)
stop = true

for i = 1, NUM_FIBERS do
    fibers[i]:join()
end

log.info('bench_d: DONE inserts=%d deletes=%d reads=%d read_miss=%d errors=%d',
         stats.inserts, stats.deletes, stats.reads, stats.read_miss, stats.errors)

local vs = box.stat.vinyl()
local is = space.index.pk:stat()
local total_ops = stats.inserts + stats.deletes
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
log.info('Compaction count: %d', is.disk.compaction.count or 0)
log.info('Dump I/O:        in=%d out=%d', din, dout)
local total_written = cout + dout
log.info('Total written:   %d', total_written)
if din > 0 then
    log.info('Write amplification: %.2f', total_written / din)
end
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
log.info('Index memory:    %d', space.index.pk:bsize() or 0)
log.info('Regulator write rate: %.2f', vs.regulator.write_rate)
log.info('Regulator dump bandwidth: %.2f', vs.regulator.dump_bandwidth)
log.info('Regulator dump watermark: %.2f', vs.regulator.dump_watermark)
log.info('Regulator rate limit: %.2f', vs.regulator.rate_limit)
log.info('=== END REPORT ===')

fiber.sleep(30)

log.info('bench_d: DONE inserts=%d deletes=%d reads=%d read_miss=%d errors=%d',
         stats.inserts, stats.deletes, stats.reads, stats.read_miss, stats.errors)

vs = box.stat.vinyl()
is = space.index.pk:stat()
total_ops = stats.inserts + stats.deletes
elapsed_s = RUNTIME_MINUTES * 60
log.info('=== FINAL REPORT ===')
log.info('Total ops:       %d', total_ops)
log.info('RPS:             %.0f', total_ops / elapsed_s)
log.info('Errors:          %d', stats.errors)
log.info('Ranges:          %d', is.range_count)
log.info('Runs:            %d', is.run_count)
log.info('Disk bytes:      %d', is.disk.bytes or 0)
log.info('Dump count:      %d', vs.scheduler.dump_count or 0)
cin  = is.disk.compaction.input.bytes or 0
cout = is.disk.compaction.output.bytes or 0
din  = is.disk.dump.input.bytes or 0
dout = is.disk.dump.output.bytes or 0
log.info('Compaction I/O:  in=%d out=%d', cin, cout)
log.info('Compaction count: %d', is.disk.compaction.count or 0)
log.info('Dump I/O:        in=%d out=%d', din, dout)
total_written = cout + dout
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
get_bytes  = is.get.bytes or 0
read_bytes = is.disk.iterator.read.bytes or 0
log.info('Get bytes:       %d', get_bytes)
log.info('Disk read bytes: %d', read_bytes)
if get_bytes > 0 then
    log.info('Read amplification: %.2f (disk_read_bytes / get_bytes)',
             read_bytes / get_bytes)
end
log.info('Index memory:    %d', space.index.pk:bsize() or 0)
log.info('Regulator write rate: %.2f', vs.regulator.write_rate)
log.info('Regulator dump bandwidth: %.2f', vs.regulator.dump_bandwidth)
log.info('Regulator dump watermark: %.2f', vs.regulator.dump_watermark)
log.info('Regulator rate limit: %.2f', vs.regulator.rate_limit)
log.info('=== END REPORT ===')

os.exit(0)

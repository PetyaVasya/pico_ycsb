#!/usr/bin/env tarantool
--
-- Vinyl Workload C: Delete Storm + Range Scans
-- (modeled after RocksDB deleterandom + readseq)
--
-- Phase 1 (load): insert N keys uniformly.
-- Phase 2 (steady): concurrently:
--   - Delete random keys (~30% of ops).
--   - Insert new keys to replace deleted ones (~20% of ops).
--   - Range scans over random intervals (~30% of ops).
--   - Point reads (~20% of ops).
--
-- Dataset: ~20 GB (200M rows × ~100 bytes).
--
-- Purpose: exercises tombstone accumulation, read amplification
-- detection, and the read-amp compaction driver.  Range scans
-- through tombstone-heavy regions decompress pages only to
-- discard them, which the read-amp tracking should detect and
-- trigger compaction.
--
-- Usage:
--   tarantool vinyl_workload_c.lua [minutes]
--
-- Resumable: checks existing row count before loading.
-- Default runtime is 10 minutes.
--

-------------------------------------------------------------------------------
-- Scale factor:  1 →  2M keys  (~200 MB)
--               10 → 20M keys  (~2 GB)
--              100 → 200M keys (~20 GB)
-------------------------------------------------------------------------------
local SCALE_FACTOR    = tonumber(arg and arg[2]) or 10
local NUM_KEYS        = 2000000 * SCALE_FACTOR
local VALUE_SIZE      = 80        -- ~100 bytes per row
local BATCH_SIZE      = 50        -- operations per transaction
local LOAD_BATCH      = 5000      -- statements per transaction during load
local SCAN_LENGTH     = 200       -- tuples to scan per range query
local NUM_FIBERS      = 8         -- concurrent workers

-- Default runtime: round(scale / (2 * lg10(scale))) minutes.
-- scale 1 → 1 min, scale 10 → 5 min, scale 100 → 25 min.
local RUNTIME_MINUTES = tonumber(arg and arg[1]) or
                        math.max(1, math.floor(SCALE_FACTOR /
                            math.max(1, 2 * math.log10(SCALE_FACTOR)) + 0.5))

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
    vinyl_memory        = RAM_BUDGET * 2,  -- ~40 MB
}

-- Schema (idempotent).
if box.space.bench_c == nil then
    local s = box.schema.space.create('bench_c', {
        engine = 'vinyl',
        format = {
            { name = 'id',    type = 'unsigned' },
            { name = 'value', type = 'string'   },
        },
    })
    s:create_index('pk', {
        parts = { 'id' },
        tombstone_range_threshold = 0.3,
        stmt_histogram_max_bins = 128,
    })
    log.info('bench_c: created space')
end

local space = box.space.bench_c

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
    if count >= NUM_KEYS then
        log.info('bench_c: data already loaded (%d rows)', count)
        return
    end
    log.info('bench_c: loading data from key %d to %d ...', count + 1, NUM_KEYS)
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
                log.info('bench_c: loaded %dM / %dM (%.0f rows/s, ETA %.0fs)',
                         i / 1e6, NUM_KEYS / 1e6, rate, eta)
            end
        end
    end
    box.commit()
    local elapsed = clock.monotonic() - t0
    log.info('bench_c: load complete, %d rows in %.1fs', space:count(), elapsed)
    box.snapshot()
end

load_data()

-------------------------------------------------------------------------------
-- Shared state
-------------------------------------------------------------------------------

local next_new_key = NUM_KEYS + 1

local stats = {
    deletes   = 0,
    inserts   = 0,
    reads     = 0,
    scans     = 0,
    scan_rows = 0,
    errors    = 0,
}
local stop = false

-------------------------------------------------------------------------------
-- Worker fibers
-------------------------------------------------------------------------------

local function worker(id)
    log.info('bench_c: worker %d started', id)
    local local_deletes   = 0
    local local_inserts   = 0
    local local_reads     = 0
    local local_scans     = 0
    local local_scan_rows = 0
    local local_errors    = 0

    while not stop do
        local ok, err = pcall(function()
            local op = math.random()

            if op < 0.30 then
                --
                -- Delete: pick random keys and delete them.
                --
                box.begin()
                for _ = 1, BATCH_SIZE do
                    local key = math.random(1, next_new_key - 1)
                    space:delete(key)
                    local_deletes = local_deletes + 1
                end
                box.commit()

            elseif op < 0.50 then
                --
                -- Insert: add new keys to maintain data volume.
                --
                local base = next_new_key
                next_new_key = next_new_key + BATCH_SIZE
                box.begin()
                for i = 0, BATCH_SIZE - 1 do
                    space:replace({ base + i, random_value() })
                    local_inserts = local_inserts + 1
                end
                box.commit()

            elseif op < 0.80 then
                --
                -- Range scan: pick a random start and scan forward.
                -- This is where tombstone read-amp is most visible.
                --
                local start_key = math.random(1,
                    math.max(1, next_new_key - SCAN_LENGTH))
                local count = 0
                for _, tuple in space:pairs(start_key, { iterator = 'GE' }) do
                    count = count + 1
                    if count >= SCAN_LENGTH then
                        break
                    end
                end
                local_scans = local_scans + 1
                local_scan_rows = local_scan_rows + count

            else
                --
                -- Point read: random key lookup.
                --
                for _ = 1, BATCH_SIZE do
                    local key = math.random(1, next_new_key - 1)
                    space:get(key)
                    local_reads = local_reads + 1
                end
            end
        end)
        if not ok then
            pcall(box.rollback)
            local_errors = local_errors + 1
        end
        fiber.yield()
    end

    stats.deletes   = stats.deletes   + local_deletes
    stats.inserts   = stats.inserts   + local_inserts
    stats.reads     = stats.reads     + local_reads
    stats.scans     = stats.scans     + local_scans
    stats.scan_rows = stats.scan_rows + local_scan_rows
    stats.errors    = stats.errors    + local_errors
    log.info('bench_c: worker %d done (del=%d ins=%d reads=%d scans=%d rows=%d err=%d)',
             id, local_deletes, local_inserts, local_reads,
             local_scans, local_scan_rows, local_errors)
end

-------------------------------------------------------------------------------
-- Final report helpers
-------------------------------------------------------------------------------

local function log_slice_estimate(name, est, trigger_field)
    if est == nil or (est.sample_count or 0) == 0 then
        log.info('%s estimate: no samples', name)
        return
    end
    log.info('%s abs_error: %.1f tuples', name, est.abs_error)
    if est.rel_error ~= nil then
        log.info('%s rel_error: %.2f%%', name, est.rel_error)
    end
    log.info('%s q_error:   %.4f', name, est.q_error)
    log.info('%s samples:   %d', name, est.sample_count)
    if trigger_field ~= nil then
        log.info('%s %s: %d', name, trigger_field, est[trigger_field] or 0)
        log.info('%s compaction_prefix_trigger: %d', name,
                 est.compaction_prefix_trigger or 0)
        log.info('%s compaction_fpr: %d', name, est.compaction_fpr or 0)
        log.info('%s compaction_fnr: %d', name, est.compaction_fnr or 0)
    end
end

local function log_histogram_estimate(is)
    log_slice_estimate('Histogram', is.histogram_estimate,
                       'compaction_hist_trigger')
    log_slice_estimate('Ratio', is.ratio_estimate,
                       'compaction_ratio_trigger')
end

-------------------------------------------------------------------------------
-- Reporter fiber
-------------------------------------------------------------------------------

local function reporter()
    while not stop do
        fiber.sleep(10)
        local ok, err = pcall(function()
            local vs = box.stat.vinyl()
            local idx = space.index.pk
            if idx then
                local is = idx:stat()
                log.info('bench_c: ranges=%d runs=%d disk=%dMB '
                         .. 'comp(in=%dMB out=%dMB) '
                         .. 'dump(count=%d out=%dMB) '
                         .. 'bloom(hit=%d miss=%d)',
                         is.range_count, is.run_count,
                         (is.disk.bytes or 0) / 1048576,
                         (is.disk.compaction.input.bytes or 0) / 1048576,
                         (is.disk.compaction.output.bytes or 0) / 1048576,
                         vs.scheduler.dump_count or 0,
                         (is.disk.dump.output.bytes or 0) / 1048576,
                         is.disk.iterator.bloom.hit,
                         is.disk.iterator.bloom.miss)
            end
            if vs.dict then
                log.info('bench_c: dict attempted=%d throttled=%d failed=%d rejected=%d accepted=%d active=%d bytes=%d',
                         vs.dict.train_attempted or 0, vs.dict.train_throttled or 0,
                         vs.dict.train_failed or 0, vs.dict.train_rejected or 0,
                         vs.dict.train_accepted or 0, vs.dict.dicts_active or 0,
                         vs.dict.dicts_bytes or 0)
            end
            if vs.index_cache then
                log.info('bench_c: index_cache hit=%d miss=%d mem=%dMB',
                         vs.index_cache.hit, vs.index_cache.miss,
                         (vs.index_cache.mem_used or 0) / 1048576)
            end
        end)
        if not ok then
            log.warn('bench_c: reporter error: %s', tostring(err))
        end
    end
end

-------------------------------------------------------------------------------
-- Main
-------------------------------------------------------------------------------

log.info('bench_c: starting %d fibers for %d minutes', NUM_FIBERS, RUNTIME_MINUTES)
math.randomseed(tonumber(clock.realtime64() % 2147483647))

local fibers = {}
for i = 1, NUM_FIBERS do
    fibers[i] = fiber.new(worker, i)
    fibers[i]:set_joinable(true)
    fibers[i]:name('bench_c_' .. i)
end

local rep = fiber.new(reporter)
rep:name('bench_c_rep')

fiber.sleep(RUNTIME_MINUTES * 60)
stop = true

for i = 1, NUM_FIBERS do
    fibers[i]:join()
end

log.info('bench_c: DONE del=%d ins=%d reads=%d scans=%d scan_rows=%d errors=%d',
         stats.deletes, stats.inserts, stats.reads,
         stats.scans, stats.scan_rows, stats.errors)

local vs = box.stat.vinyl()
local is = space.index.pk:stat()
local total_ops = stats.deletes + stats.inserts + stats.reads + stats.scans
local elapsed_s = RUNTIME_MINUTES * 60
log.info('=== FINAL REPORT ===')
log.info('Total ops:       %d', total_ops)
log.info('RPS:             %.0f', total_ops / elapsed_s)
log.info('Errors:          %d', stats.errors)
log.info('Ranges:          %d', is.range_count)
log.info('Runs:            %d', is.run_count)
log_histogram_estimate(is)
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

log.info('bench_c: DONE del=%d ins=%d reads=%d scans=%d scan_rows=%d errors=%d',
         stats.deletes, stats.inserts, stats.reads,
         stats.scans, stats.scan_rows, stats.errors)

vs = box.stat.vinyl()
is = space.index.pk:stat()
total_ops = stats.deletes + stats.inserts + stats.reads + stats.scans
elapsed_s = RUNTIME_MINUTES * 60
log.info('=== FINAL REPORT ===')
log.info('Total ops:       %d', total_ops)
log.info('RPS:             %.0f', total_ops / elapsed_s)
log.info('Errors:          %d', stats.errors)
log.info('Ranges:          %d', is.range_count)
log.info('Runs:            %d', is.run_count)
log_histogram_estimate(is)
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
local live_data = math.max(1, space:count() * 90)
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
#!/usr/bin/env tarantool
--
-- Vinyl Workload B: Time-Series Append
-- (modeled after YCSB-D / RocksDB fillseq + readrandom)
--
-- Steady phase: each iteration with probability 0.5 appends a batch at the
-- high end, else performs BATCH_SIZE point lookups (80% latest 1%, 20% uniform).
--
-- Dataset: pre-loaded to ~20 GB (200M rows), then keeps growing.
--
-- Purpose: exercises non-overlapping run detection, range splits
-- without compaction, and the MinHash sketch overlap estimation.
-- With shape-based compaction, many unnecessary compactions happen.
-- With sketch-based compaction, disjoint runs are left alone.
--
-- Usage:
--   tarantool workload/b.lua [minutes] [scale_factor]
--
-- Resumable: the next key to insert is derived from space:len().
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
local VALUE_SIZE      = 80        -- ~100 bytes per row with key
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
if box.space.bench_b == nil then
    local s = box.schema.space.create('bench_b', {
        engine = 'vinyl',
        format = {
            { name = 'ts',    type = 'unsigned' },
            { name = 'value', type = 'string'   },
        },
    })
    s:create_index('pk', {
        parts = { 'ts' },
        tombstone_range_threshold = 0.3,
        stmt_histogram_max_bins = 128,
    })
    log.info('bench_b: created space')
end

local space = box.space.bench_b

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
        log.info('bench_b: data already loaded (%d rows)', count)
        return count
    end
    log.info('bench_b: loading data from key %d to %d ...', count + 1, INITIAL_KEYS)
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
                log.info('bench_b: loaded %dM / %dM (%.0f rows/s, ETA %.0fs)',
                         i / 1e6, INITIAL_KEYS / 1e6, rate, eta)
            end
        end
    end
    -- if batch > 0 then
    box.commit()
    -- end
    local elapsed = clock.monotonic() - t0
    log.info('bench_b: load complete, %d rows in %.1fs', space:count(), elapsed)
    box.snapshot()
    return INITIAL_KEYS
end

local loaded = load_data()

-------------------------------------------------------------------------------
-- Shared state
-------------------------------------------------------------------------------

local next_key = loaded + 1

local stats = {
    inserts     = 0,
    reads       = 0,
    read_miss   = 0,
    errors      = 0,
}
local stop = false

-------------------------------------------------------------------------------
-- Worker fibers: 50% sequential append, 50% latest-biased point lookups
-------------------------------------------------------------------------------

local function worker(id)
    log.info('bench_b: worker %d started', id)
    local local_inserts   = 0
    local local_reads     = 0
    local local_read_miss = 0
    local local_errors    = 0

    while not stop do
        local ok, err = pcall(function()
            if math.random() < 0.5 then
                local base = next_key
                next_key = next_key + BATCH_SIZE
                box.begin()
                for i = 0, BATCH_SIZE - 1 do
                    space:replace({ base + i, random_value() })
                end
                box.commit()
                local_inserts = local_inserts + BATCH_SIZE
            else
                local max_key = next_key - 1
                if max_key < 1 then
                    return
                end
                for _ = 1, BATCH_SIZE do
                    local key
                    if math.random() < 0.8 then
                        -- 80%: read from the latest 1% of data.
                        local recent_start = math.max(1,
                            math.floor(max_key * 0.99))
                        key = math.random(recent_start, max_key)
                    else
                        -- 20%: uniform random across all data.
                        key = math.random(1, max_key)
                    end
                    local tuple = space:get(key)
                    local_reads = local_reads + 1
                    if tuple == nil then
                        local_read_miss = local_read_miss + 1
                    end
                end
            end
        end)
        if not ok then
            pcall(box.rollback)
            local_errors = local_errors + 1
        end
        fiber.yield()
    end

    stats.inserts   = stats.inserts   + local_inserts
    stats.reads     = stats.reads     + local_reads
    stats.read_miss = stats.read_miss + local_read_miss
    stats.errors    = stats.errors    + local_errors
    log.info('bench_b: worker %d done (inserts=%d reads=%d miss=%d errors=%d)',
             id, local_inserts, local_reads, local_read_miss, local_errors)
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
    local prev_inserts = 0
    local prev_reads   = 0
    while not stop do
        fiber.sleep(10)
        local ins = stats.inserts
        local rds = stats.reads
        local dins = ins - prev_inserts
        local drds = rds - prev_reads
        prev_inserts = ins
        prev_reads   = rds
        log.info('bench_b: inserts=%d reads=%d (+%d/+%d per 10s) '
                 .. 'read_miss=%d errors=%d next_key=%d',
                 ins, rds, dins, drds,
                 stats.read_miss, stats.errors, next_key)
        local vs = box.stat.vinyl()
        if vs.index_cache then
            log.info('bench_b: index_cache hit=%d miss=%d evict=%d mem=%d',
                     vs.index_cache.hit, vs.index_cache.miss,
                     vs.index_cache.evict, vs.index_cache.mem_used)
        end
        log.info('bench_b: scheduler dump=%d compaction_input=%d compaction_output=%d',
                 vs.scheduler.dump_count or 0,
                 vs.scheduler.compaction_input or 0,
                 vs.scheduler.compaction_output or 0)
        if vs.dict then
            log.info('bench_b: dict attempted=%d throttled=%d failed=%d rejected=%d accepted=%d active=%d bytes=%d',
                     vs.dict.train_attempted or 0, vs.dict.train_throttled or 0,
                     vs.dict.train_failed or 0, vs.dict.train_rejected or 0,
                     vs.dict.train_accepted or 0, vs.dict.dicts_active or 0,
                     vs.dict.dicts_bytes or 0)
        end
        local idx = space.index.pk
        if idx then
            local is = idx:stat()
            log.info('bench_b: pk ranges=%d runs=%d run_avg=%d',
                     is.range_count, is.run_count, is.run_avg)
            log.info('bench_b: pk compaction count=%d in=%d out=%d',
                     is.disk.compaction.count,
                     is.disk.compaction.input.bytes or 0,
                     is.disk.compaction.output.bytes or 0)
        end
    end
end

-------------------------------------------------------------------------------
-- Main
-------------------------------------------------------------------------------

log.info('bench_b: starting %d fibers for %d minutes', NUM_FIBERS, RUNTIME_MINUTES)
math.randomseed(tonumber(clock.realtime64() % 2147483647))

local fibers = {}
for i = 1, NUM_FIBERS do
    fibers[i] = fiber.new(worker, i)
    fibers[i]:set_joinable(true)
    fibers[i]:name('bench_b_' .. i)
end

local rep = fiber.new(reporter)
rep:name('bench_b_rep')

fiber.sleep(RUNTIME_MINUTES * 60)
stop = true

for i = 1, NUM_FIBERS do
    fibers[i]:join()
end

log.info('bench_b: DONE inserts=%d reads=%d read_miss=%d errors=%d',
         stats.inserts, stats.reads, stats.read_miss, stats.errors)

local vs = box.stat.vinyl()
local is = space.index.pk:stat()
local total_ops = stats.inserts + stats.reads
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

log.info('bench_b: DONE inserts=%d reads=%d read_miss=%d errors=%d',
         stats.inserts, stats.reads, stats.read_miss, stats.errors)

vs = box.stat.vinyl()
is = space.index.pk:stat()
total_ops = stats.inserts + stats.reads
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
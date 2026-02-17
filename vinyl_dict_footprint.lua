#!/usr/bin/env tarantool
--
-- Dictionary training benchmark: measure the impact of training
-- on raw tuple data vs xrow-encoded data.
--
-- Usage: tarantool vinyl_dict_footprint.lua
--
-- Reports dict_gain, dict_rate, no_dict_rate, compression CPU,
-- disk footprint, and compaction I/O after a fixed workload.
--

local log = require('log')
local fiber = require('fiber')
local fio = require('fio')

-- Print to both log and stdout for reliable capture.
local function report(fmt, ...)
    local msg = string.format(fmt, ...)
    log.info('%s', msg)
    print(msg)
end

local NUM_KEYS      = 20000
local ROWS_PER_DUMP = 2000
local NUM_DUMPS     = 10
local PAGE_SIZE     = 4096

box.cfg{
    vinyl_memory   = 64 * 1024 * 1024,
    vinyl_cache    = 0,
    checkpoint_interval = 0,
    checkpoint_count = 1,
    log_level = 4,
    log = 'picodata.log',
}

math.randomseed(1)

local chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_"
local chars_len = #chars

local services = {"auth", "billing", "search", "profile", "ads",
                  "gateway", "storage", "feed"}
local regions = {"eu-west", "us-east", "us-west", "ap-south", "ru-central"}
local levels = {"DEBUG", "INFO", "WARN", "ERROR"}
local methods = {"GET", "POST", "PUT", "DELETE"}
local paths = {"/v1/login", "/v1/pay", "/v1/items", "/v1/profile",
               "/v1/feed", "/v1/search"}

local function rand_hex(n)
    local t = {}
    for j = 1, n do
        t[j] = string.format("%x", math.random(0, 15))
    end
    return table.concat(t)
end

local function rand_token(len)
    local t = {}
    for j = 1, len do
        local idx = math.random(1, chars_len)
        t[j] = chars:sub(idx, idx)
    end
    return table.concat(t)
end

local function pick(a)
    return a[math.random(1, #a)]
end

local function make_value(i)
    return string.format(
        '{"ts":"2026-02-10T18:%02d:%02dZ","service":"%s","region":"%s","level":"%s",' ..
        '"http":{"method":"%s","path":"%s","status":%d},"trace":{"trace_id":"%s","span_id":"%s"},' ..
        '"user":{"id":"%s","session":"%s"},"tags":["tarantool","vinyl","xlog","dict","compress"],' ..
        '"message":"request completed","payload":"%s"}',
        (i % 60), (i * 7) % 60,
        pick(services), pick(regions), pick(levels),
        pick(methods), pick(paths), 200 + (i % 5),
        rand_hex(20), rand_hex(10),
        rand_token(16), rand_token(24),
        rand_token(160 + (i % 20))
    )
end

local function wait_dump(index, count)
    while index:stat().disk.dump.count < count do
        fiber.sleep(0.01)
    end
end

local function wait_compaction(index, count)
    while index:stat().disk.compaction.count < count do
        fiber.sleep(0.01)
    end
end

-- Create space with dict compression enabled.
local s = box.schema.space.create('bench', {engine = 'vinyl'})
local pk = s:create_index('pk', {
    parts = {1, 'unsigned'},
    ,
    page_size = PAGE_SIZE,
    run_count_per_level = 2,
    run_size_ratio = 4,
})

-- Phase 1: dump data to train and establish a dictionary.
report('=== PHASE 1: dumping %d rows in %d dumps ===',
         NUM_KEYS, NUM_DUMPS)
local t0 = fiber.clock()
for dump = 1, NUM_DUMPS do
    local base = (dump - 1) * ROWS_PER_DUMP
    for i = 1, ROWS_PER_DUMP do
        s:replace{base + i, make_value(base + i)}
    end
    box.snapshot()
    wait_dump(pk, dump)
end
local t1 = fiber.clock()
report('Phase 1 done in %.2f s', t1 - t0)

-- Report dict stats after dumps.
local ds = pk:stat().dict or {}
report('=== DICT STATS AFTER DUMPS ===')
report('sample_size:   %d', ds.sample_size or 0)
report('dict_gain:     %.4f', ds.dict_gain or 0)
report('dict_rate:     %.4f', ds.dict_rate or 0)
report('no_dict_rate:  %.4f', ds.no_dict_rate or 0)
report('dict_cpu_ns:   %d', ds.dict_cpu_ns or 0)
report('no_dict_cpu_ns:%d', ds.no_dict_cpu_ns or 0)

local is = pk:stat()
report('=== DISK STATS AFTER DUMPS ===')
report('ranges:        %d', is.range_count)
report('runs:          %d', is.run_count)
report('disk_bytes:    %d', is.disk.bytes or 0)
report('dump_in:       %d', is.disk.dump.input.bytes or 0)
report('dump_out:      %d', is.disk.dump.output.bytes or 0)
report('comp_in:       %d', is.disk.compaction.input.bytes or 0)
report('comp_out:      %d', is.disk.compaction.output.bytes or 0)

local vs = box.stat.vinyl()
report('=== GLOBAL DICT STATS ===')
report('attempted:     %d', vs.dict.train_attempted)
report('accepted:      %d', vs.dict.train_accepted)
report('rejected:      %d', vs.dict.train_rejected)
report('failed:        %d', vs.dict.train_failed)
report('throttled:     %d', vs.dict.train_throttled)
report('dicts_created: %d', vs.dict.dicts_created)

-- Phase 2: force a full compaction to measure compacted footprint.
report('=== PHASE 2: compacting ===')
local t2 = fiber.clock()
pk:compact()
local comp_before = is.disk.compaction.count
wait_compaction(pk, comp_before + 1)
-- Wait for all background compaction to settle.
fiber.sleep(1)
local t3 = fiber.clock()
report('Compaction done in %.2f s', t3 - t2)

is = pk:stat()
ds = is.dict or {}
report('=== DICT STATS AFTER COMPACTION ===')
report('sample_size:   %d', ds.sample_size or 0)
report('dict_gain:     %.4f', ds.dict_gain or 0)
report('dict_rate:     %.4f', ds.dict_rate or 0)
report('no_dict_rate:  %.4f', ds.no_dict_rate or 0)
report('dict_cpu_ns:   %d', ds.dict_cpu_ns or 0)
report('no_dict_cpu_ns:%d', ds.no_dict_cpu_ns or 0)

report('=== DISK STATS AFTER COMPACTION ===')
report('ranges:        %d', is.range_count)
report('runs:          %d', is.run_count)
report('disk_bytes:    %d', is.disk.bytes or 0)
report('comp_in:       %d', is.disk.compaction.input.bytes or 0)
report('comp_out:      %d', is.disk.compaction.output.bytes or 0)

-- Compute raw data size for reference.
local raw_size = 0
for i = 1, NUM_KEYS do
    raw_size = raw_size + #make_value(i) + 16 -- rough msgpack overhead
end
math.randomseed(1) -- reset for reproducibility
report('=== SUMMARY ===')
report('raw_data_est:  %d', raw_size)
report('disk_bytes:    %d', is.disk.bytes or 0)
report('space_amp:     %.2f', (is.disk.bytes or 0) / raw_size)
if ds.dict_rate and ds.dict_rate > 0 then
    report('dict_rate:     %.4f (%.1f%% of original)',
           ds.dict_rate, ds.dict_rate * 100)
end
if ds.no_dict_rate and ds.no_dict_rate > 0 then
    report('no_dict_rate:  %.4f (%.1f%% of original)',
           ds.no_dict_rate, ds.no_dict_rate * 100)
end
if ds.dict_gain and ds.dict_gain > 0 then
    report('dict_gain:     %.2f%% smaller with dict', ds.dict_gain * 100)
end
if (ds.dict_cpu_ns or 0) > 0 and (ds.no_dict_cpu_ns or 0) > 0 then
    report('cpu_ratio:     %.2fx (dict/no_dict)',
           ds.dict_cpu_ns / ds.no_dict_cpu_ns)
end
report('=== END ===')

s:drop()
os.exit(0)

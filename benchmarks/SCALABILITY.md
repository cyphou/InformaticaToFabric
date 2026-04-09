# Scalability Report

Generated: 2026-04-09 10:08 UTC

## Benchmark Results

| Scale | Total (s) | Mappings/sec | Peak Memory (MB) | Status |
|------:|----------:|-------------:|------------------:|--------|
| 10 | 3.0 | 5.0 | 150 | Pass |

## Phase Breakdown (Latest Run)

| Phase | Duration (s) | Memory Delta (MB) | Status |
|-------|-------------:|------------------:|--------|
| generate | 1.000 | +20.0 | ok |
| assess | 2.000 | +30.0 | ok |

## Recommendations

- For 100+ mappings: use `--parallel-waves 4` for 3x+ speedup
- For 500+ mappings: ensure 8GB+ RAM, consider streaming XML parser
- For 1000+ mappings: use container deployment with 16GB RAM, SSD storage

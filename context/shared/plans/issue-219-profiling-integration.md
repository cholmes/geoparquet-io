# Issue #219: Add Profiling Integration to Benchmark Suite

**Issue:** https://github.com/geoparquet/geoparquet-io/issues/219
**Status:** In Progress
**Branch:** `feature/profiling-integration`

## Summary

Add optional cProfile integration to detect which code paths cause performance regressions when benchmarks identify slowdowns.

## Goals

1. **Opt-in profiling** - Add `--profile` flag to benchmark runs
2. **Actionable output** - Generate `.prof` files + human-readable summaries
3. **CI integration** - Upload profile artifacts when regressions detected
4. **Zero overhead by default** - Profiling disabled unless explicitly requested

## Architecture

### Core Changes

**File: `geoparquet_io/core/benchmark_suite.py`**
- Modify `run_single_operation()` to accept `profile: bool = False` parameter
- Wrap execution with `cProfile.Profile()` when profiling enabled
- Return profile data path in `BenchmarkResult.details`
- Add `generate_profile_summary()` helper for top-N function analysis

**File: `geoparquet_io/benchmarks/profile_report.py` (NEW)**
- `format_profile_stats(prof_path: Path, top_n: int = 20) -> str` - Generate pstats summary
- `save_profile_data(profiler: Profile, output_path: Path)` - Serialize .prof files
- Optional: Flamegraph generation if `py-spy` available

### CLI Changes

**File: `geoparquet_io/cli/main.py`** (if benchmark command exists) OR **scripts/version_benchmark.py**
- Add `--profile` flag to enable profiling
- Add `--profile-dir` to specify output directory (default: `./profiles/`)
- Display summary of generated profile files after run

### CI Integration

**File: `.github/workflows/benchmark.yml`**
- Add step to run profiling when regressions detected (threshold: >10% time increase)
- Upload `.prof` files and text summaries as artifacts
- Add "🔍 Profile data available" link to PR comments when profiles generated

## Implementation Plan

### Phase 1: Core Profiling (TDD)

1. **Test: Profile data generation** (`tests/test_benchmark_suite.py`)
   - Test `run_single_operation(profile=True)` generates `.prof` file
   - Test profile path stored in `BenchmarkResult.details["profile_path"]`
   - Test profiling disabled by default (no overhead)

2. **Implement: Modify `run_single_operation()`**
   ```python
   def run_single_operation(
       ...,
       profile: bool = False,
       profile_dir: Path | None = None,
   ) -> BenchmarkResult:
       if profile:
           profiler = cProfile.Profile()
           profiler.enable()

       try:
           details = run_func(input_path, output_dir)
           # ... existing code ...

           if profile:
               profiler.disable()
               profile_path = profile_dir / f"{operation}_{input_path.stem}_{iteration}.prof"
               profiler.dump_stats(profile_path)
               details["profile_path"] = str(profile_path)
   ```

3. **Test: Profile summary generation** (`tests/test_profile_report.py`)
   - Test `format_profile_stats()` produces readable top-N summary
   - Test filtering internal/stdlib functions (focus on gpio code)
   - Test error handling for invalid profile files

4. **Implement: Profile report module** (`geoparquet_io/benchmarks/profile_report.py`)
   - Use `pstats.Stats` to analyze `.prof` files
   - Sort by cumulative time, show top 20 functions
   - Filter to show only `geoparquet_io.*` and `duckdb.*` functions

### Phase 2: CLI Integration

5. **Test: CLI flag parsing** (`tests/test_version_benchmark.py`)
   - Test `--profile` flag enables profiling
   - Test `--profile-dir` specifies output location
   - Test profile summary printed after benchmark completion

6. **Implement: CLI flags** (`scripts/version_benchmark.py`)
   - Add argparse flags for `--profile` and `--profile-dir`
   - Pass through to `run_single_operation()`
   - Print summary at end listing generated profiles

### Phase 3: CI Integration

7. **Test: Regression-triggered profiling** (manual CI testing)
   - Create intentional regression in test branch
   - Verify profiling auto-triggers when threshold exceeded
   - Verify artifacts uploaded correctly

8. **Implement: CI workflow** (`.github/workflows/benchmark.yml`)
   - Add conditional step: if comparison shows >10% regression
   - Re-run benchmarks with `--profile` flag for regressed operations
   - Upload artifacts with 30-day retention
   - Add PR comment with artifact links

### Phase 4: Documentation

9. **Update: `docs/guide/benchmarks.md`**
   - Add "Profiling Integration" section
   - Examples of running with `--profile`
   - How to read profile summaries
   - Link to CI artifacts

10. **Update: Issue acceptance criteria**
    - Verify all checkboxes in issue #219

## Technical Decisions

### ✅ Decided

1. **Use cProfile** - Standard library, no dependencies, good Python-level profiling
2. **Opt-in by default** - No overhead unless `--profile` flag passed
3. **Output format** - `.prof` files + pstats text summary (top 20 functions)
4. **CI trigger** - Auto-profile on >10% time regression detection
5. **Storage** - GitHub Actions artifacts with 30-day retention

### ❓ Questions for User

1. **Flamegraph generation?**
   - Should we add `py-spy` as optional dependency for SVG flamegraphs?
   - Pro: Better visualization, easy to share
   - Con: Extra dependency, requires Rust toolchain to build

2. **Profile scope in CI?**
   - Option A: Profile ALL operations when regression detected
   - Option B: Profile ONLY regressed operations (more efficient)
   - Recommendation: B (faster, more focused)

3. **Profile retention?**
   - Default: 30 days (standard artifacts)
   - Alternative: 90 days for releases only
   - Recommendation: 30 days (sufficient for debugging)

## Testing Strategy

- **Unit tests**: Profile generation, report formatting
- **Integration tests**: Full benchmark run with profiling
- **Manual CI tests**: Trigger profiling in PR with `benchmark` label
- **Coverage target**: >80% for new code

## Out of Scope

- Line-level profiling (use `line_profiler` separately if needed)
- Memory profiling beyond existing RSS tracking
- Real-time profiling dashboard
- Automatic performance optimization suggestions

## Success Criteria

- [ ] `run_single_operation(profile=True)` generates `.prof` files
- [ ] Profile summary shows top 20 slowest functions
- [ ] `--profile` flag works in `version_benchmark.py`
- [ ] CI workflow uploads profiles when regression detected
- [ ] Documentation updated in `docs/guide/benchmarks.md`
- [ ] Tests pass with >80% coverage for new code
- [ ] Complexity remains grade A

## Dependencies

**New:** None (uses stdlib `cProfile` and `pstats`)
**Optional:** `py-spy` for flamegraphs (if user wants this feature)

## Timeline Estimate

- Phase 1 (Core): 3-4 hours
- Phase 2 (CLI): 1-2 hours
- Phase 3 (CI): 2-3 hours
- Phase 4 (Docs): 1 hour
- **Total: 7-10 hours** (~1-1.5 days of focused work)

"""
test_adb.jl

Test for the Asian Development Bank (ADB) Key Indicators Database (KIDB).
No API key required.

Note: Data fetching uses an async CSRF-protected export workflow. If
fetch_data() throws an error about "server-side export job failed", the
ADB server is rejecting programmatic exports. Use search_indicators() and
get_metadata() for discovery — these always work.

Series fetched:
  PHI/1200004   — Philippines GDP at current prices (annual)
  IND/1200004   — India GDP at current prices (annual)
  KOR/1200021   — Korea GDP growth rate (annual)
  PHI/1200140   — Philippines CPI inflation (annual)

Run from test/ directory:
    julia --project=.. test_adb.jl
"""

using Pkg
Pkg.activate(joinpath(@__DIR__, ".."))

using EconRiskMetrics
using DataFrames
using Dates
using Plots

load_env(joinpath(@__DIR__, "..", ".env"))

println("=" ^ 60)
println("  Asian Development Bank — KIDB")
println("=" ^ 60)

adb = AdbSource()

println("\nValidating connection (search endpoint)...")
println(validate_connection(adb) ? "  ✓ Connected" : "  ✗ Connection failed")

# ─── Discovery (always works) ─────────────────────────────────────────────────

println("\nSearching for GDP indicators...")
gdp_hits = search_indicators(adb, "GDP")
println("  Found $(length(gdp_hits)) results:")
for h in first(gdp_hits, 5)
    println("    code=$(h["code"])  name=$(h["name"])")
end

println("\nSearching for CPI indicators...")
cpi_hits = search_indicators(adb, "CPI")
for h in first(cpi_hits, 3)
    println("    code=$(h["code"])  name=$(h["name"])")
end

println("\nFetching metadata for PHI/1200004 (Philippines GDP)...")
meta = get_metadata(adb, "PHI/1200004")
println("  Economy  : $(get(meta, "economy_code", "n/a"))")
println("  Indicator: $(get(meta, "indicator_id", "n/a"))")
if haskey(meta, "Definition")
    println("  Definition (first 120 chars): $(first(string(meta["Definition"]), 120))...")
end

println("\nListing curated series...")
series = list_available_series(adb)
for s in series
    println("  $s")
end

# ─── Data fetch (may fail if ADB export is unavailable) ───────────────────────

println("\nAttempting data fetch for PHI/1200004 (Philippines GDP)...")
println("  Note: This uses the ADB async export workflow and may fail server-side.")
try
    gdp = fetch_time_series(adb, "PHI/1200004",
                             start_date=Date(2000,1,1),
                             end_date=Date(2023,1,1))
    println("  $(nrow(gdp)) annual obs  |  $(gdp.date[1]) → $(gdp.date[end])")
    println("  Latest: $(gdp.value[end])")
    global phi_gdp = gdp
catch e
    @warn "GDP fetch failed (expected if ADB export API is unavailable): $e"
    global phi_gdp = nothing
end

println("\nAttempting data fetch for KOR/1200021 (Korea GDP growth)...")
try
    growth = fetch_time_series(adb, "KOR/1200021",
                                start_date=Date(2000,1,1))
    println("  $(nrow(growth)) annual obs  |  Latest: $(growth.value[end])%")
    global kor_growth = growth
catch e
    @warn "Korea growth fetch failed: $e"
    global kor_growth = nothing
end

# ─── Plot ─────────────────────────────────────────────────────────────────────

plots = []

if phi_gdp !== nothing
    p1 = bar(year.(phi_gdp.date), phi_gdp.value ./ 1e9,
        label="Philippines GDP",
        color=:steelblue,
        ylabel="Trillion USD",
        title="Philippines GDP at Current Prices (Annual)")
    push!(plots, p1)
end

if kor_growth !== nothing
    p2 = bar(year.(kor_growth.date), kor_growth.value,
        label="Korea GDP growth",
        color=:crimson,
        ylabel="% change",
        title="Korea GDP Growth Rate (Annual)")
    hline!(p2, [0], color=:black, lw=0.8, label="")
    push!(plots, p2)
end

if !isempty(plots)
    println("\nGenerating plot...")
    n = length(plots)
    adb_plot = plot(plots...;
        layout=(n, 1),
        size=(900, 400 * n),
        left_margin=8Plots.mm,
        bottom_margin=6Plots.mm,
        titlefontsize=10,
        legendfontsize=8,
        plot_title="Asian Development Bank — KIDB",
        plot_titlefontsize=12)

    out_path = joinpath(@__DIR__, "plots", "plot_adb.png")
    savefig(adb_plot, out_path)
    println("Plot saved → $out_path")
else
    println("\nNo data fetched — discovery endpoints worked. " *
            "Data export requires the ADB server to accept programmatic requests.")
end

println("\n" * "=" ^ 60)
println("Done.")
println("=" ^ 60)

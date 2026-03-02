"""
test_ssb.jl

Test and plot for Statistics Norway (SSB) data source.
No API key required.

Series fetched:
  08801/KPI        — Consumer Price Index (CPI), monthly
  09189/BNP        — GDP at current prices (NOK million), quarterly
  07129/AKU_SYSAV  — Unemployment rate (LFS, seasonally adjusted), quarterly

Run from test/ directory:
    julia --project=.. test_ssb.jl
"""

using Pkg
Pkg.activate(joinpath(@__DIR__, ".."))

using EconRiskMetrics
using DataFrames
using Dates
using Plots

load_env(joinpath(@__DIR__, "..", ".env"))

println("=" ^ 60)
println("  Statistics Norway (SSB) — PxWeb JSON-stat 2.0 API")
println("=" ^ 60)

ssb = SsbSource()

println("\nValidating connection...")
println(validate_connection(ssb) ? "  ✓ Connected" : "  ✗ Connection failed")

# ─── Fetch series ──────────────────────────────────────────────────────────────

println("\nFetching Consumer Price Index (08801/KPI)...")
try
    cpi = fetch_time_series(ssb, "08801/KPI", start_date=Date(2000,1,1))
    println("  $(nrow(cpi)) monthly obs")
    if nrow(cpi) > 0
        println("  $(cpi.date[1]) → $(cpi.date[end])")
        println("  Latest: $(cpi.value[end])  |  Min: $(minimum(cpi.value))  |  Max: $(maximum(cpi.value))")
    end
    global cpi_data = cpi
catch e
    @warn "CPI fetch failed: $e"
    global cpi_data = nothing
end

println("\nFetching GDP at current prices (09189/BNP)...")
try
    gdp = fetch_time_series(ssb, "09189/BNP", start_date=Date(2000,1,1))
    println("  $(nrow(gdp)) quarterly obs")
    if nrow(gdp) > 0
        println("  $(gdp.date[1]) → $(gdp.date[end])")
        println("  Latest: $(gdp.value[end]) NOK million")
    end
    global gdp_data = gdp
catch e
    @warn "GDP fetch failed: $e"
    global gdp_data = nothing
end

println("\nFetching unemployment rate (07129/AKU_SYSAV)...")
try
    unemp = fetch_time_series(ssb, "07129/AKU_SYSAV", start_date=Date(2000,1,1))
    println("  $(nrow(unemp)) obs")
    if nrow(unemp) > 0
        println("  $(unemp.date[1]) → $(unemp.date[end])")
        println("  Latest: $(unemp.value[end])%")
    end
    global unemp_data = unemp
catch e
    @warn "Unemployment fetch failed: $e"
    global unemp_data = nothing
end

println("\nFetching metadata for CPI (08801/KPI)...")
meta = get_metadata(ssb, "08801/KPI")
println("  Title : $(get(meta, "title", "n/a"))")
println("  Series: $(get(meta, "series", "n/a"))")
println("  URL   : $(get(meta, "url", "n/a"))")

println("\nListing curated series...")
series = list_available_series(ssb)
for s in series
    println("  $s")
end

# ─── Plot ─────────────────────────────────────────────────────────────────────

println("\nGenerating plots...")
plots = []

if cpi_data !== nothing && nrow(cpi_data) > 0
    p1 = plot(cpi_data.date, cpi_data.value,
        label="CPI",
        color=:steelblue, lw=1.2,
        ylabel="Index",
        title="Norway Consumer Price Index (08801/KPI)")
    push!(plots, p1)
end

if gdp_data !== nothing && nrow(gdp_data) > 0
    p2 = plot(gdp_data.date, gdp_data.value ./ 1_000,
        label="GDP",
        color=:forestgreen, lw=1.2,
        ylabel="NOK billion",
        title="Norway GDP at Current Prices (quarterly)")
    push!(plots, p2)
end

if unemp_data !== nothing && nrow(unemp_data) > 0
    p3 = plot(unemp_data.date, unemp_data.value,
        label="Unemployment (SA)",
        color=:crimson, lw=1.2,
        ylabel="% of labour force",
        title="Norway Unemployment Rate (LFS, SA)")
    push!(plots, p3)
end

if !isempty(plots)
    n = length(plots)
    ssb_plot = plot(plots...;
        layout=(n, 1),
        size=(1000, 380 * n),
        left_margin=8Plots.mm,
        bottom_margin=6Plots.mm,
        titlefontsize=10,
        legendfontsize=8,
        plot_title="Statistics Norway (SSB)",
        plot_titlefontsize=12)

    out_path = joinpath(@__DIR__, "plots", "plot_ssb.png")
    savefig(ssb_plot, out_path)
    println("Plot saved → $out_path")
else
    println("No data fetched — check connection")
end

println("\n" * "=" ^ 60)
println("Done.")
println("=" ^ 60)

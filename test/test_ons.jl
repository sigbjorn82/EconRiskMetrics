"""
test_ons.jl

Test and plot for UK Office for National Statistics (ONS) data source.
No API key required.

Series fetched:
  cpih01/L55O  — CPIH all-items annual rate
  qna/ABMI     — GDP quarterly, chained volume, SA
  lms/MGSX     — Unemployment rate, LFS 16+, SA

Run from test/ directory:
    julia --project=.. test_ons.jl
"""

using Pkg
Pkg.activate(joinpath(@__DIR__, ".."))

using EconRiskMetrics
using DataFrames
using Dates
using Plots

load_env(joinpath(@__DIR__, "..", ".env"))

println("=" ^ 60)
println("  UK Office for National Statistics — ONS API")
println("=" ^ 60)

ons = OnsSource()

println("\nValidating connection...")
println(validate_connection(ons) ? "  ✓ Connected" : "  ✗ Connection failed")

# ─── Fetch series ──────────────────────────────────────────────────────────────

println("\nFetching CPIH all-items annual rate (cpih01/L55O)...")
try
    cpi = fetch_time_series(ons, "cpih01/L55O", start_date=Date(2000,1,1))
    println("  $(nrow(cpi)) obs")
    if nrow(cpi) > 0
        println("  $(cpi.date[1]) → $(cpi.date[end])")
        println("  Latest: $(cpi.value[end])%")
    end
    global cpi_data = cpi
catch e
    @warn "CPIH fetch failed: $e"
    global cpi_data = nothing
end

println("\nFetching GDP quarterly, SA (qna/ABMI)...")
try
    gdp = fetch_time_series(ons, "qna/ABMI", start_date=Date(2000,1,1))
    println("  $(nrow(gdp)) quarterly obs")
    if nrow(gdp) > 0
        println("  $(gdp.date[1]) → $(gdp.date[end])")
        println("  Latest index: $(gdp.value[end])")
    end
    global gdp_data = gdp
catch e
    @warn "GDP fetch failed: $e"
    global gdp_data = nothing
end

println("\nFetching unemployment rate (lms/MGSX)...")
try
    unemp = fetch_time_series(ons, "lms/MGSX", start_date=Date(2000,1,1))
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

println("\nFetching metadata for CPIH...")
meta = get_metadata(ons, "cpih01/L55O")
println("  Title : $(get(meta, "title", "n/a"))")
println("  Unit  : $(get(meta, "unit", "n/a"))")

println("\nListing curated series...")
series = list_available_series(ons)
for s in series
    println("  $s")
end

# ─── Plot ─────────────────────────────────────────────────────────────────────

println("\nGenerating plots...")
plots = []

if cpi_data !== nothing && nrow(cpi_data) > 0
    p1 = plot(cpi_data.date, cpi_data.value,
        label="CPIH",
        color=:steelblue, lw=1.5,
        ylabel="Annual rate (%)",
        title="UK CPIH Annual Rate",
        fillrange=0, fillalpha=0.10, fillcolor=:steelblue)
    push!(plots, p1)
end

if gdp_data !== nothing && nrow(gdp_data) > 0
    p2 = plot(gdp_data.date, gdp_data.value,
        label="GDP (SA)",
        color=:forestgreen, lw=1.2,
        ylabel="Index",
        title="UK GDP (Chained Volume, SA)")
    push!(plots, p2)
end

if unemp_data !== nothing && nrow(unemp_data) > 0
    p3 = plot(unemp_data.date, unemp_data.value,
        label="Unemployment",
        color=:crimson, lw=1.2,
        ylabel="% of labour force",
        title="UK Unemployment Rate (LFS)")
    push!(plots, p3)
end

if !isempty(plots)
    n = length(plots)
    ons_plot = plot(plots...;
        layout=(n, 1),
        size=(1000, 380 * n),
        left_margin=8Plots.mm,
        bottom_margin=6Plots.mm,
        titlefontsize=10,
        legendfontsize=8,
        plot_title="UK Office for National Statistics",
        plot_titlefontsize=12)

    out_path = joinpath(@__DIR__, "plots", "plot_ons.png")
    savefig(ons_plot, out_path)
    println("Plot saved → $out_path")
else
    println("No data fetched — check connection")
end

println("\n" * "=" ^ 60)
println("Done.")
println("=" ^ 60)

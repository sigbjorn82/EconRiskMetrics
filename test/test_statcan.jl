"""
test_statcan.jl

Test and plot for Statistics Canada (StatCan) data source.
No API key required.

Series fetched:
  v41690973  — CPI all-items, Canada (monthly)
  v2062811   — Unemployment rate, Canada (monthly)
  v62305752  — GDP at market prices, monthly
  v39079     — Bank of Canada overnight rate

Run from test/ directory:
    julia --project=.. test_statcan.jl
"""

using Pkg
Pkg.activate(joinpath(@__DIR__, ".."))

using EconRiskMetrics
using DataFrames
using Dates
using Plots

load_env(joinpath(@__DIR__, "..", ".env"))

println("=" ^ 60)
println("  Statistics Canada — Web Data Service")
println("=" ^ 60)

sc = StatCanSource()

println("\nValidating connection...")
println(validate_connection(sc) ? "  ✓ Connected" : "  ✗ Connection failed")

# ─── Fetch series ──────────────────────────────────────────────────────────────

println("\nFetching CPI all-items, Canada (v41690973)...")
try
    cpi = fetch_time_series(sc, "v41690973", start_date=Date(2000,1,1))
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

println("\nFetching unemployment rate, Canada (v2062811)...")
try
    unemp = fetch_time_series(sc, "v2062811", start_date=Date(2000,1,1))
    println("  $(nrow(unemp)) monthly obs")
    if nrow(unemp) > 0
        println("  $(unemp.date[1]) → $(unemp.date[end])")
        println("  Latest: $(unemp.value[end])%")
    end
    global unemp_data = unemp
catch e
    @warn "Unemployment fetch failed: $e"
    global unemp_data = nothing
end

println("\nFetching Bank of Canada overnight rate (v39079)...")
try
    rate = fetch_time_series(sc, "v39079", start_date=Date(2000,1,1))
    println("  $(nrow(rate)) monthly obs")
    if nrow(rate) > 0
        println("  $(rate.date[1]) → $(rate.date[end])")
        println("  Latest: $(rate.value[end])%")
    end
    global rate_data = rate
catch e
    @warn "Rate fetch failed: $e"
    global rate_data = nothing
end

println("\nFetching metadata for CPI vector...")
meta = get_metadata(sc, "v41690973")
println("  Vector ID  : $(get(meta, "vector_id", "n/a"))")
println("  Product ID : $(get(meta, "product_id", "n/a"))")
println("  Frequency  : $(get(meta, "frequency", "n/a"))")

println("\nListing curated series...")
series = list_available_series(sc)
for s in series
    println("  $s")
end

# ─── Plot ─────────────────────────────────────────────────────────────────────

println("\nGenerating plots...")
plots = []

if cpi_data !== nothing && nrow(cpi_data) > 0
    p1 = plot(cpi_data.date, cpi_data.value,
        label="CPI all-items",
        color=:steelblue, lw=1.2,
        ylabel="Index (2002=100)",
        title="Canada CPI All-Items (Monthly)")
    push!(plots, p1)
end

if unemp_data !== nothing && nrow(unemp_data) > 0
    p2 = plot(unemp_data.date, unemp_data.value,
        label="Unemployment rate",
        color=:crimson, lw=1.2,
        ylabel="% of labour force",
        title="Canada Unemployment Rate",
        fillrange=0, fillalpha=0.10, fillcolor=:crimson)
    push!(plots, p2)
end

if rate_data !== nothing && nrow(rate_data) > 0
    p3 = plot(rate_data.date, rate_data.value,
        label="BoC overnight rate",
        color=:forestgreen, lw=1.5,
        ylabel="% per annum",
        title="Bank of Canada Overnight Rate")
    push!(plots, p3)
end

if !isempty(plots)
    n = length(plots)
    sc_plot = plot(plots...;
        layout=(n, 1),
        size=(1000, 380 * n),
        left_margin=8Plots.mm,
        bottom_margin=6Plots.mm,
        titlefontsize=10,
        legendfontsize=8,
        plot_title="Statistics Canada",
        plot_titlefontsize=12)

    out_path = joinpath(@__DIR__, "plots", "plot_statcan.png")
    savefig(sc_plot, out_path)
    println("Plot saved → $out_path")
else
    println("No data fetched — check connection")
end

println("\n" * "=" ^ 60)
println("Done.")
println("=" ^ 60)

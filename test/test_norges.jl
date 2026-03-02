"""
test_norges.jl

Test and plot for Norges Bank (Norway) data source.
No API key required.

Series fetched:
  EXR/D.USD.NOK.SP  — USD/NOK daily spot rate
  EXR/D.EUR.NOK.SP  — EUR/NOK daily spot rate
  IR/B.KPRA.SD      — Norges Bank key policy rate

Run from test/ directory:
    julia --project=.. test_norges.jl
"""

using Pkg
Pkg.activate(joinpath(@__DIR__, ".."))

using EconRiskMetrics
using DataFrames
using Dates
using Plots

load_env(joinpath(@__DIR__, "..", ".env"))

println("=" ^ 60)
println("  Norges Bank — SDMX Data API")
println("=" ^ 60)

nb = NorgesSource()

println("\nValidating connection...")
println(validate_connection(nb) ? "  ✓ Connected" : "  ✗ Connection failed")

# ─── Fetch series ──────────────────────────────────────────────────────────────

println("\nFetching USD/NOK daily (EXR/D.USD.NOK.SP)...")
try
    usdnok = fetch_time_series(nb, "EXR/D.USD.NOK.SP", start_date=Date(2015,1,1))
    println("  $(nrow(usdnok)) daily obs")
    if nrow(usdnok) > 0
        println("  $(usdnok.date[1]) → $(usdnok.date[end])")
        println("  Latest: $(usdnok.value[end])  |  Min: $(minimum(usdnok.value))  |  Max: $(maximum(usdnok.value))")
    end
    global usdnok_data = usdnok
catch e
    @warn "USD/NOK fetch failed: $e"
    global usdnok_data = nothing
end

println("\nFetching EUR/NOK daily (EXR/D.EUR.NOK.SP)...")
try
    eurnok = fetch_time_series(nb, "EXR/D.EUR.NOK.SP", start_date=Date(2015,1,1))
    println("  $(nrow(eurnok)) daily obs  |  Latest: $(nrow(eurnok) > 0 ? eurnok.value[end] : "n/a")")
    global eurnok_data = eurnok
catch e
    @warn "EUR/NOK fetch failed: $e"
    global eurnok_data = nothing
end

println("\nFetching Norges Bank policy rate (IR/B.KPRA.SD)...")
try
    rate = fetch_time_series(nb, "IR/B.KPRA.SD", start_date=Date(2000,1,1))
    println("  $(nrow(rate)) obs")
    if nrow(rate) > 0
        println("  $(rate.date[1]) → $(rate.date[end])")
        println("  Latest: $(rate.value[end])%")
    end
    global rate_data = rate
catch e
    @warn "Policy rate fetch failed: $e"
    global rate_data = nothing
end

println("\nListing curated series...")
series = list_available_series(nb)
for s in series
    println("  $s")
end

# ─── Plot ─────────────────────────────────────────────────────────────────────

println("\nGenerating plots...")
plots = []

if usdnok_data !== nothing && nrow(usdnok_data) > 0
    p1 = plot(usdnok_data.date, usdnok_data.value,
        label="USD/NOK",
        color=:steelblue, lw=0.8,
        ylabel="NOK per USD",
        title="USD/NOK Daily Spot Rate")
    push!(plots, p1)
end

if rate_data !== nothing && nrow(rate_data) > 0
    p2 = plot(rate_data.date, rate_data.value,
        label="Policy rate",
        color=:crimson, lw=1.5,
        ylabel="% per annum",
        title="Norges Bank Key Policy Rate",
        fillrange=0, fillalpha=0.12, fillcolor=:crimson)
    push!(plots, p2)
end

if !isempty(plots)
    n = length(plots)
    nb_plot = plot(plots...;
        layout=(n, 1),
        size=(1000, 380 * n),
        left_margin=8Plots.mm,
        bottom_margin=6Plots.mm,
        titlefontsize=10,
        legendfontsize=8,
        plot_title="Norges Bank",
        plot_titlefontsize=12)

    out_path = joinpath(@__DIR__, "plots", "plot_norges.png")
    savefig(nb_plot, out_path)
    println("Plot saved → $out_path")
else
    println("No data fetched — check connection")
end

println("\n" * "=" ^ 60)
println("Done.")
println("=" ^ 60)

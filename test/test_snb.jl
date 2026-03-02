"""
test_snb.jl

Test and plot for Swiss National Bank (SNB) data source.
No API key required.

Series fetched:
  zimoma      — SNB sight deposit rate (policy rate, monthly)
  devkud/EUR  — EUR/CHF daily spot rate
  devkud/USD  — USD/CHF daily spot rate
  gkbgeld/M3  — M3 money supply (monthly)

Run from test/ directory:
    julia --project=.. test_snb.jl
"""

using Pkg
Pkg.activate(joinpath(@__DIR__, ".."))

using EconRiskMetrics
using DataFrames
using Dates
using Plots

load_env(joinpath(@__DIR__, "..", ".env"))

println("=" ^ 60)
println("  Swiss National Bank — SNB Data Portal API")
println("=" ^ 60)

snb = SnbSource()

println("\nValidating connection...")
println(validate_connection(snb) ? "  ✓ Connected" : "  ✗ Connection failed")

# ─── Fetch series ──────────────────────────────────────────────────────────────

println("\nFetching SNB sight deposit rate (zimoma)...")
try
    rate = fetch_time_series(snb, "zimoma", start_date=Date(2000,1,1))
    println("  $(nrow(rate)) monthly obs")
    if nrow(rate) > 0
        println("  $(rate.date[1]) → $(rate.date[end])")
        println("  Latest: $(rate.value[end])%")
    end
    global rate_data = rate
catch e
    @warn "SNB rate fetch failed: $e"
    global rate_data = nothing
end

println("\nFetching EUR/CHF daily (devkud/EUR)...")
try
    eurchf = fetch_time_series(snb, "devkud/EUR", start_date=Date(2015,1,1))
    println("  $(nrow(eurchf)) daily obs")
    if nrow(eurchf) > 0
        println("  $(eurchf.date[1]) → $(eurchf.date[end])")
        println("  Latest: $(eurchf.value[end])  |  Min: $(minimum(eurchf.value))  |  Max: $(maximum(eurchf.value))")
    end
    global eurchf_data = eurchf
catch e
    @warn "EUR/CHF fetch failed: $e"
    global eurchf_data = nothing
end

println("\nFetching USD/CHF daily (devkud/USD)...")
try
    usdchf = fetch_time_series(snb, "devkud/USD", start_date=Date(2015,1,1))
    println("  $(nrow(usdchf)) daily obs  |  Latest: $(nrow(usdchf) > 0 ? usdchf.value[end] : "n/a")")
    global usdchf_data = usdchf
catch e
    @warn "USD/CHF fetch failed: $e"
    global usdchf_data = nothing
end

println("\nFetching metadata for EUR/CHF cube dimensions...")
meta = get_metadata(snb, "devkud/EUR")
println("  Cube   : $(get(meta, "cube", "n/a"))")
println("  d0 vals: $(get(meta, "d0_values", "n/a"))")

println("\nListing curated series...")
series = list_available_series(snb)
for s in series
    println("  $s")
end

# ─── Plot ─────────────────────────────────────────────────────────────────────

println("\nGenerating plots...")
plots = []

if eurchf_data !== nothing && nrow(eurchf_data) > 0
    p1 = plot(eurchf_data.date, eurchf_data.value,
        label="EUR/CHF",
        color=:steelblue, lw=0.8,
        ylabel="CHF per EUR",
        title="EUR/CHF Daily Rate")
    hline!(p1, [1.0], color=:black, lw=0.8, linestyle=:dash, label="Parity")
    push!(plots, p1)
end

if rate_data !== nothing && nrow(rate_data) > 0
    p2 = plot(rate_data.date, rate_data.value,
        label="SNB policy rate",
        color=:crimson, lw=1.5,
        ylabel="% per annum",
        title="SNB Sight Deposit Rate",
        fillrange=0, fillalpha=0.12, fillcolor=:crimson)
    push!(plots, p2)
end

if !isempty(plots)
    n = length(plots)
    snb_plot = plot(plots...;
        layout=(n, 1),
        size=(1000, 380 * n),
        left_margin=8Plots.mm,
        bottom_margin=6Plots.mm,
        titlefontsize=10,
        legendfontsize=8,
        plot_title="Swiss National Bank",
        plot_titlefontsize=12)

    out_path = joinpath(@__DIR__, "plots", "plot_snb.png")
    savefig(snb_plot, out_path)
    println("Plot saved → $out_path")
else
    println("No data fetched — check connection")
end

println("\n" * "=" ^ 60)
println("Done.")
println("=" ^ 60)

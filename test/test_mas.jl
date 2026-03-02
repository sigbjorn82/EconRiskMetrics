"""
test_mas.jl

Test and plot for the Monetary Authority of Singapore (MAS) data source.
No API key required.

Note: The MAS API at eservices.mas.gov.sg undergoes scheduled maintenance.
If the connection test fails, try again later.

Series fetched:
  95932927-.../usd_sgd  — Weekly USD/SGD exchange rate
  95932927-.../eur_sgd  — Weekly EUR/SGD exchange rate
  9a0bf149-.../sora     — Daily SORA overnight rate
  a3a9e992-.../m1       — Monthly M1 money supply
  a3a9e992-.../m2       — Monthly M2 money supply

Run from test/ directory:
    julia --project=.. test_mas.jl
"""

using Pkg
Pkg.activate(joinpath(@__DIR__, ".."))

using EconRiskMetrics
using DataFrames
using Dates
using Plots

load_env(joinpath(@__DIR__, "..", ".env"))

const FX_WEEKLY  = "95932927-c8bc-4e7a-b484-68a66a24edfe"
const RATES_DAILY = "9a0bf149-308c-4bd2-832d-76c8e6cb47ed"
const MONEY      = "a3a9e992-8a3f-43a4-9b45-59a7de73a0cd"

println("=" ^ 60)
println("  Monetary Authority of Singapore — MAS Datastore API")
println("=" ^ 60)

mas = MasSource()
println("\nValidating connection...")
ok = validate_connection(mas)
println(ok ? "  ✓ Connected" : "  ✗ Connection failed (API may be under maintenance)")
!ok && println("  Proceeding anyway — fetches may fail with a maintenance page.")

# ─── Fetch series ──────────────────────────────────────────────────────────────

println("\nFetching weekly USD/SGD FX...")
try
    usd = fetch_time_series(mas, "$(FX_WEEKLY)/usd_sgd",
                             start_date=Date(2015,1,1))
    println("  $(nrow(usd)) weekly obs  |  $(usd.date[1]) → $(usd.date[end])")
    println("  Latest: $(usd.value[end])  |  Min: $(minimum(usd.value))  |  Max: $(maximum(usd.value))")
    global usdsgd = usd
catch e
    @warn "USD/SGD fetch failed: $e"
    global usdsgd = nothing
end

println("\nFetching weekly EUR/SGD FX...")
try
    eur = fetch_time_series(mas, "$(FX_WEEKLY)/eur_sgd",
                             start_date=Date(2015,1,1))
    println("  $(nrow(eur)) weekly obs  |  Latest: $(eur.value[end])")
    global eursgd = eur
catch e
    @warn "EUR/SGD fetch failed: $e"
    global eursgd = nothing
end

println("\nFetching daily SORA overnight rate...")
try
    sora = fetch_time_series(mas, "$(RATES_DAILY)/sora",
                              start_date=Date(2020,1,1))
    println("  $(nrow(sora)) daily obs  |  $(sora.date[1]) → $(sora.date[end])")
    println("  Latest: $(sora.value[end])%")
    global sora_data = sora
catch e
    @warn "SORA fetch failed: $e"
    global sora_data = nothing
end

println("\nFetching monthly M1 money supply...")
try
    m1 = fetch_data(mas, "$(MONEY)/m1")
    println("  $(nrow(m1)) monthly obs  |  $(m1.date[1]) → $(m1.date[end])")
    println("  Latest: $(m1.value[end])")
    global m1_data = m1
catch e
    @warn "M1 fetch failed: $e"
    global m1_data = nothing
end

println("\nFetching column list for FX dataset...")
meta = get_metadata(mas, "$(FX_WEEKLY)/usd_sgd")
cols = get(meta, "all_columns", String[])
println("  Columns: $(join(cols, ", "))")
println("  Total rows: $(get(meta, "total_rows", "n/a"))")

# ─── Plot ─────────────────────────────────────────────────────────────────────

println("\nGenerating plots...")
plots = []

if usdsgd !== nothing
    p1 = plot(usdsgd.date, usdsgd.value,
        label="USD/SGD",
        color=:steelblue, lw=1.2,
        ylabel="SGD per USD",
        title="USD/SGD Weekly Rate")
    push!(plots, p1)
end

if sora_data !== nothing
    p2 = plot(sora_data.date, sora_data.value,
        label="SORA",
        color=:crimson, lw=1.2,
        ylabel="% per annum",
        title="SORA Overnight Rate",
        fillrange=0, fillalpha=0.12, fillcolor=:crimson)
    push!(plots, p2)
end

if m1_data !== nothing
    p3 = plot(m1_data.date, m1_data.value,
        label="M1",
        color=:forestgreen, lw=1.2,
        ylabel="S\$ million",
        title="Singapore M1 Money Supply")
    push!(plots, p3)
end

if !isempty(plots)
    n = length(plots)
    mas_plot = plot(plots...;
        layout=(n, 1),
        size=(1000, 380 * n),
        left_margin=8Plots.mm,
        bottom_margin=6Plots.mm,
        titlefontsize=10,
        legendfontsize=8,
        plot_title="Monetary Authority of Singapore",
        plot_titlefontsize=12)

    out_path = joinpath(@__DIR__, "plots", "plot_mas.png")
    savefig(mas_plot, out_path)
    println("Plot saved → $out_path")
else
    println("No data fetched — check API availability at https://eservices.mas.gov.sg")
end

println("\n" * "=" ^ 60)
println("Done.")
println("=" ^ 60)

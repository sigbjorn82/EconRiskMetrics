"""
test_ecos.jl

Test and plot for the Bank of Korea ECOS data source.
Requires a free API key from https://ecos.bok.or.kr.
Set ECOS_API_KEY in .env, or EcosSource() will fall back to the "sample" key
(limited — returns small data samples only).

Series fetched:
  722Y001/M/0101000   — Bank of Korea base rate (monthly, %)
  731Y003/D/0000001   — USD/KRW exchange rate (daily)
  021Y125/M/?         — CPI all items (monthly, multiple series)
  101Y004/M/?         — Money supply M1/M2 (monthly)

Run from test/ directory:
    julia --project=.. test_ecos.jl
"""

using Pkg
Pkg.activate(joinpath(@__DIR__, ".."))

using EconRiskMetrics
using DataFrames
using Dates
using Plots

load_env(joinpath(@__DIR__, "..", ".env"))

println("=" ^ 60)
println("  Bank of Korea — ECOS Open API")
println("=" ^ 60)

ecos = EcosSource()   # uses ECOS_API_KEY from .env, or "sample"
println("\nAPI key: $(ecos.api_key == "sample" ? "sample (limited)" : "registered key")")

println("\nValidating connection...")
println(validate_connection(ecos) ? "  ✓ Connected" : "  ✗ Connection failed")

# ─── Fetch series ──────────────────────────────────────────────────────────────

println("\nFetching BoK base rate (722Y001/M/0101000)...")
try
    rate = fetch_time_series(ecos, "722Y001/M/0101000", start_date=Date(2000,1,1))
    println("  $(nrow(rate)) monthly obs")
    if nrow(rate) > 0
        println("  $(rate.date[1]) → $(rate.date[end])")
        println("  Latest: $(rate.value[end])%  |  Min: $(minimum(rate.value))%  |  Max: $(maximum(rate.value))%")
    end
    global base_rate = rate
catch e
    @warn "Base rate fetch failed: $e"
    global base_rate = nothing
end

println("\nFetching USD/KRW daily (731Y003/D/0000001), last 2 years...")
try
    fx = fetch_time_series(ecos, "731Y003/D/0000001",
                           start_date=Date(2023,1,1))
    println("  $(nrow(fx)) daily obs")
    if nrow(fx) > 0
        println("  $(fx.date[1]) → $(fx.date[end])")
        println("  Latest: $(fx.value[end])  |  Min: $(minimum(fx.value))  |  Max: $(maximum(fx.value))")
    end
    global usdkrw = fx
catch e
    @warn "USD/KRW fetch failed: $e"
    global usdkrw = nothing
end

println("\nFetching CPI (021Y125/M/?) — may return multiple series...")
try
    cpi = fetch_time_series(ecos, "021Y125/M/?", start_date=Date(2010,1,1))
    println("  $(nrow(cpi)) rows  |  Columns: $(names(cpi))")
    if "series" in names(cpi)
        println("  Distinct series: $(length(unique(cpi.series)))")
    end
    global cpi_data = cpi
catch e
    @warn "CPI fetch failed: $e"
    global cpi_data = nothing
end

println("\nFetching metadata for base rate...")
meta = get_metadata(ecos, "722Y001/M/0101000")
println("  Stat code : $(get(meta, "stat_code", "n/a"))")
println("  Stat name : $(get(meta, "stat_name", "n/a"))")
println("  Source    : $(meta["source"])")

println("\nListing first 10 available stat tables...")
series = list_available_series(ecos)
for s in first(series, 10)
    println("  $s")
end
println("  ($(length(series)) total tables)")

# ─── Plot ─────────────────────────────────────────────────────────────────────

println("\nGenerating plots...")
plots = []

if base_rate !== nothing
    p1 = plot(base_rate.date, base_rate.value,
        label="BoK base rate",
        color=:steelblue, lw=1.5,
        ylabel="% per annum",
        title="Bank of Korea Base Rate",
        fillrange=0, fillalpha=0.12, fillcolor=:steelblue)
    push!(plots, p1)
end

if usdkrw !== nothing
    p2 = plot(usdkrw.date, usdkrw.value,
        label="USD/KRW",
        color=:crimson, lw=1,
        ylabel="KRW per USD",
        title="USD/KRW Exchange Rate")
    push!(plots, p2)
end

if !isempty(plots)
    n = length(plots)
    ecos_plot = plot(plots...;
        layout=(n, 1),
        size=(1000, 400 * n),
        left_margin=8Plots.mm,
        bottom_margin=6Plots.mm,
        titlefontsize=10,
        legendfontsize=8,
        plot_title="Bank of Korea — ECOS Data",
        plot_titlefontsize=12)

    out_path = joinpath(@__DIR__, "plots", "plot_ecos.png")
    savefig(ecos_plot, out_path)
    println("Plot saved → $out_path")
else
    println("No data fetched — skipping plot (check API key)")
end

println("\n" * "=" ^ 60)
println("Done.")
println("=" ^ 60)

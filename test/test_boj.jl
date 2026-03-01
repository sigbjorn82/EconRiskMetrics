"""
test_boj.jl

Dedicated test and plot for the Bank of Japan (BoJ) data source.
No API key required. Data sourced from the BoJ Time-Series Data Search API v1.

Series fetched:
  FM01/STRDCLUCON      — Uncollateralized overnight call rate (daily, %)
  IR01/MADR1M          — Basic loan rate (monthly, %, since 1882)
  MD01/MABS1AN11       — Monetary base, average outstanding (monthly, 100M JPY)
  FM08/FXERD04         — USD/JPY spot rate at 17:00 JST (daily)
  FM08/FXERM07         — USD/JPY monthly average
  PR01/PRCG20_2200000000 — PPI all commodities (monthly, index)

Run from test/ directory:
    julia --project=.. test_boj.jl
"""

using Pkg
Pkg.activate(joinpath(@__DIR__, ".."))

using EconRiskMetrics
using DataFrames
using Dates
using Plots

load_env(joinpath(@__DIR__, "..", ".env"))

println("=" ^ 60)
println("  Bank of Japan — BoJ Time-Series Data Search API")
println("=" ^ 60)

boj = BojSource()
println("\nValidating connection...")
println(validate_connection(boj) ? "  ✓ Connected" : "  ✗ Connection failed")

# ─── Fetch series ──────────────────────────────────────────────────────────────

println("\nFetching overnight call rate (FM01/STRDCLUCON)...")
call_rate = fetch_time_series(boj, "FM01/STRDCLUCON", start_date=Date(2000,1,1))
println("  $(nrow(call_rate)) daily obs  |  $(call_rate.date[1]) → $(call_rate.date[end])")
println("  Latest: $(call_rate.value[end])%  |  Min: $(minimum(call_rate.value))%")

println("\nFetching basic loan rate (IR01/MADR1M)...")
loan_rate = fetch_time_series(boj, "IR01/MADR1M", start_date=Date(1990,1,1))
println("  $(nrow(loan_rate)) monthly obs  |  $(loan_rate.date[1]) → $(loan_rate.date[end])")
println("  Latest: $(loan_rate.value[end])%  |  Peak: $(maximum(loan_rate.value))%")

println("\nFetching monetary base (MD01/MABS1AN11)...")
mon_base = fetch_time_series(boj, "MD01/MABS1AN11", start_date=Date(2000,1,1))
println("  $(nrow(mon_base)) monthly obs  |  $(mon_base.date[1]) → $(mon_base.date[end])")
println("  Latest: $(round(mon_base.value[end]/10000, digits=1)) T JPY")

println("\nFetching USD/JPY daily (FM08/FXERD04)...")
usdjpy = fetch_time_series(boj, "FM08/FXERD04", start_date=Date(2000,1,1))
println("  $(nrow(usdjpy)) daily obs  |  $(usdjpy.date[1]) → $(usdjpy.date[end])")
println("  Latest: $(usdjpy.value[end])  |  Min: $(minimum(usdjpy.value))  |  Max: $(maximum(usdjpy.value))")

println("\nFetching PPI all commodities (PR01/PRCG20_2200000000)...")
ppi = fetch_time_series(boj, "PR01/PRCG20_2200000000", start_date=Date(2000,1,1))
println("  $(nrow(ppi)) monthly obs  |  $(ppi.date[1]) → $(ppi.date[end])")

println("\nFetching metadata for call rate...")
meta = get_metadata(boj, "FM01/STRDCLUCON")
println("  Name     : $(meta["name"])")
println("  Frequency: $(meta["frequency"])")
println("  Unit     : $(meta["unit_jp"])")

# ─── Plots ────────────────────────────────────────────────────────────────────

println("\nGenerating plots...")

p1 = plot(call_rate.date, call_rate.value,
    label="Overnight call rate",
    color=:steelblue, lw=1,
    ylabel="% per annum",
    title="Overnight Call Rate",
    fillrange=0, fillalpha=0.12, fillcolor=:steelblue)

p2 = plot(loan_rate.date, loan_rate.value,
    label="Basic loan rate",
    color=:crimson, lw=1.5,
    marker=:circle, ms=2,
    ylabel="% per annum",
    title="BoJ Basic Loan Rate (since 1990)")

p3 = plot(mon_base.date, mon_base.value ./ 10_000,
    label="Monetary base",
    color=:forestgreen, lw=1.5,
    ylabel="Trillion JPY",
    title="Monetary Base")

p4 = plot(usdjpy.date, usdjpy.value,
    label="USD/JPY",
    color=:darkorange, lw=1,
    ylabel="JPY per USD",
    title="USD/JPY Spot Rate")
hline!(p4, [mean(usdjpy.value)], color=:gray, lw=0.8, ls=:dash,
    label="mean $(round(mean(usdjpy.value), digits=1))")

p5 = plot(ppi.date, ppi.value,
    label="PPI (all commodities)",
    color=:purple, lw=1.5,
    ylabel="Index",
    title="Producer Price Index")

boj_plot = plot(p1, p2, p3, p4, p5;
    layout=(3, 2),
    size=(1100, 900),
    left_margin=8Plots.mm,
    bottom_margin=6Plots.mm,
    top_margin=4Plots.mm,
    titlefontsize=10,
    legendfontsize=8,
    plot_title="Bank of Japan — Key Indicators (2000–present)",
    plot_titlefontsize=12)

out_path = joinpath(@__DIR__, "plots", "plot_boj.png")
savefig(boj_plot, out_path)
println("Plot saved → $out_path")

println("\n" * "=" ^ 60)
println("Done.")
println("=" ^ 60)

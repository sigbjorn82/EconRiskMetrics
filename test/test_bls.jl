"""
test_bls.jl

Dedicated test and plot for the BLS (Bureau of Labor Statistics) data source.
Uses registered API key for extended 20-year history (2000–present).

Series fetched:
  LNS14000000  — Unemployment Rate (seasonally adjusted, monthly %)
  CUUR0000SA0  — CPI-U All Items (seasonally adjusted, index 1982-84=100)
  CES0000000001 — Total Nonfarm Payrolls (thousands, seasonally adjusted)
  PRS85006092  — Nonfarm Business Labor Productivity (index, quarterly)

Run from test/ directory:
    julia --project=.. test_bls.jl
"""

using Pkg
Pkg.activate(joinpath(@__DIR__, ".."))

using EconRiskMetrics
using DataFrames
using Dates
using Plots

# Load API key
load_env(joinpath(@__DIR__, "..", ".env"))

println("=" ^ 60)
println("  BLS — Bureau of Labor Statistics")
println("=" ^ 60)

bls = BlsSource()
key_display = isempty(bls.api_key) ? "none (free tier, 3-year limit)" :
              bls.api_key[1:6] * "..." * " (registered, 20-year history)"
println("\nAPI key: $key_display")

# ─── Fetch series ──────────────────────────────────────────────────────────────

println("\nFetching Unemployment Rate (LNS14000000)...")
unemp = fetch_time_series(bls, "LNS14000000", start_date=Date(2000,1,1))
println("  $(nrow(unemp)) months  |  $(unemp.date[1]) → $(unemp.date[end])")
println("  Latest: $(unemp.value[end])%  |  Peak: $(maximum(unemp.value))%  |  Min: $(minimum(unemp.value))%")

println("\nFetching CPI-U All Items SA (CUUR0000SA0)...")
cpi = fetch_time_series(bls, "CUUR0000SA0", start_date=Date(2000,1,1))
println("  $(nrow(cpi)) months  |  $(cpi.date[1]) → $(cpi.date[end])")
println("  Latest: $(cpi.value[end])  |  2000 baseline: $(cpi.value[1])")

println("\nFetching Total Nonfarm Payrolls (CES0000000001)...")
payroll = fetch_time_series(bls, "CES0000000001", start_date=Date(2000,1,1))
println("  $(nrow(payroll)) months  |  $(payroll.date[1]) → $(payroll.date[end])")
println("  Latest: $(round(payroll.value[end]/1000, digits=2))M  |  COVID low: $(round(minimum(payroll.value)/1000, digits=2))M")

println("\nFetching Labor Productivity (PRS85006092, quarterly)...")
prod = fetch_time_series(bls, "PRS85006092", start_date=Date(2000,1,1))
println("  $(nrow(prod)) quarters  |  $(prod.date[1]) → $(prod.date[end])")
println("  Latest: $(prod.value[end])")

# ─── Plots ────────────────────────────────────────────────────────────────────

println("\nGenerating plots...")

# Compute year-over-year CPI inflation rate
cpi_yoy = let
    dates  = Date[]
    rates  = Float64[]
    for i in 13:nrow(cpi)
        push!(dates, cpi.date[i])
        push!(rates, (cpi.value[i] / cpi.value[i-12] - 1) * 100)
    end
    DataFrame(date=dates, value=rates)
end

p1 = plot(unemp.date, unemp.value,
    label="Unemployment Rate",
    color=:crimson, lw=1.5,
    ylabel="Percent (%)",
    title="Unemployment Rate",
    fillrange=0, fillalpha=0.15, fillcolor=:crimson)

p2 = plot(cpi_yoy.date, cpi_yoy.value,
    label="CPI-U YoY inflation",
    color=:darkorange, lw=1.5,
    ylabel="Year-over-Year (%)",
    title="CPI-U Inflation (YoY %)")
hline!(p2, [0], color=:black, lw=0.8, ls=:dash, label="")
hline!(p2, [2], color=:gray,  lw=0.8, ls=:dot,  label="2% target")

p3 = plot(payroll.date, payroll.value ./ 1000,
    label="Nonfarm Payrolls",
    color=:steelblue, lw=1.2,
    ylabel="Millions",
    title="Total Nonfarm Payrolls")

p4 = plot(prod.date, prod.value,
    label="Labor Productivity",
    color=:forestgreen, lw=1.5,
    marker=:circle, ms=2,
    ylabel="Index (2012=100)",
    title="Nonfarm Business Productivity")

bls_plot = plot(p1, p2, p3, p4;
    layout=(2, 2),
    size=(1100, 700),
    left_margin=8Plots.mm,
    bottom_margin=6Plots.mm,
    top_margin=4Plots.mm,
    titlefontsize=10,
    legendfontsize=8,
    plot_title="US Bureau of Labor Statistics — Key Indicators (2000–present)",
    plot_titlefontsize=12)

out_path = joinpath(@__DIR__, "plots", "plot_bls.png")
savefig(bls_plot, out_path)
println("Plot saved → $out_path")

println("\n" * "=" ^ 60)
println("Done.")
println("=" ^ 60)

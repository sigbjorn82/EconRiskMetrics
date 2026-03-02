"""
test_eia.jl

Test and plot for US Energy Information Administration (EIA) data source.
Requires a free API key from https://www.eia.gov/opendata/register.php.
Set EIA_API_KEY in .env.

Series fetched:
  petroleum/pri/spt:RWTC   — WTI crude oil spot price (daily)
  petroleum/pri/spt:RBRTE  — Brent crude oil spot price (daily)
  natural-gas/pri/sum:RNGWHHD — Henry Hub natural gas (weekly)

Run from test/ directory:
    julia --project=.. test_eia.jl
"""

using Pkg
Pkg.activate(joinpath(@__DIR__, ".."))

using EconRiskMetrics
using DataFrames
using Dates
using Plots

load_env(joinpath(@__DIR__, "..", ".env"))

println("=" ^ 60)
println("  US Energy Information Administration — EIA API v2")
println("=" ^ 60)

eia = EiaSource()
println("\nAPI key: $(isempty(eia.api_key) ? "NOT SET — set EIA_API_KEY in .env" : "configured")")

println("\nValidating connection...")
println(validate_connection(eia) ? "  ✓ Connected" : "  ✗ Connection failed (check EIA_API_KEY)")

# ─── Fetch series ──────────────────────────────────────────────────────────────

println("\nFetching WTI crude oil daily (petroleum/pri/spt:RWTC)...")
try
    wti = fetch_time_series(eia, "petroleum/pri/spt:RWTC",
                             start_date=Date(2015,1,1),
                             frequency="daily")
    println("  $(nrow(wti)) daily obs")
    if nrow(wti) > 0
        println("  $(wti.date[1]) → $(wti.date[end])")
        println("  Latest: \$$(wti.value[end])/bbl  |  Min: \$$(minimum(wti.value))  |  Max: \$$(maximum(wti.value))")
    end
    global wti_data = wti
catch e
    @warn "WTI fetch failed: $e"
    global wti_data = nothing
end

println("\nFetching Brent crude oil daily (petroleum/pri/spt:RBRTE)...")
try
    brent = fetch_time_series(eia, "petroleum/pri/spt:RBRTE",
                               start_date=Date(2015,1,1),
                               frequency="daily")
    println("  $(nrow(brent)) daily obs  |  Latest: \$$(nrow(brent) > 0 ? brent.value[end] : "n/a")/bbl")
    global brent_data = brent
catch e
    @warn "Brent fetch failed: $e"
    global brent_data = nothing
end

println("\nFetching Henry Hub natural gas weekly (natural-gas/pri/sum:RNGWHHD)...")
try
    hh = fetch_time_series(eia, "natural-gas/pri/sum:RNGWHHD",
                            start_date=Date(2015,1,1),
                            frequency="weekly")
    println("  $(nrow(hh)) weekly obs")
    if nrow(hh) > 0
        println("  $(hh.date[1]) → $(hh.date[end])")
        println("  Latest: \$$(hh.value[end])/MMBtu")
    end
    global hh_data = hh
catch e
    @warn "Henry Hub fetch failed: $e"
    global hh_data = nothing
end

println("\nListing curated series...")
series = list_available_series(eia)
for s in series
    println("  $s")
end

# ─── Plot ─────────────────────────────────────────────────────────────────────

println("\nGenerating plots...")
plots = []

if wti_data !== nothing && nrow(wti_data) > 0 &&
   brent_data !== nothing && nrow(brent_data) > 0
    p1 = plot(wti_data.date, wti_data.value,
        label="WTI (Cushing)", color=:steelblue, lw=0.8,
        ylabel="USD per barrel",
        title="Crude Oil Spot Prices")
    plot!(p1, brent_data.date, brent_data.value,
        label="Brent", color=:darkorange, lw=0.8)
    push!(plots, p1)
elseif wti_data !== nothing && nrow(wti_data) > 0
    p1 = plot(wti_data.date, wti_data.value,
        label="WTI", color=:steelblue, lw=0.8,
        ylabel="USD per barrel", title="WTI Crude Oil")
    push!(plots, p1)
end

if hh_data !== nothing && nrow(hh_data) > 0
    p2 = plot(hh_data.date, hh_data.value,
        label="Henry Hub",
        color=:firebrick, lw=0.8,
        ylabel="USD per MMBtu",
        title="Henry Hub Natural Gas (Weekly)")
    push!(plots, p2)
end

if !isempty(plots)
    n = length(plots)
    eia_plot = plot(plots...;
        layout=(n, 1),
        size=(1000, 380 * n),
        left_margin=8Plots.mm,
        bottom_margin=6Plots.mm,
        titlefontsize=10,
        legendfontsize=8,
        plot_title="US Energy Information Administration",
        plot_titlefontsize=12)

    out_path = joinpath(@__DIR__, "plots", "plot_eia.png")
    savefig(eia_plot, out_path)
    println("Plot saved → $out_path")
else
    println("No data fetched — check EIA_API_KEY in .env")
end

println("\n" * "=" ^ 60)
println("Done.")
println("=" ^ 60)

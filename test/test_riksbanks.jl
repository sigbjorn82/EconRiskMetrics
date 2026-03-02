"""
test_riksbanks.jl

Test and plot for Sveriges Riksbank (Sweden) data source.
No API key required.

Series fetched:
  crossrates/SEKEURPMI  — EUR/SEK daily mid-rate
  crossrates/SEKUSDPMI  — USD/SEK daily mid-rate
  interests/REPORATE    — Riksbank repo rate (policy rate)
  interests/STIBOR3M    — 3-month STIBOR

Run from test/ directory:
    julia --project=.. test_riksbanks.jl
"""

using Pkg
Pkg.activate(joinpath(@__DIR__, ".."))

using EconRiskMetrics
using DataFrames
using Dates
using Plots

load_env(joinpath(@__DIR__, "..", ".env"))

println("=" ^ 60)
println("  Sveriges Riksbank — SWEA v1 API")
println("=" ^ 60)

rb = RiksbanksSource()

println("\nValidating connection...")
println(validate_connection(rb) ? "  ✓ Connected" : "  ✗ Connection failed")

# ─── Fetch series ──────────────────────────────────────────────────────────────

println("\nFetching EUR/SEK daily (crossrates/SEKEURPMI)...")
try
    eursek = fetch_time_series(rb, "crossrates/SEKEURPMI",
                                start_date=Date(2010,1,1))
    println("  $(nrow(eursek)) daily obs")
    if nrow(eursek) > 0
        println("  $(eursek.date[1]) → $(eursek.date[end])")
        println("  Latest: $(eursek.value[end])  |  Min: $(minimum(eursek.value))  |  Max: $(maximum(eursek.value))")
    end
    global eursek_data = eursek
catch e
    @warn "EUR/SEK fetch failed: $e"
    global eursek_data = nothing
end

println("\nFetching USD/SEK daily (crossrates/SEKUSDPMI)...")
try
    usdsek = fetch_time_series(rb, "crossrates/SEKUSDPMI",
                                start_date=Date(2010,1,1))
    println("  $(nrow(usdsek)) daily obs  |  Latest: $(nrow(usdsek) > 0 ? usdsek.value[end] : "n/a")")
    global usdsek_data = usdsek
catch e
    @warn "USD/SEK fetch failed: $e"
    global usdsek_data = nothing
end

println("\nFetching Riksbank repo rate (interests/REPORATE)...")
try
    repo = fetch_time_series(rb, "interests/REPORATE",
                              start_date=Date(2000,1,1))
    println("  $(nrow(repo)) obs")
    if nrow(repo) > 0
        println("  $(repo.date[1]) → $(repo.date[end])")
        println("  Latest: $(repo.value[end])%")
    end
    global repo_data = repo
catch e
    @warn "Repo rate fetch failed: $e"
    global repo_data = nothing
end

println("\nFetching 3-month STIBOR (interests/STIBOR3M)...")
try
    stibor = fetch_time_series(rb, "interests/STIBOR3M",
                                start_date=Date(2010,1,1))
    println("  $(nrow(stibor)) obs  |  Latest: $(nrow(stibor) > 0 ? stibor.value[end] : "n/a")%")
    global stibor_data = stibor
catch e
    @warn "STIBOR fetch failed: $e"
    global stibor_data = nothing
end

println("\nListing available series (live from API)...")
series = list_available_series(rb)
println("  $(length(series)) series available")
for s in first(series, 8)
    println("  $s")
end
length(series) > 8 && println("  ...")

# ─── Plot ─────────────────────────────────────────────────────────────────────

println("\nGenerating plots...")
plots = []

if eursek_data !== nothing && nrow(eursek_data) > 0
    p1 = plot(eursek_data.date, eursek_data.value,
        label="EUR/SEK",
        color=:steelblue, lw=0.8,
        ylabel="SEK per EUR",
        title="EUR/SEK Daily Mid-Rate")
    push!(plots, p1)
end

if repo_data !== nothing && nrow(repo_data) > 0
    p2 = plot(repo_data.date, repo_data.value,
        label="Repo rate",
        color=:crimson, lw=1.5,
        ylabel="% per annum",
        title="Riksbank Repo Rate",
        fillrange=0, fillalpha=0.12, fillcolor=:crimson)
    push!(plots, p2)
end

if !isempty(plots)
    n = length(plots)
    rb_plot = plot(plots...;
        layout=(n, 1),
        size=(1000, 380 * n),
        left_margin=8Plots.mm,
        bottom_margin=6Plots.mm,
        titlefontsize=10,
        legendfontsize=8,
        plot_title="Sveriges Riksbank",
        plot_titlefontsize=12)

    out_path = joinpath(@__DIR__, "plots", "plot_riksbanks.png")
    savefig(rb_plot, out_path)
    println("Plot saved → $out_path")
else
    println("No data fetched — check connection")
end

println("\n" * "=" ^ 60)
println("Done.")
println("=" ^ 60)

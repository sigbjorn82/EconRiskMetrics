"""
test_oecd.jl

Dedicated test and plot for the OECD data source.
No API key required — public SDMX-JSON API.

Series fetched (Japan & Korea focus):
  QNA/JPN.B1_GE.VOBARSA.Q  — Japan real GDP (quarterly, volume, SA)
  QNA/KOR.B1_GE.VOBARSA.Q  — Korea real GDP (quarterly, volume, SA)
  PRICES_CPI/JPN.CPI010000.IXOB.M  — Japan CPI all items (monthly)
  PRICES_CPI/KOR.CPI010000.IXOB.M  — Korea CPI all items (monthly)
  STLABOUR/JPN.UNRTTE01.STSA.M     — Japan unemployment rate (monthly, SA)
  STLABOUR/KOR.UNRTTE01.STSA.M     — Korea unemployment rate (monthly, SA)
  MEI/JPN.IR3TIB01.ST.M            — Japan 3-month interbank rate (monthly)
  MEI/KOR.IR3TIB01.ST.M            — Korea 3-month interbank rate (monthly)

Run from test/ directory:
    julia --project=.. test_oecd.jl
"""

using Pkg
Pkg.activate(joinpath(@__DIR__, ".."))

using EconRiskMetrics
using DataFrames
using Dates
using Plots

println("=" ^ 60)
println("  OECD — Organisation for Economic Co-operation and Development")
println("=" ^ 60)

oecd = OECDSource()

# ─── Validate connection ───────────────────────────────────────────────────────
print("\nValidating connection... ")
println(validate_connection(oecd) ? "OK" : "FAILED")

# ─── Real GDP ─────────────────────────────────────────────────────────────────

println("\nFetching Japan real GDP (QNA/JPN.B1_GE.VOBARSA.Q)...")
gdp_jpn = fetch_time_series(oecd, "QNA/JPN.B1_GE.VOBARSA.Q", start_date=Date(2000,1,1))
println("  $(nrow(gdp_jpn)) quarters  |  $(gdp_jpn.date[1]) → $(gdp_jpn.date[end])")
println("  Latest: $(round(gdp_jpn.value[end]/1e6, digits=2)) trillion JPY")

println("\nFetching Korea real GDP (QNA/KOR.B1_GE.VOBARSA.Q)...")
gdp_kor = fetch_time_series(oecd, "QNA/KOR.B1_GE.VOBARSA.Q", start_date=Date(2000,1,1))
println("  $(nrow(gdp_kor)) quarters  |  $(gdp_kor.date[1]) → $(gdp_kor.date[end])")

# ─── CPI ──────────────────────────────────────────────────────────────────────

println("\nFetching Japan CPI (PRICES_CPI/JPN.CPI010000.IXOB.M)...")
cpi_jpn = fetch_time_series(oecd, "PRICES_CPI/JPN.CPI010000.IXOB.M", start_date=Date(2000,1,1))
println("  $(nrow(cpi_jpn)) months  |  $(cpi_jpn.date[1]) → $(cpi_jpn.date[end])")
println("  Latest: $(cpi_jpn.value[end])  |  2000 baseline: $(cpi_jpn.value[1])")

println("\nFetching Korea CPI (PRICES_CPI/KOR.CPI010000.IXOB.M)...")
cpi_kor = fetch_time_series(oecd, "PRICES_CPI/KOR.CPI010000.IXOB.M", start_date=Date(2000,1,1))
println("  $(nrow(cpi_kor)) months  |  $(cpi_kor.date[1]) → $(cpi_kor.date[end])")

# ─── Unemployment ─────────────────────────────────────────────────────────────

println("\nFetching Japan unemployment (STLABOUR/JPN.UNRTTE01.STSA.M)...")
unemp_jpn = fetch_time_series(oecd, "STLABOUR/JPN.UNRTTE01.STSA.M", start_date=Date(2000,1,1))
println("  $(nrow(unemp_jpn)) months  |  $(unemp_jpn.date[1]) → $(unemp_jpn.date[end])")
println("  Latest: $(unemp_jpn.value[end])%  |  Peak: $(maximum(unemp_jpn.value))%")

println("\nFetching Korea unemployment (STLABOUR/KOR.UNRTTE01.STSA.M)...")
unemp_kor = fetch_time_series(oecd, "STLABOUR/KOR.UNRTTE01.STSA.M", start_date=Date(2000,1,1))
println("  $(nrow(unemp_kor)) months  |  $(unemp_kor.date[1]) → $(unemp_kor.date[end])")
println("  Latest: $(unemp_kor.value[end])%  |  Peak: $(maximum(unemp_kor.value))%")

# ─── Interbank rates ──────────────────────────────────────────────────────────

println("\nFetching Japan 3-month interbank rate (MEI/JPN.IR3TIB01.ST.M)...")
rate_jpn = fetch_time_series(oecd, "MEI/JPN.IR3TIB01.ST.M", start_date=Date(2000,1,1))
println("  $(nrow(rate_jpn)) months  |  $(rate_jpn.date[1]) → $(rate_jpn.date[end])")

println("\nFetching Korea 3-month interbank rate (MEI/KOR.IR3TIB01.ST.M)...")
rate_kor = fetch_time_series(oecd, "MEI/KOR.IR3TIB01.ST.M", start_date=Date(2000,1,1))
println("  $(nrow(rate_kor)) months  |  $(rate_kor.date[1]) → $(rate_kor.date[end])")

# ─── Plots ────────────────────────────────────────────────────────────────────

println("\nGenerating plots...")

# Normalise GDP to 2010 Q1 = 100 for cross-country comparison
function norm100(df)
    base_idx = findfirst(d -> d >= Date(2010, 1, 1), df.date)
    base_idx === nothing && (base_idx = 1)
    base = df.value[base_idx]
    return df.value ./ base .* 100
end

p1 = plot(gdp_jpn.date, norm100(gdp_jpn),
    label="Japan",  color=:crimson,    lw=1.5,
    ylabel="Index (2010 Q1 = 100)",
    title="Real GDP (volume, SA)")
plot!(p1, gdp_kor.date, norm100(gdp_kor),
    label="Korea",  color=:steelblue,  lw=1.5)

p2 = plot(cpi_jpn.date, cpi_jpn.value,
    label="Japan",  color=:crimson,   lw=1.3,
    ylabel="Index",
    title="CPI — All Items")
plot!(p2, cpi_kor.date, cpi_kor.value,
    label="Korea",  color=:steelblue, lw=1.3)

p3 = plot(unemp_jpn.date, unemp_jpn.value,
    label="Japan",  color=:crimson,   lw=1.3,
    ylabel="Percent (%)",
    title="Unemployment Rate (SA)")
plot!(p3, unemp_kor.date, unemp_kor.value,
    label="Korea",  color=:steelblue, lw=1.3)

p4 = plot(rate_jpn.date, rate_jpn.value,
    label="Japan",  color=:crimson,   lw=1.3,
    ylabel="Percent (%)",
    title="3-Month Interbank Rate")
plot!(p4, rate_kor.date, rate_kor.value,
    label="Korea",  color=:steelblue, lw=1.3)
hline!(p4, [0], color=:black, lw=0.8, ls=:dash, label="")

oecd_plot = plot(p1, p2, p3, p4;
    layout=(2, 2),
    size=(1100, 700),
    left_margin=8Plots.mm,
    bottom_margin=6Plots.mm,
    top_margin=4Plots.mm,
    titlefontsize=10,
    legendfontsize=8,
    plot_title="OECD — Japan & Korea Key Indicators (2000–present)",
    plot_titlefontsize=12)

out_path = joinpath(@__DIR__, "plots", "plot_oecd.png")
savefig(oecd_plot, out_path)
println("Plot saved → $out_path")

println("\n" * "=" ^ 60)
println("Done.")
println("=" ^ 60)

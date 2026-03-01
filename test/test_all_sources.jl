"""
test_all_sources.jl

Comprehensive verification script for all EconRiskMetrics data sources.
Fetches representative series from each source, prints stats, and saves plots.

Run from test/ directory:
    julia --project=.. test_all_sources.jl
"""

using Pkg
Pkg.activate(joinpath(@__DIR__, ".."))

using EconRiskMetrics
using DataFrames
using Dates
using Plots

# ─── helpers ──────────────────────────────────────────────────────────────────

"""Load .env file from project root (handles paths with spaces)."""
function load_dotenv(path::String)
    isfile(path) || return
    for line in eachline(path)
        line = strip(line)
        isempty(line) || startswith(line, '#') && continue
        m = match(r"^([A-Za-z_][A-Za-z0-9_]*)=(.*)$", line)
        m === nothing && continue
        ENV[m.captures[1]] = m.captures[2]
    end
end

load_dotenv(joinpath(@__DIR__, "..", ".env"))

const PLOTS_DIR = joinpath(@__DIR__, "plots")

"""Print a summary table row for a fetched DataFrame."""
function print_stats(name::String, df::DataFrame)
    valid = filter(!isnan, df.value)
    mn  = isempty(valid) ? NaN : round(minimum(valid), digits=3)
    mx  = isempty(valid) ? NaN : round(maximum(valid), digits=3)
    lst = round(df.value[end], digits=3)
    println("  ✓ $name")
    println("    Rows : $(nrow(df))")
    println("    Range: $(df.date[1]) → $(df.date[end])")
    println("    Value: min=$(mn)  max=$(mx)  last=$(lst)")
end

plots = Plots.Plot[]  # collect subplots for the combined figure
results = Dict{String, Union{DataFrame, String}}()

# ─── 1. FRED ─────────────────────────────────────────────────────────────────
println("\n══════════════════════════════════")
println(" 1. FRED (Federal Reserve St. Louis)")
println("══════════════════════════════════")
try
    fred = FredSource()
    gdp  = fetch_time_series(fred, "GDPC1",     start_date=Date(2000,1,1))
    unem = fetch_time_series(fred, "UNRATE",    start_date=Date(2000,1,1))
    sp   = fetch_time_series(fred, "SP500",     start_date=Date(2015,1,1))
    fedf = fetch_time_series(fred, "FEDFUNDS",  start_date=Date(2000,1,1))

    print_stats("Real GDP (GDPC1)",          gdp)
    print_stats("Unemployment (UNRATE)",     unem)
    print_stats("S&P 500 (SP500)",           sp)
    print_stats("Fed Funds Rate (FEDFUNDS)", fedf)

    p1 = plot(gdp.date,  gdp.value,  label="Real GDP (2012 \$B)", color=:steelblue,  lw=1.5)
    p2 = plot(unem.date, unem.value, label="Unemployment %",       color=:crimson,    lw=1.5)
    p3 = plot(sp.date,   sp.value,   label="S&P 500",              color=:forestgreen, lw=1)
    p4 = plot(fedf.date, fedf.value, label="Fed Funds Rate %",     color=:darkorange, lw=1.5)

    fred_plot = plot(p1, p2, p3, p4;
        layout=4, title=["Real GDP" "Unemployment" "S&P 500" "Fed Funds"],
        size=(1000, 600), left_margin=5Plots.mm, bottom_margin=5Plots.mm,
        titlefontsize=9, legendfontsize=7)
    savefig(fred_plot, joinpath(PLOTS_DIR, "plot_fred.png"))
    println("  → Saved plots/plot_fred.png")
    results["FRED"] = gdp
catch e
    println("  ✗ FRED error: $e")
    results["FRED"] = string(e)
end

# ─── 2. Yahoo Finance (YFinance) ─────────────────────────────────────────────
println("\n══════════════════════════════════")
println(" 2. Yahoo Finance (YFinance.jl)")
println("══════════════════════════════════")
try
    # Use fetch_data (range="max") — fetch_time_series with startdt is flaky on Yahoo Finance
    yf   = YFinanceSource()
    aapl = fetch_data(yf, "AAPL")
    spy  = fetch_data(yf, "SPY")
    vix  = fetch_data(yf, "^VIX")
    btc  = fetch_data(yf, "BTC-USD")

    print_stats("Apple (AAPL adjclose)",    aapl)
    print_stats("S&P 500 ETF (SPY)",        spy)
    print_stats("VIX (^VIX)",               vix)
    print_stats("Bitcoin (BTC-USD)",        btc)

    p1 = plot(aapl.date, aapl.value, label="AAPL",    color=:steelblue,   lw=1)
    p2 = plot(spy.date,  spy.value,  label="SPY",     color=:forestgreen, lw=1)
    p3 = plot(vix.date,  vix.value,  label="VIX",     color=:crimson,     lw=1)
    p4 = plot(btc.date,  btc.value,  label="BTC-USD", color=:darkorange,  lw=1)

    yf_plot = plot(p1, p2, p3, p4;
        layout=4, title=["Apple" "S&P 500 ETF" "VIX" "Bitcoin"],
        size=(1000, 600), left_margin=5Plots.mm, bottom_margin=5Plots.mm,
        titlefontsize=9, legendfontsize=7)
    savefig(yf_plot, joinpath(PLOTS_DIR, "plot_yfinance.png"))
    println("  → Saved plots/plot_yfinance.png")
    results["YFinance"] = aapl
catch e
    println("  ✗ YFinance error: $e")
    results["YFinance"] = string(e)
end

# ─── 3. Alpha Vantage ────────────────────────────────────────────────────────
println("\n══════════════════════════════════")
println(" 3. Alpha Vantage")
println("══════════════════════════════════")
try
    av   = AlphaVantageSource()
    msft = fetch_data(av, "MSFT")
    sleep(15)  # free tier: 1 req/sec burst limit
    ibm  = fetch_data(av, "IBM")

    print_stats("Microsoft (MSFT close)", msft)
    print_stats("IBM close",              ibm)

    p1 = plot(msft.date, msft.value, label="MSFT", color=:steelblue,   lw=1)
    p2 = plot(ibm.date,  ibm.value,  label="IBM",  color=:forestgreen, lw=1)

    av_plot = plot(p1, p2;
        layout=2, title=["Microsoft (MSFT)" "IBM"],
        size=(900, 350), left_margin=5Plots.mm, bottom_margin=5Plots.mm,
        titlefontsize=9, legendfontsize=7)
    savefig(av_plot, joinpath(PLOTS_DIR, "plot_alphavantage.png"))
    println("  → Saved plots/plot_alphavantage.png")
    results["AlphaVantage"] = msft
catch e
    println("  ✗ Alpha Vantage error: $e")
    results["AlphaVantage"] = string(e)
end

# ─── 4. World Bank ───────────────────────────────────────────────────────────
println("\n══════════════════════════════════")
println(" 4. World Bank (WorldBankData.jl)")
println("══════════════════════════════════")
try
    wb    = WorldBankSource()
    pop   = fetch_data(wb, "SP.POP.TOTL")       # Total population, US
    gdppc = fetch_data(wb, "NY.GDP.PCAP.CD")    # GDP per capita, current USD
    wb_de = WorldBankSource(country="DE")
    gdp_de = fetch_data(wb_de, "NY.GDP.MKTP.CD")  # Germany GDP

    print_stats("US Total Population",          pop)
    print_stats("US GDP per Capita (USD)",      gdppc)
    print_stats("Germany GDP (current USD)",    gdp_de)

    p1 = plot(pop.date,    pop.value ./ 1e6,    label="US Pop (M)",     color=:steelblue,   lw=1.5, marker=:circle, ms=3)
    p2 = plot(gdppc.date,  gdppc.value,         label="US GDP/capita",  color=:forestgreen, lw=1.5, marker=:circle, ms=3)
    p3 = plot(gdp_de.date, gdp_de.value ./ 1e12, label="DE GDP (T USD)", color=:crimson,    lw=1.5, marker=:circle, ms=3)

    wb_plot = plot(p1, p2, p3;
        layout=3, title=["US Population" "US GDP per Capita" "Germany GDP"],
        size=(1000, 350), left_margin=5Plots.mm, bottom_margin=5Plots.mm,
        titlefontsize=9, legendfontsize=7)
    savefig(wb_plot, joinpath(PLOTS_DIR, "plot_worldbank.png"))
    println("  → Saved plots/plot_worldbank.png")
    results["WorldBank"] = pop
catch e
    println("  ✗ World Bank error: $e")
    results["WorldBank"] = string(e)
end

# ─── 5. IMF ──────────────────────────────────────────────────────────────────
println("\n══════════════════════════════════")
println(" 5. IMF (IMFData.jl)")
println("══════════════════════════════════")
try
    imf = IMFSource()
    cpi = fetch_time_series(imf, "PCPI_IX", start_date=Date(2000,1,1))
    print_stats("US CPI Index (PCPI_IX)", cpi)

    imf_gb = IMFSource(area="GB", frequency="Q")
    gdp_gb = fetch_time_series(imf_gb, "NGDP_R", start_date=Date(2000,1,1))
    print_stats("UK Real GDP (NGDP_R, quarterly)", gdp_gb)

    p1 = plot(cpi.date,   cpi.value,   label="US CPI",     color=:steelblue,   lw=1.5)
    p2 = plot(gdp_gb.date, gdp_gb.value, label="UK Real GDP", color=:forestgreen, lw=1.5)

    imf_plot = plot(p1, p2;
        layout=2, title=["US CPI (IMF IFS)" "UK Real GDP (IMF IFS)"],
        size=(900, 350), left_margin=5Plots.mm, bottom_margin=5Plots.mm,
        titlefontsize=9, legendfontsize=7)
    savefig(imf_plot, joinpath(PLOTS_DIR, "plot_imf.png"))
    println("  → Saved plots/plot_imf.png")
    results["IMF"] = cpi
catch e
    println("  ✗ IMF error (server may be temporarily down): $e")
    results["IMF"] = string(e)
end

# ─── 6. Bank of England ──────────────────────────────────────────────────────
println("\n══════════════════════════════════")
println(" 6. Bank of England (BoE IADB)")
println("══════════════════════════════════")
try
    boe      = BankOfEnglandSource()
    bank_rate = fetch_time_series(boe, "IUDBEDR", start_date=Date(2010,1,1))
    gbpusd   = fetch_time_series(boe, "XUMAUSS",  start_date=Date(2010,1,1))

    print_stats("BoE Bank Rate (IUDBEDR)",   bank_rate)
    print_stats("GBP/USD spot (XUMAUSS)",    gbpusd)

    p1 = plot(bank_rate.date, bank_rate.value, label="Bank Rate %",  color=:steelblue,   lw=1.5)
    p2 = plot(gbpusd.date,    gbpusd.value,    label="GBP/USD",      color=:forestgreen, lw=1)

    boe_plot = plot(p1, p2;
        layout=2, title=["BoE Bank Rate" "GBP/USD Spot"],
        size=(900, 350), left_margin=5Plots.mm, bottom_margin=5Plots.mm,
        titlefontsize=9, legendfontsize=7)
    savefig(boe_plot, joinpath(PLOTS_DIR, "plot_boe.png"))
    println("  → Saved plots/plot_boe.png")
    results["BoE"] = bank_rate
catch e
    println("  ✗ Bank of England error: $e")
    results["BoE"] = string(e)
end

# ─── 7. ECB ──────────────────────────────────────────────────────────────────
println("\n══════════════════════════════════")
println(" 7. ECB (European Central Bank)")
println("══════════════════════════════════")
try
    ecb    = ECBSource()
    eurusd = fetch_time_series(ecb, "EXR/D.USD.EUR.SP00.A", start_date=Date(2015,1,1))
    eurgbp = fetch_time_series(ecb, "EXR/D.GBP.EUR.SP00.A", start_date=Date(2015,1,1))
    hicp   = fetch_time_series(ecb, "ICP/M.U2.N.000000.4.ANR", start_date=Date(2005,1,1))

    print_stats("EUR/USD daily (ECB)",           eurusd)
    print_stats("EUR/GBP daily (ECB)",           eurgbp)
    print_stats("Euro area HICP annual rate (%)", hicp)

    p1 = plot(eurusd.date, eurusd.value, label="EUR/USD", color=:steelblue,   lw=1)
    p2 = plot(eurgbp.date, eurgbp.value, label="EUR/GBP", color=:forestgreen, lw=1)
    p3 = plot(hicp.date,   hicp.value,   label="HICP %",  color=:crimson,     lw=1.5)

    ecb_plot = plot(p1, p2, p3;
        layout=3, title=["EUR/USD" "EUR/GBP" "Euro Area HICP"],
        size=(1000, 350), left_margin=5Plots.mm, bottom_margin=5Plots.mm,
        titlefontsize=9, legendfontsize=7)
    savefig(ecb_plot, joinpath(PLOTS_DIR, "plot_ecb.png"))
    println("  → Saved plots/plot_ecb.png")
    results["ECB"] = eurusd
catch e
    println("  ✗ ECB error: $e")
    results["ECB"] = string(e)
end

# ─── 8. BLS ──────────────────────────────────────────────────────────────────
println("\n══════════════════════════════════")
println(" 8. BLS (Bureau of Labor Statistics)")
println("══════════════════════════════════")
try
    # With API key: up to 500 req/day and 20-year window
    bls   = BlsSource()
    unemp   = fetch_time_series(bls, "LNS14000000",   start_date=Date(2000,1,1))
    cpi     = fetch_time_series(bls, "CUUR0000SA0",   start_date=Date(2000,1,1))
    payroll = fetch_time_series(bls, "CES0000000001", start_date=Date(2000,1,1))

    print_stats("Unemployment Rate % (LNS14000000)",       unemp)
    print_stats("CPI-U All Items SA (CUUR0000SA0)",        cpi)
    print_stats("Total Nonfarm Payrolls (CES0000000001)",  payroll)

    p1 = plot(unemp.date,   unemp.value,        label="Unemployment %", color=:crimson,     lw=1.5)
    p2 = plot(cpi.date,     cpi.value,          label="CPI-U",          color=:steelblue,   lw=1.5)
    p3 = plot(payroll.date, payroll.value./1000, label="Payrolls (M)",  color=:forestgreen, lw=1)

    bls_plot = plot(p1, p2, p3;
        layout=3, title=["Unemployment Rate" "CPI-U (SA)" "Nonfarm Payrolls"],
        size=(1000, 350), left_margin=5Plots.mm, bottom_margin=5Plots.mm,
        titlefontsize=9, legendfontsize=7)
    savefig(bls_plot, joinpath(PLOTS_DIR, "plot_bls.png"))
    println("  → Saved plots/plot_bls.png")
    results["BLS"] = unemp
catch e
    println("  ✗ BLS error: $e")
    results["BLS"] = string(e)
end

# ─── 9. Eurostat ─────────────────────────────────────────────────────────────
println("\n══════════════════════════════════")
println(" 9. Eurostat")
println("══════════════════════════════════")
try
    es    = EurostatSource()
    hicp  = fetch_data(es, "prc_hicp_midx"; geo="EU27_2020", coicop="CP00", unit="I15")
    unemp = fetch_data(es, "une_rt_m";
                       geo="EU27_2020", age="TOTAL", sex="T", unit="PC_ACT", s_adj="SA")

    print_stats("EU27 HICP index 2015=100 (monthly)",    hicp)
    print_stats("EU27 Unemployment Rate % (monthly SA)", unemp)

    # Germany GDP annual (separate call to avoid linting kwargs issue)
    gdp_de = fetch_data(es, "nama_10_gdp"; geo="DE", na_item="B1GQ", unit="CP_MEUR")
    print_stats("Germany GDP current prices (M EUR, annual)", gdp_de)

    p1 = plot(hicp.date,   hicp.value,          label="HICP 2015=100", color=:steelblue,   lw=1.5)
    p2 = plot(unemp.date,  unemp.value,         label="Unemp %",       color=:crimson,     lw=1.5)
    p3 = plot(gdp_de.date, gdp_de.value ./ 1e6, label="DE GDP (B EUR)", color=:forestgreen, lw=1.5, marker=:circle, ms=3)

    es_plot = plot(p1, p2, p3;
        layout=3, title=["EU27 HICP" "EU27 Unemployment" "Germany GDP"],
        size=(1000, 350), left_margin=5Plots.mm, bottom_margin=5Plots.mm,
        titlefontsize=9, legendfontsize=7)
    savefig(es_plot, joinpath(PLOTS_DIR, "plot_eurostat.png"))
    println("  → Saved plots/plot_eurostat.png")
    results["Eurostat"] = hicp
catch e
    println("  ✗ Eurostat error: $e")
    results["Eurostat"] = string(e)
end

# ─── Summary ──────────────────────────────────────────────────────────────────
println("\n══════════════════════════════════")
println(" Summary")
println("══════════════════════════════════")
for (src, res) in sort(collect(results); by=first)
    if res isa DataFrame
        println("  ✓ $(rpad(src, 14)) — $(nrow(res)) rows  $(res.date[1]) → $(res.date[end])")
    else
        short = length(res) > 80 ? res[1:80] * "…" : res
        println("  ✗ $(rpad(src, 14)) — $short")
    end
end
println()

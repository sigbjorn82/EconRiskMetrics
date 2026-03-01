using Pkg
Pkg.activate(joinpath(@__DIR__, ".."))

using EconRiskMetrics
using DataFrames
using Dates
using Plots

# Load API key from .env
load_env(joinpath(@__DIR__, "..", ".env"))

# Create FRED connection
fred = FredSource()

# Fetch S&P 500 data from 2007 to present
println("Fetching S&P 500 data (2007-2026)...")
sp500 = fetch_time_series(fred, "SP500",
    start_date=Date(2007, 1, 1),
    end_date=Date(2026, 12, 31))

println("Fetched $(nrow(sp500)) data points")
println("Date range: $(sp500.date[1]) to $(sp500.date[end])")
println("Latest value: \$$(round(sp500.value[end], digits=2))")

# Filter out NaN values for clean plotting
valid = .!isnan.(sp500.value)
sp500_clean = sp500[valid, :]

# Create plot
plot(sp500_clean.date, sp500_clean.value,
    title = "S&P 500 Index (2007–2026)",
    xlabel = "Date",
    ylabel = "Index Value (USD)",
    label = "S&P 500",
    linewidth = 1.5,
    color = :blue,
    legend = :topleft,
    size = (1000, 500),
    dpi = 150,
    grid = true,
    margin = 5Plots.mm)

# Save
savefig(joinpath(@__DIR__, "plots", "sp500_plot.png"))
println("\nPlot saved to test/plots/sp500_plot.png")

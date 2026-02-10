#!/usr/bin/env julia

"""
Test script for FredSource implementation
Run with: julia --project=. test_fred.jl
"""

using Pkg
Pkg.activate(".")

# Load the EconRiskMetrics package
using EconRiskMetrics

# Load environment variables from .env file
load_env(joinpath(@__DIR__, ".env"))
using DataFrames
using Dates

println("=" ^ 60)
println("Testing FredSource Implementation")
println("=" ^ 60)

# Create FredSource instance (uses ENV["FRED_API_KEY"])
println("\n1. Creating FredSource instance...")
try
    fred = FredSource()
    println("✓ FredSource created successfully")
catch e
    println("✗ Error creating FredSource: $e")
    exit(1)
end

# Test connection
println("\n2. Validating FRED connection...")
fred = FredSource()
if validate_connection(fred)
    println("✓ Connection to FRED API successful")
else
    println("✗ Connection failed")
    exit(1)
end

# Test fetching data
println("\n3. Fetching Real GDP data (GDPC1)...")
try
    gdp_data = fetch_data(fred, "GDPC1")
    println("✓ Fetched $(nrow(gdp_data)) data points")
    println("   Date range: $(minimum(gdp_data.date)) to $(maximum(gdp_data.date))")
    println("   Latest value: \$(gdp_data.value[end]) billion")
catch e
    println("✗ Error fetching data: $e")
end

# Test time series with date range
println("\n4. Fetching recent GDP data (2020-2024)...")
try
    recent_gdp = fetch_time_series(
        fred, "GDPC1",
        start_date=Date(2020, 1, 1),
        end_date=Date(2024, 12, 31)
    )
    println("✓ Fetched $(nrow(recent_gdp)) data points")
catch e
    println("✗ Error: $e")
end

# Test metadata
println("\n5. Fetching metadata for GDPC1...")
try
    metadata = get_metadata(fred, "GDPC1")
    println("✓ Title: $(metadata["title"])")
    println("   Units: $(metadata["units"])")
    println("   Frequency: $(metadata["frequency"])")
catch e
    println("✗ Error: $e")
end

# Test asset type support
println("\n6. Testing asset type support...")
println("   Economic indicators: $(supports_asset_type(fred, :economic_indicator))")
println("   Indices: $(supports_asset_type(fred, :index))")
println("   Options: $(supports_asset_type(fred, :options))")

# Test unemployment rate
println("\n7. Fetching Unemployment Rate (UNRATE)...")
try
    unrate = fetch_data(fred, "UNRATE")
    println("✓ Current unemployment rate: $(unrate.value[end])%")
catch e
    println("✗ Error: $e")
end

println("\n" * "=" ^ 60)
println("All tests completed!")
println("=" ^ 60)

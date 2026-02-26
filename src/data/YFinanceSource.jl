"""
    YFinanceSource.jl

Concrete implementation of DataSource for Yahoo Finance market data.
Wraps YFinance.jl to match the DataSource interface.
No API key required.
"""

using YFinance
using DataFrames
using Dates

"""
    YFinanceSource <: DataSource

Data source for Yahoo Finance market data (stocks, ETFs, indices, forex, crypto).
No API key required.

# Constructor
    YFinanceSource()

# Example
```julia
yf = YFinanceSource()
aapl = fetch_data(yf, "AAPL")
spy  = fetch_time_series(yf, "SPY", start_date=Date(2020,1,1))
```

# Notes
- Uses adjusted close prices by default (`price_column=:adjclose`)
- Valid `price_column` values: `:adjclose`, `:open`, `:high`, `:low`, `:close`
- Valid intervals: "1d" (default), "1wk", "1mo"
- Covers stocks, ETFs, indices (^GSPC, ^VIX), forex (EURUSD=X), crypto (BTC-USD)
"""
struct YFinanceSource <: DataSource end

function fetch_data(source::YFinanceSource, identifier::String;
                    price_column::Symbol=:adjclose,
                    interval::String="1d", kwargs...)
    try
        result = YFinance.get_prices(identifier; interval=interval, range="max")
        df = DataFrame(result)
        isempty(df) && error("No data returned for '$identifier'")

        (price_column in propertynames(df)) ||
            error("Column '$price_column' not found. Valid: adjclose, open, high, low, close")

        out = DataFrame(
            date  = Date.(df.timestamp),
            value = Float64.(df[!, price_column]),
        )
        sort!(out, :date)
        return out
    catch e
        throw(DataSourceError("YFinanceSource", "Failed to fetch '$identifier': $(e)"))
    end
end

function fetch_time_series(source::YFinanceSource, identifier::String;
                           start_date::Union{Date,Nothing}=nothing,
                           end_date::Union{Date,Nothing}=nothing,
                           price_column::Symbol=:adjclose,
                           interval::String="1d", kwargs...)
    try
        # YFinance requires both startdt and enddt if either is provided
        if start_date !== nothing || end_date !== nothing
            startdt = start_date !== nothing ? string(start_date) : "1970-01-01"
            enddt   = end_date   !== nothing ? string(end_date)   : string(today())
            result = YFinance.get_prices(identifier; startdt=startdt, enddt=enddt, interval=interval)
        else
            result = YFinance.get_prices(identifier; interval=interval, range="max")
        end
        df = DataFrame(result)
        isempty(df) && error("No data returned for '$identifier'")

        (price_column in propertynames(df)) ||
            error("Column '$price_column' not found. Valid: adjclose, open, high, low, close")

        out = DataFrame(
            date  = Date.(df.timestamp),
            value = Float64.(df[!, price_column]),
        )
        sort!(out, :date)
        return out
    catch e
        throw(DataSourceError("YFinanceSource",
            "Failed to fetch time series '$identifier' ($(start_date) to $(end_date)): $(e)"))
    end
end

function validate_connection(source::YFinanceSource)
    try
        result = YFinance.get_prices("AAPL"; range="5d")
        return !isempty(result)
    catch
        return false
    end
end

function supports_asset_type(source::YFinanceSource, asset_type::Symbol)
    return asset_type in [:equity, :etf, :index, :forex, :crypto, :futures]
end

function get_metadata(source::YFinanceSource, identifier::String)
    try
        info = YFinance.get_quoteSummary(identifier, item="assetProfile")
        return Dict{String,Any}(
            "id"     => identifier,
            "source" => "Yahoo Finance",
            "info"   => info,
        )
    catch
        return Dict{String,Any}("id" => identifier, "source" => "Yahoo Finance")
    end
end

function list_available_series(source::YFinanceSource; kwargs...)
    @warn "Yahoo Finance uses standard ticker symbols. Search at: https://finance.yahoo.com"
    return [
        "AAPL",     # Apple
        "MSFT",     # Microsoft
        "SPY",      # S&P 500 ETF
        "QQQ",      # Nasdaq 100 ETF
        "^GSPC",    # S&P 500 Index
        "^DJI",     # Dow Jones Industrial Average
        "^VIX",     # CBOE Volatility Index
        "EURUSD=X", # EUR/USD forex
        "BTC-USD",  # Bitcoin / USD
        "GC=F",     # Gold futures
        "CL=F",     # Crude oil futures (WTI)
    ]
end

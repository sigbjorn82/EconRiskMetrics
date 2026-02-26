"""
    AlphaVantageSource.jl

Concrete implementation of DataSource for Alpha Vantage market data.
Uses direct HTTP calls due to a bug in AlphaVantage.jl's response parser.
"""

using HTTP
using JSON3
using Dates
using DataFrames

"""
    AlphaVantageSource <: DataSource

Data source for Alpha Vantage market data (stocks, forex, crypto).

# Fields
- `api_key::String`: Alpha Vantage API key

# Constructor
    AlphaVantageSource(api_key::Union{AbstractString,Nothing}=nothing)

# Example
```julia
av = AlphaVantageSource()
aapl = fetch_data(av, "AAPL")
msft = fetch_time_series(av, "MSFT", start_date=Date(2020,1,1))
```

# API Key
Get your free key at: https://www.alphavantage.co/support/#api-key
"""
struct AlphaVantageSource <: DataSource
    api_key::String
end

function AlphaVantageSource(api_key::Union{AbstractString,Nothing}=nothing)
    key = if api_key !== nothing
        String(api_key)
    else
        k = get(ENV, "ALPHA_VANTAGE_API_KEY", nothing)
        k === nothing ? get(ENV, "ALPHA_VANTAGE_KEY", nothing) : k
    end

    if key === nothing || isempty(key)
        throw(DataSourceError("AlphaVantageSource",
            "No API key provided. Set ALPHA_VANTAGE_API_KEY environment variable or pass to constructor. " *
            "Get your free key at: https://www.alphavantage.co/support/#api-key"))
    end

    return AlphaVantageSource(key)
end

"""Fetch JSON data from Alpha Vantage API."""
function _av_request(source::AlphaVantageSource, params::Dict{String,String})
    params["apikey"] = source.api_key
    response = HTTP.request("GET", "https://www.alphavantage.co/query", []; query=params)
    body = String(copy(response.body))
    json = JSON3.read(body)

    # Check for API errors
    if haskey(json, :Note)
        error("Alpha Vantage API limit exceeded. Free tier: 25 requests/day.")
    end
    if haskey(json, :Information)
        error(string(json.Information))
    end

    return json
end

function fetch_data(source::AlphaVantageSource, identifier::String;
                    price_column::Symbol=:close, outputsize::String="compact", kwargs...)
    try
        params = Dict{String,String}(
            "function"   => "TIME_SERIES_DAILY",
            "symbol"     => identifier,
            "outputsize" => outputsize,
        )
        json = _av_request(source, params)

        # Find the time series key (e.g., "Time Series (Daily)")
        ts_key = nothing
        for k in keys(json)
            if occursin("Time Series", string(k))
                ts_key = k
                break
            end
        end
        ts_key === nothing && error("No time series data found in response")

        ts_data = json[ts_key]
        return _parse_av_timeseries(ts_data, price_column)
    catch e
        throw(DataSourceError("AlphaVantageSource", "Failed to fetch '$identifier': $(e)"))
    end
end

function fetch_time_series(source::AlphaVantageSource, identifier::String;
                           start_date::Union{Date,Nothing}=nothing,
                           end_date::Union{Date,Nothing}=nothing,
                           price_column::Symbol=:close, kwargs...)
    try
        df = fetch_data(source, identifier; price_column=price_column, outputsize="compact")

        if start_date !== nothing
            df = df[df.date .>= start_date, :]
        end
        if end_date !== nothing
            df = df[df.date .<= end_date, :]
        end

        return df
    catch e
        throw(DataSourceError("AlphaVantageSource",
            "Failed to fetch time series '$identifier' ($(start_date) to $(end_date)): $(e)"))
    end
end

"""Parse Alpha Vantage JSON time series into standardized DataFrame."""
function _parse_av_timeseries(ts_data, price_column::Symbol)
    col_map = Dict(
        :open   => "1. open",
        :high   => "2. high",
        :low    => "3. low",
        :close  => "4. close",
        :volume => "5. volume",
    )
    col_key = get(col_map, price_column, "4. close")

    dates = Date[]
    values = Float64[]

    for (date_str, ohlcv) in pairs(ts_data)
        push!(dates, Date(string(date_str), "yyyy-mm-dd"))
        push!(values, parse(Float64, string(ohlcv[Symbol(col_key)])))
    end

    result = DataFrame(date=dates, value=values)
    sort!(result, :date)
    return result
end

function validate_connection(source::AlphaVantageSource)
    try
        params = Dict{String,String}(
            "function"   => "TIME_SERIES_DAILY",
            "symbol"     => "IBM",
            "outputsize" => "compact",
        )
        _av_request(source, params)
        return true
    catch
        return false
    end
end

function supports_asset_type(source::AlphaVantageSource, asset_type::Symbol)
    return asset_type in [:equity, :forex, :crypto, :index]
end

function get_metadata(source::AlphaVantageSource, identifier::String)
    try
        params = Dict{String,String}(
            "function" => "OVERVIEW",
            "symbol"   => identifier,
        )
        json = _av_request(source, params)
        return Dict{String,Any}(
            "id"       => identifier,
            "title"    => string(get(json, :Name, identifier)),
            "exchange" => string(get(json, :Exchange, "")),
            "currency" => string(get(json, :Currency, "")),
            "sector"   => string(get(json, :Sector, "")),
            "source"   => "Alpha Vantage",
        )
    catch
        return Dict{String,Any}("id" => identifier, "source" => "Alpha Vantage")
    end
end

function list_available_series(source::AlphaVantageSource; kwargs...)
    @warn "AlphaVantage uses standard ticker symbols (e.g., AAPL, MSFT, GOOGL). " *
          "Search at: https://www.alphavantage.co/"

    return [
        "AAPL",   # Apple
        "MSFT",   # Microsoft
        "GOOGL",  # Alphabet
        "AMZN",   # Amazon
        "TSLA",   # Tesla
        "SPY",    # S&P 500 ETF
        "QQQ",    # NASDAQ 100 ETF
        "DIA",    # Dow Jones ETF
    ]
end

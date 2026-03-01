"""
    BojSource.jl

Concrete implementation of DataSource for Bank of Japan statistical data.
Direct HTTP wrapper for the BoJ Time-Series Data Search API v1.
No API key required.

API reference: https://www.stat-search.boj.or.jp/info/api_manual_en.pdf
"""

using HTTP
using JSON3
using Dates
using DataFrames

"""
    BojSource <: DataSource

Data source for Bank of Japan (BoJ) statistics via the Time-Series Data Search API v1.
No API key required.

# Identifier Format
`"DB/SERIES_CODE"` — e.g. `"FM01/STRDCLUCON"` (overnight call rate)

# Constructor
    BojSource()

# Example
```julia
boj = BojSource()
call_rate = fetch_data(boj, "FM01/STRDCLUCON")          # overnight call rate (daily)
usdjpy    = fetch_time_series(boj, "FM08/FXERD01",
                start_date=Date(2020,1,1))              # USD/JPY daily
mon_base  = fetch_data(boj, "MD01/MABS1AN11")           # monetary base (monthly)
```

# Key Databases
- FM01  — Financial markets / call rates (daily)
- FM02  — Call rates, monthly averages
- FM08  — USD/JPY and EUR/USD FX rates (daily + monthly)
- MD01  — Monetary base
- BS01  — Bank of Japan balance sheet accounts
- IR01  — Basic discount/loan rate
- PR01  — Producer price indices (31,000+ series)
- BP01  — Balance of payments (17,000+ series)

# Key Series
- FM01/STRDCLUCON    — Uncollateralized overnight call rate, average (daily)
- FM08/FXERD01       — USD/JPY spot at 9:00 JST (daily)
- FM08/FXERD04       — USD/JPY spot at 17:00 JST (daily)
- FM08/FXERM07       — USD/JPY monthly average
- MD01/MABS1AN11     — Monetary base, average outstanding (monthly, since 1970)
- IR01/MADR1M        — Basic loan rate (monthly, since 1882)
- BS01/MABJMTA       — BoJ total assets
- PR01/PRCG20_2200000000 — PPI all commodities (monthly, since 1960)

# Browse Series
Use `list_available_series(boj, db="FM01")` or visit:
https://www.stat-search.boj.or.jp/index_en.html
"""
struct BojSource <: DataSource
    base_url::String
end

function BojSource()
    return BojSource("https://www.stat-search.boj.or.jp/api/v1")
end

"""Parse BoJ integer date (YYYYMMDD, YYYYMM, or YYYY) to Date."""
function _parse_boj_date(d::Integer)
    s = string(d)
    if length(s) == 8      # YYYYMMDD (daily)
        return Date(parse(Int, s[1:4]), parse(Int, s[5:6]), parse(Int, s[7:8]))
    elseif length(s) == 6  # YYYYMM (monthly)
        return Date(parse(Int, s[1:4]), parse(Int, s[5:6]), 1)
    elseif length(s) == 4  # YYYY (annual)
        return Date(parse(Int, s), 1, 1)
    else
        error("Unknown BoJ date format: $d")
    end
end

"""GET request to BoJ API, returns parsed JSON."""
function _boj_request(source::BojSource, endpoint::String, params::Vector{Pair{String,String}})
    url = "$(source.base_url)/$(endpoint)"
    response = HTTP.request("GET", url, ["Accept-Encoding" => "gzip"]; query=params)
    body = response.body
    # HTTP.jl handles gzip decompression automatically via Accept-Encoding
    return JSON3.read(String(copy(body)))
end

"""Parse DB/CODE identifier string into (db, code) tuple."""
function _parse_boj_identifier(identifier::String)
    parts = split(identifier, '/')
    length(parts) == 2 ||
        error("BoJ identifier must be 'DB/SERIES_CODE', e.g. 'FM01/STRDCLUCON'")
    return String(parts[1]), String(parts[2])
end

"""Parse a BoJ getDataCode JSON response into a standardised DataFrame."""
function _parse_boj_response(json)
    if json.STATUS != 200
        error("BoJ API error $(json.STATUS): $(json.MESSAGE)")
    end
    rs    = json.RESULTSET[1]
    dates = rs.VALUES.SURVEY_DATES
    vals  = rs.VALUES.VALUES

    date_col  = Date[]
    value_col = Float64[]
    for (d_int, v) in zip(dates, vals)
        v === nothing && continue
        push!(date_col,  _parse_boj_date(d_int))
        push!(value_col, Float64(v))
    end
    result = DataFrame(date=date_col, value=value_col)
    sort!(result, :date)
    return result
end

function fetch_data(source::BojSource, identifier::String; kwargs...)
    try
        db, code = _parse_boj_identifier(identifier)
        params = Pair{String,String}["DB" => db, "code" => code, "outputType" => "json"]
        json = _boj_request(source, "getDataCode", params)
        return _parse_boj_response(json)
    catch e
        throw(DataSourceError("BojSource", "Failed to fetch '$identifier': $(e)"))
    end
end

function fetch_time_series(source::BojSource, identifier::String;
                           start_date::Union{Date,Nothing}=nothing,
                           end_date::Union{Date,Nothing}=nothing)
    try
        df = fetch_data(source, identifier)
        if start_date !== nothing
            df = df[df.date .>= start_date, :]
        end
        if end_date !== nothing
            df = df[df.date .<= end_date, :]
        end
        return df
    catch e
        throw(DataSourceError("BojSource",
            "Failed to fetch time series '$identifier' ($(start_date) to $(end_date)): $(e)"))
    end
end

function validate_connection(source::BojSource)
    try
        params = Pair{String,String}["DB" => "FM01", "code" => "STRDCLUCON", "outputType" => "json"]
        json = _boj_request(source, "getDataCode", params)
        return json.STATUS == 200
    catch
        return false
    end
end

function supports_asset_type(::BojSource, asset_type::Symbol)
    return asset_type in [:interest_rate, :forex, :monetary, :economic_indicator, :inflation]
end

function get_metadata(source::BojSource, identifier::String)
    try
        db, code = _parse_boj_identifier(identifier)
        params = Pair{String,String}["DB" => db, "outputType" => "json"]
        json = _boj_request(source, "getMetadata", params)
        json.STATUS == 200 || error("Metadata fetch failed: $(json.MESSAGE)")
        # Find the matching series in the metadata list
        for s in json.RESULTSET
            if string(get(s, :SERIES_CODE, "")) == code
                return Dict{String,Any}(
                    "id"        => identifier,
                    "db"        => db,
                    "code"      => code,
                    "source"    => "Bank of Japan",
                    "name"      => string(get(s, :NAME_OF_TIME_SERIES, "")),
                    "name_jp"   => string(get(s, :NAME_OF_TIME_SERIES_J, "")),
                    "frequency" => string(get(s, :FREQUENCY, "")),
                    "unit"      => string(get(s, :UNIT, "")),
                    "unit_jp"   => string(get(s, :UNIT_J, "")),
                    "start"     => string(get(s, :START_OF_THE_TIME_SERIES, "")),
                    "end"       => string(get(s, :END_OF_THE_TIME_SERIES, "")),
                    "updated"   => string(get(s, :LAST_UPDATE, "")),
                    "url"       => "https://www.stat-search.boj.or.jp/",
                )
            end
        end
        # Series not in metadata list — return basic info
        return Dict{String,Any}(
            "id" => identifier, "db" => db, "code" => code,
            "source" => "Bank of Japan",
            "url" => "https://www.stat-search.boj.or.jp/",
        )
    catch
        return Dict{String,Any}(
            "id" => identifier, "source" => "Bank of Japan",
            "url" => "https://www.stat-search.boj.or.jp/",
        )
    end
end

function list_available_series(source::BojSource; db::String="FM01", kwargs...)
    try
        params = Pair{String,String}["DB" => db, "outputType" => "json"]
        json = _boj_request(source, "getMetadata", params)
        json.STATUS == 200 || error("$(json.MESSAGE)")
        result = String[]
        for s in json.RESULTSET
            code = string(get(s, :SERIES_CODE, ""))
            isempty(code) && continue
            push!(result, "$(db)/$(code)")
        end
        return result
    catch e
        @warn "BojSource: failed to list series for DB=$db: $e"
        return [
            "FM01/STRDCLUCON",   # Overnight call rate (daily)
            "FM08/FXERD01",      # USD/JPY spot 9:00 (daily)
            "FM08/FXERD04",      # USD/JPY spot 17:00 (daily)
            "FM08/FXERM07",      # USD/JPY monthly average
            "MD01/MABS1AN11",    # Monetary base (monthly)
            "IR01/MADR1M",       # Basic loan rate (monthly)
            "BS01/MABJMTA",      # BoJ total assets
            "PR01/PRCG20_2200000000",  # PPI all commodities (monthly)
        ]
    end
end

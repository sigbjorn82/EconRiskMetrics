"""
    RiksbanksSource.jl

Concrete implementation of DataSource for Sveriges Riksbank
(Sweden's central bank) interest and exchange rate data.
Direct HTTP wrapper for the Riksbank SWEA v1 REST API.
No API key required.

API reference: https://www.riksbank.se/en-gb/statistics/search-interest--exchange-rates/api-for-interest-and-exchange-rates/
"""

using HTTP
using JSON3
using Dates
using DataFrames

"""
    RiksbanksSource <: DataSource

Data source for Sveriges Riksbank (Sweden) via the SWEA v1 REST API.
Covers daily SEK exchange rates and Swedish interest/benchmark rates.
No API key required.

# Constructor
    RiksbanksSource()

# Identifier Format
`"endpoint/series_id"` — e.g. `"crossrates/SEKEURPMI"` or `"interests/REPORATE"`

- `endpoint`  — `"crossrates"` (FX rates) or `"interests"` (interest rates)
- `series_id` — Riksbank series code

If no `/` is provided, the identifier is treated as a series_id and both
endpoints are tried automatically (crossrates first, then interests).

# Example
```julia
rb = RiksbanksSource()

# EUR/SEK daily mid-rate
eursek = fetch_data(rb, "crossrates/SEKEURPMI")

# Riksbank repo rate (policy rate)
repo = fetch_time_series(rb, "interests/REPORATE",
                          start_date=Date(2000,1,1))

# 3-month STIBOR
stibor = fetch_data(rb, "interests/STIBOR3M")

# Auto-detect endpoint from series ID alone
usdrate = fetch_data(rb, "SEKUSDPMI")
```

# Key Series
Exchange rates (`crossrates/`):
- `"crossrates/SEKEURPMI"` — EUR/SEK daily mid-rate
- `"crossrates/SEKUSDPMI"` — USD/SEK daily mid-rate
- `"crossrates/SEKGBPPMI"` — GBP/SEK daily mid-rate
- `"crossrates/SEKJPYPMI"` — JPY/SEK daily mid-rate
- `"crossrates/SEKNOKPMI"` — NOK/SEK daily mid-rate
- `"crossrates/SEKDKKPMI"` — DKK/SEK daily mid-rate

Interest rates (`interests/`):
- `"interests/REPORATE"`   — Riksbank repo rate (policy rate)
- `"interests/STIBOR3M"`   — 3-month STIBOR (interbank offered rate)
- `"interests/STIBOR6M"`   — 6-month STIBOR
- `"interests/SWEGOVBOND10Y"` — 10-year Swedish government bond yield

# Browsing
Use `list_available_series(rb)` for a curated list, or browse
https://www.riksbank.se/en-gb/statistics/search-interest--exchange-rates/
"""
struct RiksbanksSource <: DataSource
    base_url::String
end

RiksbanksSource() = RiksbanksSource("https://api.riksbank.se/swea/v1")

"""
Parse RiksbanksSource identifier into (endpoint, series_id).
If no '/' prefix, endpoint is empty (auto-detect).
"""
function _parse_riksbanks_identifier(identifier::String)
    idx = findfirst('/', identifier)
    if idx === nothing
        return "", identifier
    end
    return identifier[1:idx-1], identifier[idx+1:end]
end

"""Format Date as YYYY-MM-DD for Riksbank URL path."""
_riksbanks_date_str(d::Date) = Dates.format(d, "yyyy-mm-dd")

"""GET observations from Riksbank API for a given endpoint and series."""
function _riksbanks_request(source::RiksbanksSource, endpoint::String,
                              series_id::String, from::String, to::String)
    url      = "$(source.base_url)/$(endpoint)/$(series_id)/$(from)/$(to)"
    response = HTTP.request("GET", url)
    return JSON3.read(String(copy(response.body)))
end

"""
Try both endpoints if endpoint is empty; return (endpoint, json) or throw.
"""
function _riksbanks_request_autodetect(source::RiksbanksSource,
                                        series_id::String,
                                        from::String, to::String)
    for ep in ("crossrates", "interests")
        try
            json = _riksbanks_request(source, ep, series_id, from, to)
            # Successful if we get an array back
            json isa AbstractVector && return ep, json
            json isa JSON3.Array    && return ep, json
        catch
        end
    end
    error("Series '$series_id' not found in crossrates or interests endpoints")
end

"""Parse Riksbank JSON response array into a DataFrame."""
function _parse_riksbanks_response(json)
    dates  = Date[]
    values = Float64[]

    for p in json
        d_str = string(get(p, :date, ""))
        isempty(d_str) && continue
        d = try Date(d_str, dateformat"yyyy-mm-dd") catch; continue end
        v = get(p, :value, nothing)
        (v === nothing || ismissing(v)) && continue
        fv = v isa Number ? Float64(v) : tryparse(Float64, string(v))
        fv === nothing && continue
        push!(dates, d)
        push!(values, fv)
    end

    result = DataFrame(date=dates, value=values)
    sort!(result, :date)
    return result
end

function fetch_data(source::RiksbanksSource, identifier::String; kwargs...)
    try
        endpoint, series_id = _parse_riksbanks_identifier(identifier)
        from = "1668-01-01"   # Riksbank founded 1668
        to   = _riksbanks_date_str(today())

        if isempty(endpoint)
            _, json = _riksbanks_request_autodetect(source, series_id, from, to)
        else
            json = _riksbanks_request(source, endpoint, series_id, from, to)
        end
        return _parse_riksbanks_response(json)
    catch e
        throw(DataSourceError("RiksbanksSource", "Failed to fetch '$identifier': $(e)"))
    end
end

function fetch_time_series(source::RiksbanksSource, identifier::String;
                           start_date::Union{Date,Nothing}=nothing,
                           end_date::Union{Date,Nothing}=nothing, kwargs...)
    try
        endpoint, series_id = _parse_riksbanks_identifier(identifier)
        from = start_date !== nothing ? _riksbanks_date_str(start_date) : "1668-01-01"
        to   = end_date   !== nothing ? _riksbanks_date_str(end_date)   : _riksbanks_date_str(today())

        if isempty(endpoint)
            _, json = _riksbanks_request_autodetect(source, series_id, from, to)
        else
            json = _riksbanks_request(source, endpoint, series_id, from, to)
        end
        df = _parse_riksbanks_response(json)
        start_date !== nothing && (df = df[df.date .>= start_date, :])
        end_date   !== nothing && (df = df[df.date .<= end_date,   :])
        return df
    catch e
        throw(DataSourceError("RiksbanksSource",
            "Failed to fetch '$identifier' ($(start_date) to $(end_date)): $(e)"))
    end
end

function validate_connection(source::RiksbanksSource)
    try
        json = _riksbanks_request(source, "crossrates", "SEKEURPMI",
                                   "2024-01-02", "2024-01-05")
        return json isa AbstractVector && length(json) > 0
    catch
        return false
    end
end

function supports_asset_type(::RiksbanksSource, asset_type::Symbol)
    return asset_type in [:forex, :interest_rate, :economic_indicator]
end

function get_metadata(source::RiksbanksSource, identifier::String)
    try
        endpoint, series_id = _parse_riksbanks_identifier(identifier)
        ep  = isempty(endpoint) ? "crossrates" : endpoint
        url = "$(source.base_url)/$(ep)/$(series_id)"
        response = HTTP.request("GET", url)
        json     = JSON3.read(String(copy(response.body)))
        return Dict{String,Any}(
            "id"          => identifier,
            "series_id"   => series_id,
            "endpoint"    => ep,
            "description" => string(get(json, :description, "")),
            "source"      => "Sveriges Riksbank",
            "url"         => "https://www.riksbank.se/en-gb/statistics/",
        )
    catch
        return Dict{String,Any}(
            "id"     => identifier,
            "source" => "Sveriges Riksbank",
            "url"    => "https://www.riksbank.se/en-gb/statistics/",
        )
    end
end

function list_available_series(source::RiksbanksSource; kwargs...)
    try
        # Fetch live list of available crossrates
        url = "$(source.base_url)/crossrates"
        r1  = HTTP.request("GET", url)
        j1  = JSON3.read(String(copy(r1.body)))
        url2 = "$(source.base_url)/interests"
        r2   = HTTP.request("GET", url2)
        j2   = JSON3.read(String(copy(r2.body)))
        result = String[]
        for s in j1
            sid = string(get(s, :seriesId, ""))
            isempty(sid) || push!(result, "crossrates/$(sid)")
        end
        for s in j2
            sid = string(get(s, :seriesId, ""))
            isempty(sid) || push!(result, "interests/$(sid)")
        end
        !isempty(result) && return result
    catch
    end
    # Curated fallback
    return [
        "crossrates/SEKEURPMI",    # EUR/SEK daily mid-rate
        "crossrates/SEKUSDPMI",    # USD/SEK daily mid-rate
        "crossrates/SEKGBPPMI",    # GBP/SEK daily mid-rate
        "crossrates/SEKJPYPMI",    # JPY/SEK daily mid-rate
        "crossrates/SEKNOKPMI",    # NOK/SEK daily mid-rate
        "crossrates/SEKDKKPMI",    # DKK/SEK daily mid-rate
        "interests/REPORATE",      # Riksbank repo rate
        "interests/STIBOR3M",      # 3-month STIBOR
        "interests/STIBOR6M",      # 6-month STIBOR
        "interests/SWEGOVBOND10Y", # 10-year Swedish government bond yield
    ]
end

"""
    SnbSource.jl

Concrete implementation of DataSource for the Swiss National Bank (SNB)
data portal API.
Direct HTTP wrapper — no API key required.

API reference: https://data.snb.ch/api/
"""

using HTTP
using JSON3
using Dates
using DataFrames

"""
    SnbSource <: DataSource

Data source for Swiss National Bank (SNB) via the data portal REST API.
No API key required.

# Constructor
    SnbSource()

# Identifier Format
`"cube_name"` or `"cube_name/dim0_value/dim1_value/..."` — e.g.:
- `"zimoma"`        — SNB policy rate (single-series cube)
- `"devkud/EUR"`    — EUR/CHF daily FX (filter dimension d0 = EUR)
- `"gkbgeld/M0"`    — Monetary base M0 (filter dimension d0 = M0)

Dimension filters are applied as `d0=val, d1=val, ...` query parameters.
For single-series cubes no filter is needed. Discover available dimension
values with `get_metadata(snb, "cube_name")`.

# Example
```julia
snb = SnbSource()

# SNB sight deposit rate (policy rate, monthly)
rate = fetch_data(snb, "zimoma")

# EUR/CHF daily FX rate
eurchf = fetch_time_series(snb, "devkud/EUR",
                           start_date=Date(2015,1,1))

# USD/CHF daily FX rate
usdchf = fetch_data(snb, "devkud/USD")

# Monetary aggregates M1/M2/M3 (monthly)
m1 = fetch_data(snb, "gkbgeld/M1")
```

# Key Cubes and Identifiers
- `"zimoma"`        — SNB sight deposit rate / policy rate (monthly)
- `"zirepo"`        — SNB repo rate (daily)
- `"devkud/EUR"`    — EUR/CHF daily spot rate
- `"devkud/USD"`    — USD/CHF daily spot rate
- `"devkud/GBP"`    — GBP/CHF daily spot rate
- `"devkud/JPY"`    — JPY/CHF daily spot rate
- `"devkum/EUR"`    — EUR/CHF monthly average
- `"gkbgeld/M0"`    — Monetary base M0 (monthly)
- `"gkbgeld/M1"`    — M1 money supply (monthly)
- `"gkbgeld/M2"`    — M2 money supply (monthly)
- `"gkbgeld/M3"`    — M3 money supply (monthly)
- `"snbbilancur"`   — SNB balance sheet total (monthly)
- `"snbimfsdds"`    — International reserves (monthly)

# Browsing
Use `list_available_series(snb)` for curated list, or browse
https://data.snb.ch for all available cubes and their dimension values.
"""
struct SnbSource <: DataSource
    base_url::String
end

SnbSource() = SnbSource("https://data.snb.ch/api")

"""Parse SNB identifier into (cube_name, dim_filters::Vector{String})."""
function _parse_snb_identifier(identifier::String)
    parts = String.(split(identifier, '/'))
    cube  = parts[1]
    dims  = length(parts) > 1 ? parts[2:end] : String[]
    return cube, dims
end

"""Parse an SNB date string to Date. Handles YYYY-MM-DD, YYYY-MM, YYYY."""
function _parse_snb_date(s::String)
    s = strip(s)
    if length(s) == 10
        return Date(s, dateformat"yyyy-mm-dd")
    elseif length(s) == 7
        return Date(s * "-01", dateformat"yyyy-mm-dd")
    else
        return Date(parse(Int, s[1:4]), 1, 1)
    end
end

"""Format a Date for SNB API date params: YYYY-MM-DD for daily, YYYY-MM for monthly."""
_snb_date_str(d::Date) = Dates.format(d, "yyyy-mm-dd")

"""GET cube data from SNB API; returns raw JSON."""
function _snb_request(source::SnbSource, cube::String, dims::Vector{String};
                      from::Union{String,Nothing}=nothing,
                      to::Union{String,Nothing}=nothing)
    url    = "$(source.base_url)/cube/$(cube)/data/json/en"
    params = Pair{String,String}[]
    from !== nothing && push!(params, "fromDate" => from)
    to   !== nothing && push!(params, "toDate"   => to)
    for (i, v) in enumerate(dims)
        push!(params, "d$(i-1)" => v)
    end
    response = HTTP.request("GET", url; query=params)
    return JSON3.read(String(copy(response.body)))
end

"""
Parse SNB JSON response into a standardised DataFrame.

The SNB API returns two possible response shapes:
  1. Single-series: `{"cube": "...", "date": [{"date": "...", "value": 1.75}]}`
  2. Multi-series:  `{"cube": "...", "data": [{"dims": {...}, "dates": [...], "values": [...]}]}`
"""
function _parse_snb_response(json)
    dates  = Date[]
    values = Float64[]

    if haskey(json, :date)
        # Single-series: array of {date, value} objects
        for p in json.date
            d_str = string(get(p, :date, ""))
            isempty(d_str) && continue
            v = get(p, :value, nothing)
            (v === nothing || ismissing(v)) && continue
            fv = v isa Number ? Float64(v) : tryparse(Float64, string(v))
            fv === nothing && continue
            push!(dates,  _parse_snb_date(d_str))
            push!(values, fv)
        end
        result = DataFrame(date=dates, value=values)
        sort!(result, :date)
        return result
    end

    if haskey(json, :data)
        # Multi-series: array of {dims, dates, values}
        series_keys = String[]
        for entry in json.data
            dim_map  = get(entry, :dims, (;))
            dim_vals = [string(v) for (_, v) in pairs(dim_map)]
            series_k = join(dim_vals, ".")
            ds       = get(entry, :dates, [])
            vs       = get(entry, :values, [])
            for (d_str, v) in zip(ds, vs)
                d_s = string(d_str)
                isempty(d_s) && continue
                (v === nothing || ismissing(v)) && continue
                fv = v isa Number ? Float64(v) : tryparse(Float64, string(v))
                fv === nothing && continue
                push!(dates,       _parse_snb_date(d_s))
                push!(values,      fv)
                push!(series_keys, series_k)
            end
        end
        unique_s = unique(series_keys)
        result = length(unique_s) == 1 ?
            DataFrame(date=dates, value=values) :
            DataFrame(date=dates, value=values, series=series_keys)
        sort!(result, :date)
        return result
    end

    error("Unexpected SNB response format (missing 'date' or 'data' key)")
end

function fetch_data(source::SnbSource, identifier::String; kwargs...)
    try
        cube, dims = _parse_snb_identifier(identifier)
        json = _snb_request(source, cube, dims)
        return _parse_snb_response(json)
    catch e
        throw(DataSourceError("SnbSource", "Failed to fetch '$identifier': $(e)"))
    end
end

function fetch_time_series(source::SnbSource, identifier::String;
                           start_date::Union{Date,Nothing}=nothing,
                           end_date::Union{Date,Nothing}=nothing, kwargs...)
    try
        cube, dims = _parse_snb_identifier(identifier)
        from = start_date !== nothing ? _snb_date_str(start_date) : nothing
        to   = end_date   !== nothing ? _snb_date_str(end_date)   : nothing
        json = _snb_request(source, cube, dims; from=from, to=to)
        df   = _parse_snb_response(json)
        start_date !== nothing && (df = df[df.date .>= start_date, :])
        end_date   !== nothing && (df = df[df.date .<= end_date,   :])
        return df
    catch e
        throw(DataSourceError("SnbSource",
            "Failed to fetch '$identifier' ($(start_date) to $(end_date)): $(e)"))
    end
end

function validate_connection(source::SnbSource)
    try
        json = _snb_request(source, "zimoma", String[];
                            from="2024-01", to="2024-06")
        return haskey(json, :date) || haskey(json, :data)
    catch
        return false
    end
end

function supports_asset_type(::SnbSource, asset_type::Symbol)
    return asset_type in [:forex, :interest_rate, :monetary, :economic_indicator,
                          :inflation]
end

function get_metadata(source::SnbSource, identifier::String)
    try
        cube, _ = _parse_snb_identifier(identifier)
        url      = "$(source.base_url)/cube/$(cube)/dimensions"
        response = HTTP.request("GET", url)
        json     = JSON3.read(String(copy(response.body)))
        dims     = get(json, :dimensions, [])
        dim_info = Dict{String,Any}()
        for (i, d) in enumerate(dims)
            vals = [string(v) for v in get(d, :values, [])]
            dim_info["d$(i-1)_values"] = vals
        end
        return merge(Dict{String,Any}(
            "id"     => identifier,
            "cube"   => cube,
            "source" => "Swiss National Bank",
            "url"    => "https://data.snb.ch",
        ), dim_info)
    catch
        return Dict{String,Any}(
            "id"     => identifier,
            "source" => "Swiss National Bank",
            "url"    => "https://data.snb.ch",
        )
    end
end

function list_available_series(::SnbSource; kwargs...)
    @warn "SnbSource: Browse all cubes at https://data.snb.ch"
    return [
        "zimoma",          # SNB sight deposit rate (policy rate, monthly)
        "zirepo",          # SNB repo rate (daily)
        "devkud/EUR",      # EUR/CHF daily spot rate
        "devkud/USD",      # USD/CHF daily spot rate
        "devkud/GBP",      # GBP/CHF daily spot rate
        "devkud/JPY",      # JPY/CHF daily spot rate
        "devkum/EUR",      # EUR/CHF monthly average
        "devkum/USD",      # USD/CHF monthly average
        "gkbgeld/M0",      # Monetary base M0 (monthly)
        "gkbgeld/M1",      # M1 money supply (monthly)
        "gkbgeld/M2",      # M2 money supply (monthly)
        "gkbgeld/M3",      # M3 money supply (monthly)
        "snbbilancur",     # SNB balance sheet total (monthly)
        "snbimfsdds",      # International reserves (monthly)
    ]
end

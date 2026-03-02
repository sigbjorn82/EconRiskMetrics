"""
    StatCanSource.jl

Concrete implementation of DataSource for Statistics Canada
(StatCan) Web Data Service (WDS).
Direct HTTP wrapper — no API key required.

API reference: https://www150.statcan.gc.ca/t1/tbl1/en/api/
"""

using HTTP
using JSON3
using Dates
using DataFrames

"""
    StatCanSource <: DataSource

Data source for Statistics Canada via the Web Data Service (WDS).
No API key required.

# Constructor
    StatCanSource()

# Identifier Format
`"v{vectorId}"` — e.g. `"v41690973"` (CPI all-items, Canada)

The vectorId is the unique integer identifying a single time series.
Find vector IDs by browsing https://www150.statcan.gc.ca, selecting a
table, choosing a series, and reading the `vector/v{ID}` part of the URL.

# Example
```julia
sc = StatCanSource()

# CPI all-items, Canada (monthly)
cpi = fetch_data(sc, "v41690973")

# Unemployment rate, Canada, both sexes (monthly)
unemp = fetch_time_series(sc, "v2062811", start_date=Date(2010,1,1))

# Bank of Canada overnight rate (monthly)
rate = fetch_data(sc, "v39079")
```

# Key Vector IDs
- `"v41690973"` — CPI all-items, Canada (monthly, Table 18-10-0004)
- `"v2062811"`  — Unemployment rate, Canada (monthly, Table 14-10-0287)
- `"v62305752"` — GDP at market prices, monthly (Table 36-10-0434)
- `"v39079"`    — Bank of Canada overnight rate (monthly, Table 10-10-0122)
- `"v111955444"` — New Housing Price Index, Canada (monthly, Table 18-10-0205)
- `"v86823773"` — Population, Canada (quarterly, Table 17-10-0009)

# Browsing
Use `list_available_series(sc)` for a curated list, or browse
https://www150.statcan.gc.ca to discover table and vector IDs.
"""
struct StatCanSource <: DataSource
    base_url::String
end

StatCanSource() = StatCanSource("https://www150.statcan.gc.ca/t1/tbl1/en")

"""Extract integer vectorId from identifier string (strips leading 'v')."""
function _statcan_vid(identifier::String)
    s = startswith(identifier, "v") || startswith(identifier, "V") ?
        identifier[2:end] : identifier
    vid = tryparse(Int, s)
    vid === nothing &&
        error("StatCan identifier must be 'v{integer}', e.g. 'v41690973'")
    return vid
end

"""POST to StatCan vectorDataRange endpoint; returns raw JSON."""
function _statcan_request(source::StatCanSource, vid::Int,
                           start_dt::Date, end_dt::Date)
    url  = "$(source.base_url)/tv!downloadTool/dtbl/vectorDataRange"
    body = JSON3.write([Dict(
        "vectorId"                  => vid,
        "startDataPointReleaseDate" => "$(start_dt)T00:00:00",
        "endDataPointReleaseDate"   => "$(end_dt)T00:00:00",
    )])
    response = HTTP.request("POST", url,
                            ["Content-Type" => "application/json"], body)
    return JSON3.read(String(copy(response.body)))
end

"""GET latest N periods from StatCan; returns raw JSON."""
function _statcan_request_latest(source::StatCanSource, vid::Int, n::Int)
    url      = "$(source.base_url)/tv!downloadTool/dtbl/vector/$(vid)/$(n)"
    response = HTTP.request("GET", url)
    return JSON3.read(String(copy(response.body)))
end

"""Parse a StatCan JSON response (single-vector) into a DataFrame."""
function _parse_statcan_response(json)
    status = get(json, :status, "")
    string(status) == "SUCCESS" ||
        error("StatCan API returned non-SUCCESS status: $(status)")

    obj = json.object
    # obj is an array (one element per vectorId)
    arr = obj isa AbstractVector ? obj : [obj]
    isempty(arr) && return DataFrame(date=Date[], value=Float64[])

    pts = get(arr[1], :vectorDataPoint, nothing)
    (pts === nothing || length(pts) == 0) &&
        return DataFrame(date=Date[], value=Float64[])

    dates  = Date[]
    values = Float64[]
    for p in pts
        v = get(p, :value, nothing)
        (v === nothing || ismissing(v)) && continue
        fv = v isa Number ? Float64(v) : tryparse(Float64, string(v))
        fv === nothing && continue
        d_str = string(get(p, :refPer, ""))
        isempty(d_str) && continue
        d = tryparse(Date, d_str)
        d === nothing && continue
        push!(dates, d)
        push!(values, fv)
    end

    result = DataFrame(date=dates, value=values)
    sort!(result, :date)
    return result
end

function fetch_data(source::StatCanSource, identifier::String; kwargs...)
    try
        vid = _statcan_vid(identifier)
        json = _statcan_request(source, vid,
                                Date(1867, 1, 1), Date(2099, 12, 31))
        return _parse_statcan_response(json)
    catch e
        throw(DataSourceError("StatCanSource", "Failed to fetch '$identifier': $(e)"))
    end
end

function fetch_time_series(source::StatCanSource, identifier::String;
                           start_date::Union{Date,Nothing}=nothing,
                           end_date::Union{Date,Nothing}=nothing, kwargs...)
    try
        vid = _statcan_vid(identifier)
        sd  = start_date !== nothing ? start_date : Date(1867, 1, 1)
        ed  = end_date   !== nothing ? end_date   : Date(2099, 12, 31)
        json = _statcan_request(source, vid, sd, ed)
        df   = _parse_statcan_response(json)
        start_date !== nothing && (df = df[df.date .>= start_date, :])
        end_date   !== nothing && (df = df[df.date .<= end_date,   :])
        return df
    catch e
        throw(DataSourceError("StatCanSource",
            "Failed to fetch '$identifier' ($(start_date) to $(end_date)): $(e)"))
    end
end

function validate_connection(source::StatCanSource)
    try
        # Fetch last 1 period of CPI vector as a connectivity check
        json = _statcan_request_latest(source, 41690973, 1)
        return string(get(json, :status, "")) == "SUCCESS"
    catch
        return false
    end
end

function supports_asset_type(::StatCanSource, asset_type::Symbol)
    return asset_type in [:economic_indicator, :gdp, :cpi, :unemployment,
                          :interest_rate, :housing, :monetary]
end

function get_metadata(source::StatCanSource, identifier::String)
    try
        vid  = _statcan_vid(identifier)
        json = _statcan_request_latest(source, vid, 1)
        string(get(json, :status, "")) == "SUCCESS" || error("API error")
        arr  = json.object
        obj  = arr isa AbstractVector ? arr[1] : arr
        return Dict{String,Any}(
            "id"           => identifier,
            "vector_id"    => vid,
            "product_id"   => get(obj, :productId, nothing),
            "coordinate"   => string(get(obj, :coordinate, "")),
            "frequency"    => get(obj, :frequencyCode, nothing),
            "source"       => "Statistics Canada",
            "url"          => "https://www150.statcan.gc.ca",
        )
    catch
        return Dict{String,Any}(
            "id"     => identifier,
            "source" => "Statistics Canada",
            "url"    => "https://www150.statcan.gc.ca",
        )
    end
end

function list_available_series(::StatCanSource; kwargs...)
    @warn "StatCanSource: Browse all tables at https://www150.statcan.gc.ca"
    return [
        "v41690973",   # CPI all-items, Canada (monthly)
        "v2062811",    # Unemployment rate, Canada (monthly)
        "v62305752",   # GDP at market prices, monthly
        "v39079",      # Bank of Canada overnight rate (monthly)
        "v111955444",  # New Housing Price Index, Canada (monthly)
        "v86823773",   # Population, Canada (quarterly)
        "v36406",      # 3-month T-bill yield (monthly)
        "v122620",     # 10-year Government of Canada bond (monthly)
        "v41692327",   # CPI shelter, Canada (monthly)
        "v41692451",   # CPI food, Canada (monthly)
    ]
end

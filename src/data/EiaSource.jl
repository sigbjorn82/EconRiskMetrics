"""
    EiaSource.jl

Concrete implementation of DataSource for the US Energy Information
Administration (EIA) open data API v2.
Requires a free API key from https://www.eia.gov/opendata/register.php.

API reference: https://www.eia.gov/opendata/
"""

using HTTP
using JSON3
using Dates
using DataFrames

"""
    EiaSource <: DataSource

Data source for US Energy Information Administration (EIA) via API v2.
Requires a free API key (register at https://www.eia.gov/opendata/register.php).
Set `EIA_API_KEY` in `.env`.

# Constructor
    EiaSource()                   # uses EIA_API_KEY env var
    EiaSource(api_key::String)    # use provided key

# Identifier Format
`"route:series_value"` — e.g. `"petroleum/pri/spt:RWTC"`

- `route`        — slash-separated API path (e.g. `"petroleum/pri/spt"`)
- `series_value` — value for the `series` facet filter

# Frequency
Pass `frequency` keyword to `fetch_data` / `fetch_time_series`.
Default: `"daily"`. Accepted values: `"daily"`, `"weekly"`, `"monthly"`, `"annual"`.

# Example
```julia
eia = EiaSource()

# WTI crude oil spot price (daily)
wti = fetch_data(eia, "petroleum/pri/spt:RWTC")

# Brent crude oil spot price (daily)
brent = fetch_time_series(eia, "petroleum/pri/spt:RBRTE",
                           start_date=Date(2015,1,1))

# Henry Hub natural gas spot price (weekly)
hh = fetch_data(eia, "natural-gas/pri/sum:RNGWHHD";
                frequency="weekly")

# US regular gasoline retail price (weekly)
gas = fetch_data(eia, "petroleum/pri/gnd:EMM_EPMR_PTE_NUS_DPG";
                 frequency="weekly")
```

# Key Series
- `"petroleum/pri/spt:RWTC"`                    — WTI crude oil (Cushing, OK), daily
- `"petroleum/pri/spt:RBRTE"`                   — Brent crude oil spot, daily
- `"petroleum/pri/gnd:EMM_EPMR_PTE_NUS_DPG"`   — US regular gasoline retail, weekly
- `"natural-gas/pri/sum:RNGWHHD"`               — Henry Hub natural gas, weekly
- `"natural-gas/pri/fut:RNGC1"`                 — Natural gas front-month futures, daily
- `"coal/production/annual:US-TOT"`             — US total coal production, annual

# Browsing
Use `list_available_series(eia)` for curated list, or browse
https://www.eia.gov/opendata/browser/ to discover routes and facets.
"""
struct EiaSource <: DataSource
    base_url::String
    api_key::String
end

function EiaSource(api_key::String="")
    key = isempty(api_key) ? get(ENV, "EIA_API_KEY", "") : api_key
    return EiaSource("https://api.eia.gov/v2", key)
end

"""Parse an EIA identifier into (route, series_value)."""
function _parse_eia_identifier(identifier::String)
    idx = findfirst(':', identifier)
    idx === nothing &&
        error("EIA identifier must be 'route:series_value', e.g. 'petroleum/pri/spt:RWTC'")
    return identifier[1:idx-1], identifier[idx+1:end]
end

"""Format a Date for the EIA API based on frequency."""
function _eia_date_str(d::Date, frequency::String)
    if frequency in ("daily", "weekly")
        return Dates.format(d, "yyyy-mm-dd")
    elseif frequency == "monthly"
        return Dates.format(d, "yyyy-mm")
    else   # annual
        return string(year(d))
    end
end

"""
Fetch one page of EIA data. Returns (records, total).
`offset` is 0-based.
"""
function _eia_fetch_page(source::EiaSource, route::String, series::String,
                          frequency::String, start_str::Union{String,Nothing},
                          end_str::Union{String,Nothing}, offset::Int)
    url    = "$(source.base_url)/$(route)/data"
    params = Pair{String,String}[
        "api_key"           => source.api_key,
        "frequency"         => frequency,
        "data[]"            => "value",
        "facets[series][]"  => series,
        "sort[0][column]"   => "period",
        "sort[0][direction]"=> "asc",
        "length"            => "5000",
        "offset"            => string(offset),
    ]
    start_str !== nothing && push!(params, "start" => start_str)
    end_str   !== nothing && push!(params, "end"   => end_str)

    response = HTTP.request("GET", url; query=params)
    json     = JSON3.read(String(copy(response.body)))

    resp  = json.response
    total = Int(resp.total)
    data  = resp.data
    return data, total
end

"""Fetch all paginated EIA data and return a DataFrame."""
function _eia_fetch_all(source::EiaSource, route::String, series::String,
                         frequency::String,
                         start_str::Union{String,Nothing},
                         end_str::Union{String,Nothing})
    all_dates  = Date[]
    all_values = Float64[]
    offset     = 0

    while true
        records, total = _eia_fetch_page(source, route, series, frequency,
                                          start_str, end_str, offset)
        for r in records
            period = string(get(r, :period, ""))
            isempty(period) && continue
            v_raw = get(r, :value, nothing)
            (v_raw === nothing || ismissing(v_raw)) && continue
            fv = v_raw isa Number ? Float64(v_raw) : tryparse(Float64, string(v_raw))
            fv === nothing && continue
            # Parse period to Date
            d = try
                if length(period) == 10
                    Date(period, dateformat"yyyy-mm-dd")
                elseif length(period) == 7
                    Date(period * "-01", dateformat"yyyy-mm-dd")
                else
                    Date(parse(Int, period[1:4]), 1, 1)
                end
            catch
                continue
            end
            push!(all_dates,  d)
            push!(all_values, fv)
        end
        offset += 5000
        offset >= total && break
    end

    result = DataFrame(date=all_dates, value=all_values)
    sort!(result, :date)
    return result
end

function fetch_data(source::EiaSource, identifier::String;
                    frequency::String="daily", kwargs...)
    try
        isempty(source.api_key) &&
            error("EIA_API_KEY not set. Register at https://www.eia.gov/opendata/register.php")
        route, series = _parse_eia_identifier(identifier)
        return _eia_fetch_all(source, route, series, frequency, nothing, nothing)
    catch e
        throw(DataSourceError("EiaSource", "Failed to fetch '$identifier': $(e)"))
    end
end

function fetch_time_series(source::EiaSource, identifier::String;
                           start_date::Union{Date,Nothing}=nothing,
                           end_date::Union{Date,Nothing}=nothing,
                           frequency::String="daily", kwargs...)
    try
        isempty(source.api_key) &&
            error("EIA_API_KEY not set. Register at https://www.eia.gov/opendata/register.php")
        route, series = _parse_eia_identifier(identifier)
        sd = start_date !== nothing ? _eia_date_str(start_date, frequency) : nothing
        ed = end_date   !== nothing ? _eia_date_str(end_date,   frequency) : nothing
        df = _eia_fetch_all(source, route, series, frequency, sd, ed)
        start_date !== nothing && (df = df[df.date .>= start_date, :])
        end_date   !== nothing && (df = df[df.date .<= end_date,   :])
        return df
    catch e
        throw(DataSourceError("EiaSource",
            "Failed to fetch '$identifier' ($(start_date) to $(end_date)): $(e)"))
    end
end

function validate_connection(source::EiaSource)
    try
        isempty(source.api_key) && return false
        _, total = _eia_fetch_page(source, "petroleum/pri/spt", "RWTC",
                                   "daily", "2024-01-02", "2024-01-05", 0)
        return total > 0
    catch
        return false
    end
end

function supports_asset_type(::EiaSource, asset_type::Symbol)
    return asset_type in [:commodity, :energy, :economic_indicator]
end

function get_metadata(source::EiaSource, identifier::String)
    try
        route, series = _parse_eia_identifier(identifier)
        url      = "$(source.base_url)/$(route)"
        params   = Pair{String,String}["api_key" => source.api_key]
        response = HTTP.request("GET", url; query=params)
        json     = JSON3.read(String(copy(response.body)))
        resp     = get(json, :response, (;))
        return Dict{String,Any}(
            "id"          => identifier,
            "route"       => route,
            "series"      => series,
            "description" => string(get(resp, :description, "")),
            "source"      => "US Energy Information Administration",
            "url"         => "https://www.eia.gov/opendata/browser/",
        )
    catch
        return Dict{String,Any}(
            "id"     => identifier,
            "source" => "US Energy Information Administration",
            "url"    => "https://www.eia.gov/opendata/browser/",
        )
    end
end

function list_available_series(::EiaSource; kwargs...)
    @warn "EiaSource: Browse series at https://www.eia.gov/opendata/browser/"
    return [
        "petroleum/pri/spt:RWTC",                   # WTI crude (Cushing, OK), daily
        "petroleum/pri/spt:RBRTE",                  # Brent crude spot, daily
        "petroleum/pri/gnd:EMM_EPMR_PTE_NUS_DPG",  # US regular gasoline retail, weekly
        "natural-gas/pri/sum:RNGWHHD",              # Henry Hub natural gas, weekly
        "natural-gas/pri/fut:RNGC1",                # Natural gas front-month futures, daily
        "coal/production/annual:US-TOT",            # US total coal production, annual
    ]
end

"""
    EcosSource.jl

Concrete implementation of DataSource for Bank of Korea ECOS
(Economic Statistics System).
Direct HTTP wrapper for the ECOS Open API v2.
Requires a free API key from https://ecos.bok.or.kr.

API reference: https://ecos.bok.or.kr/api/#/DevGuide/DevGuide
"""

using HTTP
using JSON3
using Dates
using DataFrames

"""
    EcosSource <: DataSource

Data source for Bank of Korea ECOS (Economic Statistics System).
Requires a free API key from https://ecos.bok.or.kr (free registration).
The key `"sample"` can be used for limited testing (returns small data samples).

# Constructor
    EcosSource()                   # uses ECOS_API_KEY env var or "sample"
    EcosSource(api_key::String)    # use provided key

# Identifier Format
`"STATCODE/CYCLE/ITEM1"` — e.g. `"722Y001/M/0101000"`

- `STATCODE` — statistical table code (e.g. `"722Y001"`)
- `CYCLE`    — frequency: `M` monthly, `D` daily, `Q` quarterly, `S` semi-annual, `A` annual
- `ITEM1`    — item dimension 1 code; use `?` as wildcard
- Additional item dimensions: dot-separated (e.g. `"0101000.0102000"`)

Short form `"STATCODE/ITEM1"` defaults to monthly cycle.

# Example
```julia
ecos = EcosSource("MY_API_KEY")

# Bank of Korea base rate (monthly)
rate = fetch_data(ecos, "722Y001/M/0101000")

# USD/KRW exchange rate (daily) with date filter
fx = fetch_time_series(ecos, "731Y003/D/0000001",
                       start_date=Date(2020,1,1))

# CPI all components (monthly)
cpi = fetch_data(ecos, "021Y125/M/?")
```

# Key Series
- `"722Y001/M/0101000"` — Bank of Korea base rate (monthly, since 1954)
- `"731Y003/D/0000001"` — USD/KRW exchange rate (daily)
- `"021Y125/M/?"` — CPI all items (monthly, use `?` to get all components)
- `"101Y004/M/?"` — Money supply M1/M2 (monthly)
- `"901Y009/Q/?"` — GDP national accounts (quarterly)
- `"200Y001/M/?"` — Balance of payments (monthly)
- `"111Y002/M/?"` — International reserves (monthly)
- `"902Y003/M/?"` — Labour market statistics (monthly)
- `"403Y001/M/?"` — Industrial production index (monthly)

# Browsing Series
Use `list_available_series(ecos)` to list stat tables, or visit
https://ecos.bok.or.kr to browse the full catalogue.
"""
struct EcosSource <: DataSource
    base_url::String
    api_key::String
end

function EcosSource(api_key::String="")
    key = isempty(api_key) ? get(ENV, "ECOS_API_KEY", "sample") : api_key
    return EcosSource("https://ecos.bok.or.kr/api", key)
end

"""Parse a nullable JSON field to String, returning "" for null/nothing."""
function _ecos_str(v)
    (v === nothing || ismissing(v)) && return ""
    return string(v)
end

"""Parse ECOS TIME string to Date (auto-detects format from content)."""
function _parse_ecos_date(time_str::String)
    if length(time_str) == 8                              # YYYYMMDD (daily)
        return Date(time_str, dateformat"yyyymmdd")
    elseif length(time_str) == 6                          # YYYYMM (monthly)
        return Date(time_str * "01", dateformat"yyyymmdd")
    elseif length(time_str) == 7 && time_str[5] == 'Q'   # YYYYQ1–Q4
        yr = parse(Int, time_str[1:4])
        q  = parse(Int, time_str[7])
        return Date(yr, (q - 1) * 3 + 1, 1)
    elseif length(time_str) == 7 && time_str[5] == 'S'   # YYYYS1–S2
        yr = parse(Int, time_str[1:4])
        s  = parse(Int, time_str[7])
        return Date(yr, s == 1 ? 1 : 7, 1)
    else                                                  # YYYY (annual)
        return Date(parse(Int, time_str[1:4]), 1, 1)
    end
end

"""Format a Date to an ECOS date string for the given cycle."""
function _ecos_date_str(d::Date, cycle::String)
    if cycle == "D"
        return Dates.format(d, "yyyymmdd")
    elseif cycle == "M"
        return Dates.format(d, "yyyymm")
    elseif cycle == "Q"
        q = (month(d) - 1) ÷ 3 + 1
        return "$(year(d))Q$(q)"
    elseif cycle == "S"
        s = month(d) <= 6 ? 1 : 2
        return "$(year(d))S$(s)"
    else
        return string(year(d))
    end
end

"""Wide-range default start date string for a given cycle."""
function _ecos_default_start(cycle::String)
    cycle == "D" ? "19600101" : cycle == "M" ? "196001" :
    cycle == "Q" ? "1960Q1"   : cycle == "S" ? "1960S1" : "1960"
end

"""Today as an ECOS end date string for a given cycle."""
_ecos_default_end(cycle::String) = _ecos_date_str(today(), cycle)

"""Parse ECOS identifier into (stat_code, cycle, items[4])."""
function _parse_ecos_identifier(identifier::String)
    parts = String.(split(identifier, '/'))
    length(parts) >= 2 ||
        error("ECOS identifier must be 'STATCODE/CYCLE/ITEM1', e.g. '722Y001/M/0101000'")
    stat_code = parts[1]
    valid_cycles = Set(["M", "D", "Q", "S", "A"])
    if length(parts) == 2
        if uppercase(parts[2]) in valid_cycles
            cycle     = uppercase(parts[2])
            item_parts = ["?"]
        else
            cycle     = "M"
            item_parts = String.(split(parts[2], '.'))
        end
    else
        cycle     = uppercase(parts[2])
        item_parts = String.(split(parts[3], '.'))
    end
    while length(item_parts) < 4
        push!(item_parts, "?")
    end
    return stat_code, cycle, item_parts[1:4]
end

"""GET request to ECOS API; path_parts are appended after .../json/en/..."""
function _ecos_request(source::EcosSource, service::String, path_parts::Vector{String})
    url = join([source.base_url, service, source.api_key, "json", "en", path_parts...], "/")
    response = HTTP.request("GET", url)
    return JSON3.read(String(copy(response.body)))
end

"""Parse a StatisticSearch JSON response into a standardised DataFrame."""
function _parse_ecos_response(json)
    # Top-level errors arrive as {"RESULT": {"CODE": "ERROR-XXX", "MESSAGE": "..."}}
    if haskey(json, :RESULT)
        code = _ecos_str(get(json.RESULT, :CODE, ""))
        msg  = _ecos_str(get(json.RESULT, :MESSAGE, ""))
        if code == "ERROR-301"
            @warn "ECOS sample key is restricted to 10 rows. " *
                  "Register for a free full-access key at https://ecos.bok.or.kr → ECOS_API_KEY."
            return DataFrame(date=Date[], value=Float64[])
        end
        code == "INFO-000" || error("ECOS API error $(code): $(msg)")
    end
    haskey(json, :StatisticSearch) ||
        error("Unexpected ECOS response format (missing StatisticSearch key)")
    ss   = json.StatisticSearch
    rows = get(ss, :row, nothing)
    (rows === nothing || length(rows) == 0) && return DataFrame(date=Date[], value=Float64[])

    dates       = Date[]
    values      = Float64[]
    series_keys = String[]

    for r in rows
        val_str = _ecos_str(get(r, :DATA_VALUE, ""))
        isempty(val_str) && continue
        v = tryparse(Float64, val_str)
        v === nothing && continue
        push!(dates,  _parse_ecos_date(string(r.TIME)))
        push!(values, v)
        item_vals = [_ecos_str(get(r, k, nothing))
                     for k in (:ITEM_CODE1, :ITEM_CODE2, :ITEM_CODE3, :ITEM_CODE4)]
        push!(series_keys, join(filter(!isempty, item_vals), "."))
    end

    unique_series = unique(series_keys)
    result = length(unique_series) == 1 ?
        DataFrame(date=dates, value=values) :
        DataFrame(date=dates, value=values, series=series_keys)
    sort!(result, :date)
    return result
end

function fetch_data(source::EcosSource, identifier::String; kwargs...)
    try
        stat_code, cycle, items = _parse_ecos_identifier(identifier)
        path = ["1", "100000", stat_code, cycle,
                _ecos_default_start(cycle), _ecos_default_end(cycle), items...]
        return _parse_ecos_response(_ecos_request(source, "StatisticSearch", path))
    catch e
        throw(DataSourceError("EcosSource", "Failed to fetch '$identifier': $(e)"))
    end
end

function fetch_time_series(source::EcosSource, identifier::String;
                           start_date::Union{Date,Nothing}=nothing,
                           end_date::Union{Date,Nothing}=nothing, kwargs...)
    try
        stat_code, cycle, items = _parse_ecos_identifier(identifier)
        sd = start_date !== nothing ? _ecos_date_str(start_date, cycle) : _ecos_default_start(cycle)
        ed = end_date   !== nothing ? _ecos_date_str(end_date,   cycle) : _ecos_default_end(cycle)
        path = ["1", "100000", stat_code, cycle, sd, ed, items...]
        df = _parse_ecos_response(_ecos_request(source, "StatisticSearch", path))
        start_date !== nothing && (df = df[df.date .>= start_date, :])
        end_date   !== nothing && (df = df[df.date .<= end_date,   :])
        return df
    catch e
        throw(DataSourceError("EcosSource",
            "Failed to fetch '$identifier' ($(start_date) to $(end_date)): $(e)"))
    end
end

function validate_connection(source::EcosSource)
    try
        # StatisticTableList has no row-count restriction — works with the sample key
        json = _ecos_request(source, "StatisticTableList", ["1", "1"])
        haskey(json, :StatisticTableList) && return true
        if haskey(json, :RESULT)
            code = _ecos_str(get(json.RESULT, :CODE, ""))
            return code == "INFO-000"
        end
        return false
    catch
        return false
    end
end

function supports_asset_type(::EcosSource, asset_type::Symbol)
    return asset_type in [:interest_rate, :forex, :gdp, :cpi, :economic_indicator,
                          :monetary, :inflation, :unemployment]
end

function get_metadata(source::EcosSource, identifier::String)
    try
        stat_code, cycle, _ = _parse_ecos_identifier(identifier)
        path = ["1", "100", stat_code]
        json = _ecos_request(source, "StatisticItemList", path)
        if haskey(json, :StatisticItemList)
            rows = get(json.StatisticItemList, :row, [])
            !isempty(rows) && return Dict{String,Any}(
                "id"         => identifier,
                "stat_code"  => stat_code,
                "cycle"      => cycle,
                "source"     => "Bank of Korea ECOS",
                "stat_name"  => _ecos_str(get(rows[1], :STAT_NAME, "")),
                "item_count" => length(rows),
                "url"        => "https://ecos.bok.or.kr",
            )
        end
    catch
    end
    return Dict{String,Any}(
        "id" => identifier, "source" => "Bank of Korea ECOS",
        "url" => "https://ecos.bok.or.kr",
    )
end

function list_available_series(source::EcosSource; kwargs...)
    try
        json = _ecos_request(source, "StatisticTableList", ["1", "500"])
        if haskey(json, :StatisticTableList)
            rows = get(json.StatisticTableList, :row, [])
            result = String[]
            for r in rows
                sc = _ecos_str(get(r, :STAT_CODE, ""))
                cy = _ecos_str(get(r, :CYCLE, "M"))
                isempty(sc) && continue
                push!(result, "$(sc)/$(cy)/?")
            end
            !isempty(result) && return result
        end
    catch
    end
    # Curated fallback list
    return [
        "722Y001/M/0101000",   # BoK base rate (monthly, since 1954)
        "731Y003/D/0000001",   # USD/KRW exchange rate (daily)
        "021Y125/M/?",         # CPI all items (monthly)
        "101Y004/M/?",         # Money supply M1/M2 (monthly)
        "901Y009/Q/?",         # GDP national accounts (quarterly)
        "200Y001/M/?",         # Balance of payments (monthly)
        "111Y002/M/?",         # International reserves (monthly)
        "902Y003/M/?",         # Labour market statistics (monthly)
        "403Y001/M/?",         # Industrial production index (monthly)
    ]
end

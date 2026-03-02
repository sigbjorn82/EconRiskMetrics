"""
    OnsSource.jl

Concrete implementation of DataSource for UK Office for National
Statistics (ONS) time series data.
Direct HTTP wrapper for the ONS legacy time series API.
No API key required.

API reference: https://developer.ons.gov.uk/
"""

using HTTP
using JSON3
using Dates
using DataFrames

"""
    OnsSource <: DataSource

Data source for UK Office for National Statistics (ONS) via the
legacy time series API. No API key required.

Complements `BankOfEnglandSource` (monetary/rates) with national
accounts, labour market, and price statistics.

# Constructor
    OnsSource()

# Identifier Format
`"dataset_id/series_id"` — e.g. `"cpih01/L55O"` (CPIH all-items annual rate)

- `dataset_id` — ONS publication code (e.g. `"cpih01"`, `"qna"`, `"lms"`)
- `series_id`  — CDID code (e.g. `"L55O"`, `"ABMI"`, `"MGSX"`)

Short form `"series_id"` (no slash) queries the series directly without
specifying a dataset — works for most series but may be slower.

# Example
```julia
ons = OnsSource()

# CPIH inflation (annual rate, all-items)
cpi = fetch_data(ons, "cpih01/L55O")

# UK GDP (quarterly, chained volume, seasonally adjusted)
gdp = fetch_time_series(ons, "qna/ABMI", start_date=Date(2000,1,1))

# UK unemployment rate (LFS, 16+, seasonally adjusted)
unemp = fetch_data(ons, "lms/MGSX")
```

# Key Series (dataset/CDID)
- `"cpih01/L55O"`  — CPIH all-items annual rate (%)
- `"mm23/D7G7"`    — CPI all-items annual rate (%)
- `"qna/ABMI"`     — GDP quarterly, chained volume, SA
- `"mgdp/IHYP"`    — GDP monthly estimate
- `"lms/MGSX"`     — Unemployment rate, LFS 16+, SA
- `"lms/MGRZ"`     — Employment rate, LFS 16+, SA
- `"mm23/CHAW"`    — RPI all-items (%)
- `"retail/EAPB"`  — Retail sales volume index, SA
- `"hpi/UKHP"`     — UK House Price Index (all dwellings)

# Browsing
Use `list_available_series(ons)` for a curated list, or visit
https://www.ons.gov.uk to find CDID codes in dataset download CSVs.
"""
struct OnsSource <: DataSource
    base_url::String
end

OnsSource() = OnsSource("https://api.ons.gov.uk/v1")

"""Parse an ONS identifier into (dataset_id, series_id). dataset_id may be empty."""
function _parse_ons_identifier(identifier::String)
    idx = findfirst('/', identifier)
    if idx === nothing
        return "", identifier          # series_id only
    end
    return identifier[1:idx-1], identifier[idx+1:end]
end

"""GET time series data from ONS API; returns raw JSON."""
function _ons_request(source::OnsSource, dataset_id::String, series_id::String)
    url = if isempty(dataset_id)
        "$(source.base_url)/timeseries/$(uppercase(series_id))/data"
    else
        "$(source.base_url)/dataset/$(dataset_id)/timeseries/$(uppercase(series_id))/data"
    end
    response = HTTP.request("GET", url)
    return JSON3.read(String(copy(response.body)))
end

"""
Parse an ONS date string to a Date.
Handles: "2024" (annual), "2023 Q3" (quarterly), "2024 JAN" (monthly).
"""
function _parse_ons_date(s::String)
    s = strip(s)
    if length(s) == 4 && all(isdigit, s)          # "2024"
        return Date(parse(Int, s), 1, 1)
    elseif occursin(" Q", s)                        # "2023 Q3"
        parts = split(s)
        yr = parse(Int, parts[1])
        q  = parse(Int, String(parts[2])[2])
        return Date(yr, (q - 1) * 3 + 1, 1)
    elseif length(s) == 8 && s[5] == ' '           # "2024 JAN" / "2024 FEB" etc.
        months = Dict("JAN"=>1,"FEB"=>2,"MAR"=>3,"APR"=>4,"MAY"=>5,"JUN"=>6,
                      "JUL"=>7,"AUG"=>8,"SEP"=>9,"OCT"=>10,"NOV"=>11,"DEC"=>12)
        yr  = parse(Int, s[1:4])
        mon = get(months, uppercase(s[6:8]), nothing)
        mon === nothing && error("Unknown month in ONS date: '$s'")
        return Date(yr, mon, 1)
    elseif length(s) == 10                          # "2024-01-01" (some daily series)
        return Date(s, dateformat"yyyy-mm-dd")
    else
        error("Unknown ONS date format: '$s'")
    end
end

"""Parse ONS JSON response into a DataFrame (uses highest-frequency data available)."""
function _parse_ons_response(json)
    # Prefer months > quarters > years
    data_arr = nothing
    freq     = :annual

    months = get(json, :months, nothing)
    if months !== nothing && length(months) > 0
        data_arr = months; freq = :monthly
    else
        quarters = get(json, :quarters, nothing)
        if quarters !== nothing && length(quarters) > 0
            data_arr = quarters; freq = :quarterly
        else
            years = get(json, :years, nothing)
            years !== nothing && length(years) > 0 && (data_arr = years; freq = :annual)
        end
    end

    data_arr === nothing && return DataFrame(date=Date[], value=Float64[])

    dates  = Date[]
    values = Float64[]
    for p in data_arr
        val_str = string(get(p, :value, ""))
        isempty(val_str) && continue
        v = tryparse(Float64, val_str)
        v === nothing && continue
        d_str = string(get(p, :date, ""))
        isempty(d_str) && continue
        d = try _parse_ons_date(d_str) catch; continue end
        push!(dates, d)
        push!(values, v)
    end

    result = DataFrame(date=dates, value=values)
    sort!(result, :date)
    return result
end

function fetch_data(source::OnsSource, identifier::String; kwargs...)
    try
        dataset_id, series_id = _parse_ons_identifier(identifier)
        json = _ons_request(source, dataset_id, series_id)
        return _parse_ons_response(json)
    catch e
        throw(DataSourceError("OnsSource", "Failed to fetch '$identifier': $(e)"))
    end
end

function fetch_time_series(source::OnsSource, identifier::String;
                           start_date::Union{Date,Nothing}=nothing,
                           end_date::Union{Date,Nothing}=nothing, kwargs...)
    try
        df = fetch_data(source, identifier)
        start_date !== nothing && (df = df[df.date .>= start_date, :])
        end_date   !== nothing && (df = df[df.date .<= end_date,   :])
        return df
    catch e
        throw(DataSourceError("OnsSource",
            "Failed to fetch '$identifier' ($(start_date) to $(end_date)): $(e)"))
    end
end

function validate_connection(source::OnsSource)
    try
        json = _ons_request(source, "cpih01", "L55O")
        years = get(json, :years, nothing)
        return years !== nothing && length(years) > 0
    catch
        return false
    end
end

function supports_asset_type(::OnsSource, asset_type::Symbol)
    return asset_type in [:economic_indicator, :gdp, :cpi, :unemployment,
                          :housing, :inflation, :retail]
end

function get_metadata(source::OnsSource, identifier::String)
    try
        dataset_id, series_id = _parse_ons_identifier(identifier)
        url = if isempty(dataset_id)
            "$(source.base_url)/timeseries/$(uppercase(series_id))"
        else
            "$(source.base_url)/dataset/$(dataset_id)/timeseries/$(uppercase(series_id))"
        end
        response = HTTP.request("GET", url)
        json     = JSON3.read(String(copy(response.body)))
        desc     = get(json, :description, (;))
        return Dict{String,Any}(
            "id"         => identifier,
            "series_id"  => series_id,
            "dataset_id" => dataset_id,
            "title"      => string(get(desc, :title, "")),
            "unit"       => string(get(desc, :unit, "")),
            "source"     => "UK Office for National Statistics",
            "url"        => "https://www.ons.gov.uk",
        )
    catch
        return Dict{String,Any}(
            "id"     => identifier,
            "source" => "UK Office for National Statistics",
            "url"    => "https://www.ons.gov.uk",
        )
    end
end

function list_available_series(::OnsSource; kwargs...)
    @warn "OnsSource: Find CDID codes at https://www.ons.gov.uk (in dataset download CSVs)"
    return [
        "cpih01/L55O",   # CPIH all-items annual rate
        "mm23/D7G7",     # CPI all-items annual rate
        "mm23/CHAW",     # RPI all-items
        "qna/ABMI",      # GDP quarterly, chained volume, SA
        "mgdp/IHYP",     # GDP monthly estimate
        "lms/MGSX",      # Unemployment rate, LFS 16+, SA
        "lms/MGRZ",      # Employment rate, LFS 16+, SA
        "lms/BCJD",      # Claimant count
        "retail/EAPB",   # Retail sales volume index, SA
        "hpi/UKHP",      # UK House Price Index
        "pusf/J5II",     # Public sector net borrowing
        "bop/HBOP",      # Current account balance
    ]
end

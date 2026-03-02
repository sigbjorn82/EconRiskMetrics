"""
    OECDSource.jl

Concrete implementation of DataSource for OECD.Stat data.
Direct HTTP wrapper for the OECD SDMX-JSON API.
No API key required.
"""

using HTTP
using JSON3
using Dates
using DataFrames

"""
    OECDSource <: DataSource

Data source for OECD.Stat via SDMX-JSON API.

# Fields
- `base_url::String`: OECD API base URL

# Constructor
    OECDSource()

# Identifier Format
`"DATASET/KEY"` — e.g. `"QNA/JPN.B1_GE.VOBARSA.Q"`

Key components are dot-separated dimension values; use `+` to combine
multiple members in one dimension (e.g. `"JPN+KOR"`).

# Example
```julia
oecd = OECDSource()

# Japan real GDP (quarterly, volume, seasonally adjusted)
gdp = fetch_data(oecd, "QNA/JPN.B1_GE.VOBARSA.Q")

# Japan + Korea CPI (monthly), returns :series column to distinguish
cpi = fetch_data(oecd, "PRICES_CPI/JPN+KOR.CPI010000.IXOB.M")

# Japan unemployment with date range
unemp = fetch_time_series(oecd, "STLABOUR/JPN.UNRTTE01.STSA.M",
                          start_date=Date(2010,1,1))
```

# Key Datasets
- `QNA`         — Quarterly National Accounts (GDP, components)
- `PRICES_CPI`  — Consumer Price Indices (monthly)
- `STLABOUR`    — Short-Term Labour Market (unemployment, employment)
- `MEI`         — Main Economic Indicators (broad monthly coverage)
- `KEI`         — Key Economic Indicators
- `OECD_BANKING`— Banking sector statistics
- `FIN_IND_FBS` — Financial sector indicators

# Country Codes
JPN, KOR, AUS, CHN, USA, GBR, DEU, FRA, ITA, CAN, NOR, SWE, DNK, NLD, ...
"""
struct OECDSource <: DataSource
    base_url::String
end

function OECDSource()
    return OECDSource("https://stats.oecd.org/SDMX-JSON/data")
end

"""Internal GET request to OECD SDMX-JSON API."""
function _oecd_request(source::OECDSource, dataset::String, key::String;
                       startTime::Union{String,Nothing}=nothing,
                       endTime::Union{String,Nothing}=nothing)
    url = "$(source.base_url)/$(dataset)/$(key)/all"
    params = Pair{String,String}["format" => "jsondata"]
    startTime !== nothing && push!(params, "startTime" => startTime)
    endTime   !== nothing && push!(params, "endTime"   => endTime)

    response = HTTP.request("GET", url, []; query=params)
    return JSON3.read(String(copy(response.body)))
end

"""Parse OECD period string to Date (daily, monthly, quarterly, annual)."""
function _parse_oecd_date(s::String)
    if length(s) == 10
        return Date(s, dateformat"yyyy-mm-dd")
    elseif length(s) == 7 && !occursin('Q', s)
        return Date(s * "-01", dateformat"yyyy-mm-dd")
    elseif occursin('Q', s)
        yr = parse(Int, s[1:4])
        q  = parse(Int, s[end])
        return Date(yr, (q - 1) * 3 + 1, 1)
    else
        return Date(parse(Int, s[1:4]), 1, 1)
    end
end

"""
Parse OECD SDMX-JSON response into a standardised DataFrame.

Returns `[:date, :value]` for a single series, or
`[:date, :value, :series]` when multiple series are present.
The `:series` value is the colon-separated dimension index key
(e.g. `"0:1:0:0"`) that maps into the series dimension values.
"""
function _parse_oecd_json(json)
    # Locate TIME_PERIOD in observation dimensions
    obs_dims = json.structure.dimensions.observation
    time_dim = nothing
    for d in obs_dims
        if string(d.id) == "TIME_PERIOD"
            time_dim = d
            break
        end
    end
    time_dim === nothing && error("TIME_PERIOD not found in OECD response")
    time_values = [string(v.id) for v in time_dim.values]

    series_dict = json.dataSets[1].series
    n_series    = length(series_dict)

    dates       = Date[]
    values      = Float64[]
    series_keys = String[]

    for (key_str, series_obj) in pairs(series_dict)
        sk = string(key_str)
        for (idx_str, obs_vals) in pairs(series_obj.observations)
            obs_vals[1] === nothing && continue
            idx = parse(Int, string(idx_str)) + 1   # 0-based → 1-based
            push!(dates,       _parse_oecd_date(time_values[idx]))
            push!(values,      Float64(obs_vals[1]))
            push!(series_keys, sk)
        end
    end

    result = n_series == 1 ?
        DataFrame(date=dates, value=values) :
        DataFrame(date=dates, value=values, series=series_keys)
    sort!(result, :date)
    return result
end

function fetch_data(source::OECDSource, identifier::String; kwargs...)
    try
        parts = split(identifier, '/')
        length(parts) == 2 ||
            error("OECD identifier must be 'DATASET/KEY', e.g. 'QNA/JPN.B1_GE.VOBARSA.Q'")
        dataset, key = String(parts[1]), String(parts[2])
        json = _oecd_request(source, dataset, key)
        return _parse_oecd_json(json)
    catch e
        throw(DataSourceError("OECDSource", "Failed to fetch '$identifier': $(e)"))
    end
end

function fetch_time_series(source::OECDSource, identifier::String;
                           start_date::Union{Date,Nothing}=nothing,
                           end_date::Union{Date,Nothing}=nothing, kwargs...)
    try
        parts = split(identifier, '/')
        length(parts) == 2 ||
            error("OECD identifier must be 'DATASET/KEY', e.g. 'QNA/JPN.B1_GE.VOBARSA.Q'")
        dataset, key = String(parts[1]), String(parts[2])

        # OECD accepts year strings for startTime/endTime; client-side filter corrects intra-year precision
        startTime = start_date !== nothing ? string(year(start_date)) : nothing
        endTime   = end_date   !== nothing ? string(year(end_date))   : nothing

        json = _oecd_request(source, dataset, key; startTime=startTime, endTime=endTime)
        df   = _parse_oecd_json(json)

        start_date !== nothing && (df = df[df.date .>= start_date, :])
        end_date   !== nothing && (df = df[df.date .<= end_date,   :])
        return df
    catch e
        throw(DataSourceError("OECDSource",
            "Failed to fetch '$identifier' ($(start_date) to $(end_date)): $(e)"))
    end
end

function validate_connection(source::OECDSource)
    try
        json = _oecd_request(source, "QNA", "JPN.B1_GE.VOBARSA.Q";
                             startTime="2023", endTime="2024")
        return length(json.dataSets) > 0
    catch
        return false
    end
end

function supports_asset_type(::OECDSource, asset_type::Symbol)
    return asset_type in [:economic_indicator, :gdp, :cpi, :unemployment, :interest_rate]
end

function get_metadata(::OECDSource, identifier::String)
    return Dict{String,Any}(
        "id"     => identifier,
        "source" => "OECD.Stat",
        "url"    => "https://stats.oecd.org/",
        "note"   => "Browse OECD datasets at: https://stats.oecd.org/",
    )
end

function list_available_series(::OECDSource; kwargs...)
    @warn "OECD uses DATASET/KEY identifiers. Browse at: https://stats.oecd.org/"
    return [
        "QNA/JPN.B1_GE.VOBARSA.Q",          # Japan real GDP (quarterly, vol, SA)
        "QNA/KOR.B1_GE.VOBARSA.Q",          # Korea real GDP (quarterly, vol, SA)
        "QNA/AUS.B1_GE.VOBARSA.Q",          # Australia real GDP (quarterly, vol, SA)
        "PRICES_CPI/JPN.CPI010000.IXOB.M",  # Japan CPI all items (monthly index)
        "PRICES_CPI/KOR.CPI010000.IXOB.M",  # Korea CPI all items (monthly index)
        "STLABOUR/JPN.UNRTTE01.STSA.M",     # Japan unemployment rate (monthly, SA)
        "STLABOUR/KOR.UNRTTE01.STSA.M",     # Korea unemployment rate (monthly, SA)
        "MEI/JPN.IR3TIB01.ST.M",            # Japan 3-month interbank rate (monthly)
        "MEI/KOR.IR3TIB01.ST.M",            # Korea 3-month interbank rate (monthly)
        "MEI/AUS.IR3TIB01.ST.M",            # Australia 3-month interbank rate (monthly)
    ]
end

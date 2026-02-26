"""
    ECBSource.jl

Concrete implementation of DataSource for European Central Bank statistical data.
Direct HTTP wrapper for the ECB Statistical Data Warehouse (SDW) SDMX 2.1 REST API.
No API key required.
"""

using HTTP
using JSON3
using Dates
using DataFrames

"""
    ECBSource <: DataSource

Data source for ECB Statistical Data Warehouse via SDMX 2.1 JSON API.

# Fields
- `base_url::String`: ECB API base URL

# Constructor
    ECBSource()

# Identifier Format
`"DATAFLOW/KEY"` — e.g. `"EXR/D.USD.EUR.SP00.A"`

Key breakdown for EXR (Exchange Rates):
- Frequency:  D (daily), M (monthly), Q (quarterly), A (annual)
- Currency:   USD, GBP, JPY, CHF, SEK, NOK, ...
- Base:       EUR (always EUR for ECB rates)
- Type:       SP00 (spot, average)
- Suffix:     A (average/close)

# Example
```julia
ecb = ECBSource()
eurusd = fetch_data(ecb, "EXR/D.USD.EUR.SP00.A")
eurgbp = fetch_time_series(ecb, "EXR/D.GBP.EUR.SP00.A", start_date=Date(2020,1,1))
hicp   = fetch_data(ecb, "ICP/M.U2.N.000000.4.ANR")  # HICP euro area inflation
```

# Key Datasets
- EXR  — Exchange rates
- ICP  — Inflation / HICP
- FM   — Financial markets (Euribor, OIS)
- MIR  — Monetary interest rates
- BSI  — Balance sheet items
"""
struct ECBSource <: DataSource
    base_url::String
end

function ECBSource()
    return ECBSource("https://data-api.ecb.europa.eu/service")
end

"""Fetch SDMX-JSON data from ECB API."""
function _ecb_request(source::ECBSource, flow::String, key::String;
                       startPeriod::Union{String,Nothing}=nothing,
                       endPeriod::Union{String,Nothing}=nothing)
    url = "$(source.base_url)/data/$(flow)/$(key)"
    params = Pair{String,String}["format" => "jsondata"]
    startPeriod !== nothing && push!(params, "startPeriod" => startPeriod)
    endPeriod   !== nothing && push!(params, "endPeriod"   => endPeriod)

    response = HTTP.request("GET", url, []; query=params)
    return JSON3.read(String(copy(response.body)))
end

"""Parse ECB period string to Date: daily yyyy-mm-dd, monthly yyyy-mm, quarterly yyyy-Qn, annual yyyy."""
function _parse_ecb_date(s::String)
    if length(s) == 10
        return Date(s, dateformat"yyyy-mm-dd")
    elseif length(s) == 7 && !occursin('Q', s)
        return Date(s * "-01", dateformat"yyyy-mm-dd")
    elseif occursin('Q', s)
        yr = parse(Int, s[1:4])
        q  = parse(Int, s[end])
        return Date(yr, (q - 1) * 3 + 1, 1)
    else
        return Date(parse(Int, s), 1, 1)
    end
end

"""Parse ECB SDMX-JSON response into standardized DataFrame."""
function _parse_ecb_json(json)
    # Find TIME_PERIOD in observation dimensions
    obs_dims = json.structure.dimensions.observation
    time_dim = nothing
    for d in obs_dims
        if string(d.id) == "TIME_PERIOD"
            time_dim = d
            break
        end
    end
    time_dim === nothing && error("TIME_PERIOD dimension not found in ECB response")
    time_values = [string(v.id) for v in time_dim.values]

    # Get observations from the first series (keys may be "0:0:0:0:0" etc.)
    observations = nothing
    for (_, v) in pairs(json.dataSets[1].series)
        observations = v.observations
        break
    end
    observations === nothing && error("No series data found in ECB response")

    dates  = Date[]
    values = Float64[]

    for (idx_str, obs) in pairs(observations)
        idx = parse(Int, string(idx_str)) + 1  # 0-based → 1-based
        obs[1] === nothing && continue
        push!(dates,  _parse_ecb_date(time_values[idx]))
        push!(values, Float64(obs[1]))
    end

    result = DataFrame(date=dates, value=values)
    sort!(result, :date)
    return result
end

function fetch_data(source::ECBSource, identifier::String; kwargs...)
    try
        parts = split(identifier, '/')
        length(parts) == 2 ||
            error("ECB identifier must be 'FLOW/KEY', e.g. 'EXR/D.USD.EUR.SP00.A'")
        flow, key = String(parts[1]), String(parts[2])
        json = _ecb_request(source, flow, key)
        return _parse_ecb_json(json)
    catch e
        throw(DataSourceError("ECBSource", "Failed to fetch '$identifier': $(e)"))
    end
end

function fetch_time_series(source::ECBSource, identifier::String;
                           start_date::Union{Date,Nothing}=nothing,
                           end_date::Union{Date,Nothing}=nothing, kwargs...)
    try
        parts = split(identifier, '/')
        length(parts) == 2 ||
            error("ECB identifier must be 'FLOW/KEY', e.g. 'EXR/D.USD.EUR.SP00.A'")
        flow, key = String(parts[1]), String(parts[2])

        startPeriod = start_date !== nothing ? string(start_date) : nothing
        endPeriod   = end_date   !== nothing ? string(end_date)   : nothing

        json = _ecb_request(source, flow, key; startPeriod=startPeriod, endPeriod=endPeriod)
        df = _parse_ecb_json(json)

        if start_date !== nothing
            df = df[df.date .>= start_date, :]
        end
        if end_date !== nothing
            df = df[df.date .<= end_date, :]
        end
        return df
    catch e
        throw(DataSourceError("ECBSource",
            "Failed to fetch time series '$identifier' ($(start_date) to $(end_date)): $(e)"))
    end
end

function validate_connection(source::ECBSource)
    try
        json = _ecb_request(source, "EXR", "D.USD.EUR.SP00.A";
                            startPeriod="2024-01-01", endPeriod="2024-01-05")
        return length(json.dataSets) > 0
    catch
        return false
    end
end

function supports_asset_type(source::ECBSource, asset_type::Symbol)
    return asset_type in [:forex, :interest_rate, :economic_indicator, :monetary]
end

function get_metadata(source::ECBSource, identifier::String)
    return Dict{String,Any}(
        "id"     => identifier,
        "source" => "European Central Bank",
        "url"    => "https://data.ecb.europa.eu/",
        "note"   => "Browse ECB datasets at: https://data.ecb.europa.eu/",
    )
end

function list_available_series(source::ECBSource; kwargs...)
    @warn "ECB uses FLOW/KEY identifiers. Browse at: https://data.ecb.europa.eu/"
    return [
        "EXR/D.USD.EUR.SP00.A",            # EUR/USD daily spot rate
        "EXR/D.GBP.EUR.SP00.A",            # EUR/GBP daily spot rate
        "EXR/D.JPY.EUR.SP00.A",            # EUR/JPY daily spot rate
        "EXR/M.USD.EUR.SP00.A",            # EUR/USD monthly
        "ICP/M.U2.N.000000.4.ANR",         # HICP euro area, all items, annual rate
        "ICP/M.DE.N.000000.4.ANR",         # HICP Germany
        "FM/B.U2.EUR.RT.MM.EURIBOR3MD_.HSTA",  # Euribor 3-month rate
        "FM/B.U2.EUR.RT.MM.ESTRVOLUME.HSTA",   # Euro Short-Term Rate (ESTR)
    ]
end

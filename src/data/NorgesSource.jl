"""
    NorgesSource.jl

Concrete implementation of DataSource for Norges Bank (Norway's
central bank) statistical data.
Direct HTTP wrapper for the Norges Bank SDMX 2.1 REST API.
No API key required.

API reference: https://data.norges-bank.no/api/
"""

using HTTP
using JSON3
using Dates
using DataFrames

"""
    NorgesSource <: DataSource

Data source for Norges Bank (Norway) via the SDMX 2.1 REST API.
No API key required.

Uses the same SDMX-JSON response format as ECBSource and OECDSource.

# Constructor
    NorgesSource()

# Identifier Format
`"FLOW/KEY"` — e.g. `"EXR/D.USD.NOK.SP"`

- `FLOW` — dataset code (e.g. `EXR`, `IR`, `GPFG`)
- `KEY`  — dot-separated dimension values (use `.` for all values in a position)

# Example
```julia
nb = NorgesSource()

# Daily NOK/USD exchange rate
usdnok = fetch_time_series(nb, "EXR/D.USD.NOK.SP",
                            start_date=Date(2020,1,1))

# Daily NOK/EUR exchange rate
eurnok = fetch_data(nb, "EXR/D.EUR.NOK.SP")

# Norges Bank key policy rate (monthly)
rate = fetch_data(nb, "IR/B.KPRA.SD")

# Government Pension Fund Global (annual)
gpfg = fetch_data(nb, "GPFG/A..")
```

# Key Series
- `"EXR/D.USD.NOK.SP"` — USD/NOK daily spot rate
- `"EXR/D.EUR.NOK.SP"` — EUR/NOK daily spot rate
- `"EXR/D.GBP.NOK.SP"` — GBP/NOK daily spot rate
- `"EXR/D.SEK.NOK.SP"` — SEK/NOK daily spot rate
- `"EXR/M.USD.NOK.SP"` — USD/NOK monthly average
- `"IR/B.KPRA.SD"`      — Norges Bank key policy rate
- `"GPFG/A.."`          — Government Pension Fund Global (annual)

# Browsing
Use `list_available_series(nb)` for curated list, or browse the
Norges Bank data portal at https://data.norges-bank.no.
"""
struct NorgesSource <: DataSource
    base_url::String
end

NorgesSource() = NorgesSource("https://data.norges-bank.no/api")

"""Parse a Norges Bank / SDMX period string to Date."""
function _parse_norges_date(s::String)
    s = strip(s)
    if length(s) == 10
        return Date(s, dateformat"yyyy-mm-dd")
    elseif length(s) == 7 && s[5] == '-'
        return Date(s * "-01", dateformat"yyyy-mm-dd")
    elseif length(s) == 7 && s[5] == 'Q'
        yr = parse(Int, s[1:4])
        q  = parse(Int, s[7])
        return Date(yr, (q - 1) * 3 + 1, 1)
    else
        return Date(parse(Int, s[1:4]), 1, 1)
    end
end

"""GET SDMX-JSON data from Norges Bank API."""
function _norges_request(source::NorgesSource, flow::String, key::String;
                          startPeriod::Union{String,Nothing}=nothing,
                          endPeriod::Union{String,Nothing}=nothing)
    url    = "$(source.base_url)/data/$(flow)/$(key)"
    params = Pair{String,String}["format" => "sdmx-json", "locale" => "en"]
    startPeriod !== nothing && push!(params, "startPeriod" => startPeriod)
    endPeriod   !== nothing && push!(params, "endPeriod"   => endPeriod)
    response = HTTP.request("GET", url; query=params)
    return JSON3.read(String(copy(response.body)))
end

"""Parse Norges Bank SDMX-JSON response into a DataFrame."""
function _parse_norges_json(json)
    data = json.data
    # Find TIME_PERIOD in observation dimensions
    obs_dims   = data.structure.dimensions.observation
    time_dim   = nothing
    for d in obs_dims
        string(d.id) == "TIME_PERIOD" && (time_dim = d; break)
    end
    time_dim === nothing && error("TIME_PERIOD dimension not found in response")
    time_values = [string(v.id) for v in time_dim.values]

    datasets = data.dataSets
    isempty(datasets) && return DataFrame(date=Date[], value=Float64[])

    all_dates  = Date[]
    all_values = Float64[]
    all_series = String[]

    for (series_key, series_obj) in pairs(datasets[1].series)
        observations = series_obj.observations
        for (idx_str, obs) in pairs(observations)
            idx = parse(Int, string(idx_str)) + 1   # 0-based → 1-based
            obs[1] === nothing && continue
            push!(all_dates,  _parse_norges_date(time_values[idx]))
            push!(all_values, Float64(obs[1]))
            push!(all_series, string(series_key))
        end
    end

    unique_series = unique(all_series)
    result = length(unique_series) == 1 ?
        DataFrame(date=all_dates, value=all_values) :
        DataFrame(date=all_dates, value=all_values, series=all_series)
    sort!(result, :date)
    return result
end

function fetch_data(source::NorgesSource, identifier::String; kwargs...)
    try
        idx = findfirst('/', identifier)
        idx === nothing &&
            error("Norges Bank identifier must be 'FLOW/KEY', e.g. 'EXR/D.USD.NOK.SP'")
        flow = identifier[1:idx-1]
        key  = identifier[idx+1:end]
        json = _norges_request(source, flow, key)
        return _parse_norges_json(json)
    catch e
        throw(DataSourceError("NorgesSource", "Failed to fetch '$identifier': $(e)"))
    end
end

function fetch_time_series(source::NorgesSource, identifier::String;
                           start_date::Union{Date,Nothing}=nothing,
                           end_date::Union{Date,Nothing}=nothing, kwargs...)
    try
        idx = findfirst('/', identifier)
        idx === nothing &&
            error("Norges Bank identifier must be 'FLOW/KEY', e.g. 'EXR/D.USD.NOK.SP'")
        flow = identifier[1:idx-1]
        key  = identifier[idx+1:end]

        sp = start_date !== nothing ? string(start_date) : nothing
        ep = end_date   !== nothing ? string(end_date)   : nothing

        json = _norges_request(source, flow, key; startPeriod=sp, endPeriod=ep)
        df   = _parse_norges_json(json)
        start_date !== nothing && (df = df[df.date .>= start_date, :])
        end_date   !== nothing && (df = df[df.date .<= end_date,   :])
        return df
    catch e
        throw(DataSourceError("NorgesSource",
            "Failed to fetch '$identifier' ($(start_date) to $(end_date)): $(e)"))
    end
end

function validate_connection(source::NorgesSource)
    try
        json = _norges_request(source, "EXR", "D.USD.NOK.SP";
                               startPeriod="2024-01-02", endPeriod="2024-01-05")
        return length(json.data.dataSets) > 0
    catch
        return false
    end
end

function supports_asset_type(::NorgesSource, asset_type::Symbol)
    return asset_type in [:forex, :interest_rate, :economic_indicator, :monetary]
end

function get_metadata(::NorgesSource, identifier::String)
    return Dict{String,Any}(
        "id"     => identifier,
        "source" => "Norges Bank",
        "url"    => "https://data.norges-bank.no",
        "note"   => "Browse Norges Bank datasets at https://data.norges-bank.no",
    )
end

function list_available_series(::NorgesSource; kwargs...)
    @warn "NorgesSource: Browse datasets at https://data.norges-bank.no"
    return [
        "EXR/D.USD.NOK.SP",   # USD/NOK daily spot rate
        "EXR/D.EUR.NOK.SP",   # EUR/NOK daily spot rate
        "EXR/D.GBP.NOK.SP",   # GBP/NOK daily spot rate
        "EXR/D.SEK.NOK.SP",   # SEK/NOK daily spot rate
        "EXR/D.DKK.NOK.SP",   # DKK/NOK daily spot rate
        "EXR/M.USD.NOK.SP",   # USD/NOK monthly average
        "EXR/M.EUR.NOK.SP",   # EUR/NOK monthly average
        "IR/B.KPRA.SD",       # Key policy rate
        "GPFG/A..",           # Government Pension Fund Global (annual)
    ]
end

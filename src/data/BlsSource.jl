"""
    BlsSource.jl

Concrete implementation of DataSource for US Bureau of Labor Statistics data.
Direct HTTP wrapper for the BLS Public Data API v2.
No API key required for basic access; free registration unlocks higher limits.
"""

using HTTP
using JSON3
using Dates
using DataFrames

"""
    BlsSource <: DataSource

Data source for US Bureau of Labor Statistics (BLS) via Public Data API v2.

# Fields
- `api_key::String`: BLS API key (empty string for unauthenticated access)

# Constructor
    BlsSource(api_key=nothing)

Reads from `BLS_API_KEY` environment variable if no key is passed.

# Example
```julia
bls = BlsSource()                    # no key (25 req/day, 3 years history)
bls = BlsSource("your_key_here")     # free key (500 req/day, 10 years)
unemp = fetch_data(bls, "LNS14000000")   # Unemployment rate
cpi   = fetch_data(bls, "CUUR0000SA0")  # CPI-U all items
```

# Free API Key
Register at: https://www.bls.gov/developers/home.htm

# Series Codes
- LNS14000000   — Unemployment Rate (seasonally adjusted)
- CES0000000001 — Total Nonfarm Payrolls
- CUUR0000SA0   — CPI-U All Items (not seasonally adjusted)
- CUSR0000SA0   — CPI-U All Items (seasonally adjusted)
- PRS85006092   — Nonfarm Business Sector: Labor Productivity
- WPU00000000   — Producer Price Index, All Commodities
"""
struct BlsSource <: DataSource
    api_key::String
end

function BlsSource(api_key::Union{AbstractString,Nothing}=nothing)
    key = if api_key !== nothing
        String(api_key)
    else
        k = get(ENV, "BLS_API_KEY", nothing)
        k !== nothing ? k : get(ENV, "BLS_KEY", "")
    end
    return BlsSource(key)
end

const _BLS_API_URL = "https://api.bls.gov/publicAPI/v2/timeseries/data/"

"""POST request to BLS API v2. Returns parsed JSON response."""
function _bls_request(source::BlsSource, series_id::String;
                       startyear::Union{Int,Nothing}=nothing,
                       endyear::Union{Int,Nothing}=nothing)
    body = Dict{String,Any}("seriesid" => [series_id])
    startyear !== nothing && (body["startyear"] = string(startyear))
    endyear   !== nothing && (body["endyear"]   = string(endyear))
    !isempty(source.api_key) && (body["registrationkey"] = source.api_key)

    response = HTTP.request(
        "POST", _BLS_API_URL,
        ["Content-Type" => "application/json"],
        JSON3.write(body),
    )
    json = JSON3.read(String(copy(response.body)))

    if json.status != "REQUEST_SUCCEEDED"
        msgs = join(json.message, "; ")
        error("BLS API error: $msgs")
    end
    return json
end

"""Parse a BLS period string (e.g. "M01", "Q02") and year into a Date. Returns nothing for annual averages."""
function _parse_bls_period(year::String, period::String)
    yr = parse(Int, year)
    if startswith(period, 'M') && period != "M13"
        return Date(yr, parse(Int, period[2:end]), 1)
    elseif startswith(period, 'Q')
        q = parse(Int, period[2:end])
        return Date(yr, (q - 1) * 3 + 1, 1)
    else
        return nothing  # M13 = annual average; skip
    end
end

"""Parse BLS series observations into a standardized DataFrame."""
function _parse_bls_series(series)
    dates  = Date[]
    values = Float64[]
    for obs in series.data
        d = _parse_bls_period(string(obs.year), string(obs.period))
        d === nothing && continue
        v = tryparse(Float64, string(obs.value))
        v === nothing && continue
        push!(dates, d)
        push!(values, v)
    end
    result = DataFrame(date=dates, value=values)
    sort!(result, :date)
    return result
end

function fetch_data(source::BlsSource, identifier::String; kwargs...)
    try
        json = _bls_request(source, identifier)
        return _parse_bls_series(json.Results.series[1])
    catch e
        throw(DataSourceError("BlsSource", "Failed to fetch '$identifier': $(e)"))
    end
end

function fetch_time_series(source::BlsSource, identifier::String;
                           start_date::Union{Date,Nothing}=nothing,
                           end_date::Union{Date,Nothing}=nothing, kwargs...)
    try
        # BLS API requires both startyear and endyear if either is given
        startyear = start_date !== nothing ? Dates.year(start_date) : nothing
        endyear   = end_date   !== nothing ? Dates.year(end_date)   :
                    start_date !== nothing ? Dates.year(today())     : nothing

        json = _bls_request(source, identifier; startyear=startyear, endyear=endyear)
        df = _parse_bls_series(json.Results.series[1])

        if start_date !== nothing
            df = df[df.date .>= start_date, :]
        end
        if end_date !== nothing
            df = df[df.date .<= end_date, :]
        end
        return df
    catch e
        throw(DataSourceError("BlsSource",
            "Failed to fetch time series '$identifier' ($(start_date) to $(end_date)): $(e)"))
    end
end

function validate_connection(source::BlsSource)
    try
        json = _bls_request(source, "LNS14000000")
        return json.status == "REQUEST_SUCCEEDED"
    catch
        return false
    end
end

function supports_asset_type(source::BlsSource, asset_type::Symbol)
    return asset_type in [:economic_indicator, :labor, :inflation]
end

function get_metadata(source::BlsSource, identifier::String)
    return Dict{String,Any}(
        "id"     => identifier,
        "source" => "Bureau of Labor Statistics",
        "url"    => "https://www.bls.gov/",
        "note"   => "Use series finder at: https://www.bls.gov/help/hlpforma.htm",
    )
end

function list_available_series(source::BlsSource; kwargs...)
    @warn "BLS uses series IDs. Browse at: https://www.bls.gov/data/ " *
          "or https://beta.bls.gov/dataQuery/find"
    return [
        "LNS14000000",   # Unemployment Rate (seasonally adjusted)
        "LNS11000000",   # Civilian Labor Force Level
        "CES0000000001", # Total Nonfarm Payrolls (thousands)
        "CUUR0000SA0",   # CPI-U All Items (not seasonally adjusted)
        "CUSR0000SA0",   # CPI-U All Items (seasonally adjusted)
        "WPU00000000",   # PPI All Commodities
        "PRS85006092",   # Nonfarm Business Sector: Labor Productivity
        "LEU0252881600", # Median Weekly Earnings
    ]
end

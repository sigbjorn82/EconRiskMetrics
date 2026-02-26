"""
    FredSource.jl

Concrete implementation of DataSource for FRED (Federal Reserve Economic Data).
Wraps the FredData.jl package to match the DataSource interface.
"""

using FredData
using Dates
using DataFrames
using HTTP
using JSON3

"""
    FredSource <: DataSource

Data source for Federal Reserve Economic Data (FRED).

Wraps the FredData.jl package to provide access to 800,000+ economic time series
from the St. Louis Federal Reserve.

# Fields
- `fred::Fred`: Wrapped FredData.Fred object
- `api_key::String`: FRED API key

# Constructor
    FredSource(api_key::Union{String,Nothing}=nothing)

Create a new FredSource instance.

# Arguments
- `api_key::Union{String,Nothing}`: FRED API key. If `nothing`, reads from ENV["FRED_API_KEY"]

# Example
```julia
# Using environment variable
fred = FredSource()

# Or pass API key directly
fred = FredSource("your_api_key_here")

# Fetch data
gdp = fetch_data(fred, "GDPC1")
```

# API Key Registration
Get your free API key at: https://fredaccount.stlouisfed.org/apikeys
"""
struct FredSource <: DataSource
    fred::Fred
    api_key::String
end

function FredSource(api_key::Union{AbstractString,Nothing}=nothing)
    # Try environment variable first, then constructor argument
    key = api_key !== nothing ? String(api_key) : get(ENV, "FRED_API_KEY", nothing)

    if key === nothing
        throw(DataSourceError("FredSource",
            "No API key provided. Set FRED_API_KEY environment variable or pass to constructor. " *
            "Get your API key at: https://fredaccount.stlouisfed.org/apikeys"))
    end

    try
        fred = Fred(key)
        return FredSource(fred, key)
    catch e
        throw(DataSourceError("FredSource", "Failed to initialize FRED connection: $(e)"))
    end
end

"""
    _fetch_observations_direct(source::FredSource, identifier::String; kwargs...) -> DataFrame

Direct HTTP fallback for fetching FRED observations.
Some series (e.g., SP500) exist in FRED but not ALFRED, causing FredData.jl's
metadata query to fail. This function fetches only observations, bypassing metadata.
"""
function _fetch_observations_direct(source::FredSource, identifier::String; kwargs...)
    base_url = "https://api.stlouisfed.org/fred/series/observations"
    params = Dict{String,String}(
        "api_key"   => source.api_key,
        "file_type" => "json",
        "series_id" => identifier
    )
    for (key, value) in kwargs
        params[string(key)] = string(value)
    end

    response = HTTP.request("GET", base_url, []; query=params)
    json = JSON3.read(String(copy(response.body)))

    n = length(json.observations)
    dates  = Vector{Date}(undef, n)
    values = Vector{Float64}(undef, n)
    for (i, obs) in enumerate(json.observations)
        dates[i] = Date(String(obs.date), "yyyy-mm-dd")
        values[i] = try
            parse(Float64, String(obs.value))
        catch
            NaN
        end
    end

    return DataFrame(date=dates, value=values)
end

"""
    fetch_data(source::FredSource, identifier::String; kwargs...) -> DataFrame

Fetch data from FRED for the given series identifier.

# Arguments
- `source::FredSource`: The FredSource instance
- `identifier::String`: FRED series ID (e.g., "GDPC1", "UNRATE", "DGS10")
- `kwargs...`: Additional parameters passed to FredData

# Returns
- `DataFrame`: Data with columns [:date, :value]

# Example
```julia
fred = FredSource()
gdp = fetch_data(fred, "GDPC1")  # Real GDP
unemployment = fetch_data(fred, "UNRATE")  # Unemployment rate
```
"""
function fetch_data(source::FredSource, identifier::String; kwargs...)
    try
        # Fetch data using FredData
        data = FredData.get_data(source.fred, identifier; kwargs...)

        # Convert to standardized DataFrame format
        df = DataFrame(
            date = data.data[:, :date],
            value = data.data[:, :value]
        )

        return df
    catch e
        # Some series (e.g., SP500) exist in FRED but not ALFRED.
        # FredData.jl's metadata query fails for these. Fall back to direct HTTP.
        if e isa HTTP.Exceptions.StatusError && e.status == 400
            @info "FredData metadata failed for '$identifier', using direct HTTP fallback"
            try
                return _fetch_observations_direct(source, identifier; kwargs...)
            catch e2
                throw(DataSourceError("FredSource", "Failed to fetch '$identifier': $(e2)"))
            end
        end
        throw(DataSourceError("FredSource", "Failed to fetch '$identifier': $(e)"))
    end
end

"""
    fetch_time_series(source::FredSource, identifier::String;
                      start_date=nothing, end_date=nothing, kwargs...) -> DataFrame

Fetch time series data within a specified date range.

# Arguments
- `source::FredSource`: The FredSource instance
- `identifier::String`: FRED series ID
- `start_date::Union{Date,Nothing}`: Start date (default: earliest available)
- `end_date::Union{Date,Nothing}`: End date (default: most recent)
- `kwargs...`: Additional parameters

# Returns
- `DataFrame`: Time series with columns [:date, :value]

# Example
```julia
fred = FredSource()
recent_gdp = fetch_time_series(fred, "GDPC1",
                                start_date=Date(2020, 1, 1),
                                end_date=Date(2024, 12, 31))
```
"""
function fetch_time_series(source::FredSource, identifier::String;
                          start_date::Union{Date,Nothing}=nothing,
                          end_date::Union{Date,Nothing}=nothing,
                          kwargs...)
    try
        # Build parameters for FredData
        params = Dict{Symbol,Any}()

        if start_date !== nothing
            params[:observation_start] = Dates.format(start_date, "yyyy-mm-dd")
        end

        if end_date !== nothing
            params[:observation_end] = Dates.format(end_date, "yyyy-mm-dd")
        end

        # Merge with additional kwargs
        params = merge(params, Dict(kwargs))

        # Try FredData first, fall back to direct HTTP for ALFRED-incompatible series
        df = try
            data = FredData.get_data(source.fred, identifier; params...)
            DataFrame(date = data.data[:, :date], value = data.data[:, :value])
        catch e
            if e isa HTTP.Exceptions.StatusError && e.status == 400
                @info "FredData metadata failed for '$identifier', using direct HTTP fallback"
                _fetch_observations_direct(source, identifier; params...)
            else
                rethrow(e)
            end
        end

        # Filter by date range
        if start_date !== nothing
            df = df[df.date .>= start_date, :]
        end

        if end_date !== nothing
            df = df[df.date .<= end_date, :]
        end

        return df
    catch e
        throw(DataSourceError("FredSource",
            "Failed to fetch time series '$identifier' ($(start_date) to $(end_date)): $(e)"))
    end
end

"""
    get_metadata(source::FredSource, identifier::String) -> Dict{String,Any}

Get metadata for a specific FRED series.

# Arguments
- `source::FredSource`: The FredSource instance
- `identifier::String`: FRED series ID

# Returns
- `Dict{String,Any}`: Metadata including title, units, frequency, seasonal_adjustment, notes, last_updated

# Example
```julia
fred = FredSource()
metadata = get_metadata(fred, "GDPC1")
println(metadata["title"])
println(metadata["units"])
```
"""
function get_metadata(source::FredSource, identifier::String)
    try
        # Fetch series data (which includes metadata)
        series = FredData.get_data(source.fred, identifier)

        # Extract relevant metadata fields
        metadata = Dict{String,Any}(
            "id" => FredData.id(series),
            "title" => FredData.title(series),
            "units" => FredData.units(series),
            "units_short" => FredData.units_short(series),
            "frequency" => FredData.freq(series),
            "frequency_short" => FredData.freq_short(series),
            "seasonal_adjustment" => FredData.seas_adj(series),
            "seasonal_adjustment_short" => FredData.seas_adj_short(series),
            "notes" => FredData.notes(series),
            "last_updated" => FredData.last_updated(series),
            "realtime_start" => FredData.realtime_start(series),
            "realtime_end" => FredData.realtime_end(series)
        )

        return metadata
    catch e
        throw(DataSourceError("FredSource", "Failed to fetch metadata for '$identifier': $(e)"))
    end
end

"""
    validate_connection(source::FredSource) -> Bool

Validate that the FRED API connection is working.

# Arguments
- `source::FredSource`: The FredSource instance

# Returns
- `Bool`: true if connection is valid, false otherwise

# Example
```julia
fred = FredSource()
if validate_connection(fred)
    println("FRED connection OK")
end
```
"""
function validate_connection(source::FredSource)
    try
        # Try to fetch a simple, always-available series (GDP)
        FredData.get_data(source.fred, "GDP")
        return true
    catch
        return false
    end
end

"""
    supports_asset_type(source::FredSource, asset_type::Symbol) -> Bool

Check if FredSource supports a given asset type.

FRED primarily provides economic indicators and some indices.

# Arguments
- `source::FredSource`: The FredSource instance
- `asset_type::Symbol`: Asset type to check

# Supported Types
- `:economic_indicator` - Yes
- `:index` - Yes
- `:equity` - No
- `:forex` - No
- `:options` - No

# Example
```julia
if supports_asset_type(fred, :economic_indicator)
    data = fetch_data(fred, "GDPC1")
end
```
"""
function supports_asset_type(source::FredSource, asset_type::Symbol)
    supported_types = [:economic_indicator, :index]
    return asset_type in supported_types
end

"""
    list_available_series(source::FredSource; kwargs...) -> Vector{String}

List available FRED series (search functionality).

Note: FRED has 800,000+ series, so this uses search parameters.

# Arguments
- `source::FredSource`: The FredSource instance
- `kwargs...`: Search parameters
  - `search_text::String`: Text to search for
  - `limit::Int`: Maximum number of results (default: 100)

# Returns
- `Vector{String}`: List of series IDs matching the search

# Example
```julia
fred = FredSource()
# Search is not fully implemented in this version
# For now, consult FRED website: https://fred.stlouisfed.org/
```
"""
function list_available_series(source::FredSource; kwargs...)
    # Note: FredData.jl may not have a direct search function
    # This is a placeholder implementation
    @warn "list_available_series is not fully implemented. " *
          "Please search for series at https://fred.stlouisfed.org/ " *
          "Common series: GDP, GDPC1, UNRATE, CPIAUCSL, DGS10, FEDFUNDS"

    # Return some common series as examples
    common_series = [
        "GDP",      # Gross Domestic Product
        "GDPC1",    # Real GDP
        "UNRATE",   # Unemployment Rate
        "CPIAUCSL", # Consumer Price Index
        "DGS10",    # 10-Year Treasury Rate
        "FEDFUNDS", # Federal Funds Rate
        "DEXUSEU",  # USD/EUR Exchange Rate
        "SP500",    # S&P 500 Index
        "VIXCLS",   # VIX Volatility Index
        "MORTGAGE30US" # 30-Year Mortgage Rate
    ]

    return common_series
end

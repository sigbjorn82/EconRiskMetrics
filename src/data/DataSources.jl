"""
    DataSources.jl

Abstract interface for all data sources in EconRiskMetrics.
Defines the common API that all data providers must implement.

All concrete data sources (FRED, Saxo Bank, World Bank, etc.) 
should inherit from `DataSource` and implement the required methods.
"""

module DataSources

using Dates
using DataFrames

export DataSource, DataSourceError
export FredSource, WorldBankSource, IMFSource, AlphaVantageSource, BankOfEnglandSource
export YFinanceSource, BlsSource, ECBSource, EurostatSource, OECDSource, BojSource
export EcosSource, MasSource, AdbSource
export StatCanSource, OnsSource, SnbSource, NorgesSource, EiaSource, RiksbanksSource
export SsbSource
export fetch_data, fetch_time_series, list_available_series
export get_metadata, validate_connection, supports_asset_type
export search_indicators

"""
    DataSource

Abstract type for all data sources.

All concrete implementations (FredSource, SaxoBankSource, etc.) 
must subtype this and implement the required methods.
"""
abstract type DataSource end

"""
    DataSourceError

Exception type for data source errors.
"""
struct DataSourceError <: Exception
    source::String
    message::String
end

Base.showerror(io::IO, e::DataSourceError) = 
    print(io, "DataSourceError from $(e.source): $(e.message)")

"""
    fetch_data(source::DataSource, identifier::String; kwargs...) -> DataFrame

Fetch data from a source given an identifier.

# Arguments
- `source::DataSource`: The data source instance
- `identifier::String`: Data identifier (e.g., FRED series code, ticker symbol)
- `kwargs...`: Additional parameters specific to the source

# Returns
- `DataFrame`: Data with at least columns [:date, :value] or similar

# Example
```julia
fred = FredSource("API_KEY")
data = fetch_data(fred, "GDPC1")
```
"""
function fetch_data(source::DataSource, identifier::String; kwargs...)
    throw(ErrorException("fetch_data not implemented for $(typeof(source))"))
end

"""
    fetch_time_series(source::DataSource, identifier::String; 
                      start_date=nothing, end_date=nothing, kwargs...) -> DataFrame

Fetch time series data within a date range.

# Arguments
- `source::DataSource`: The data source instance
- `identifier::String`: Series identifier
- `start_date::Union{Date,Nothing}`: Start date (default: earliest available)
- `end_date::Union{Date,Nothing}`: End date (default: most recent)
- `kwargs...`: Additional parameters

# Returns
- `DataFrame`: Time series with columns [:date, :value]
"""
function fetch_time_series(source::DataSource, identifier::String;
                          start_date::Union{Date,Nothing}=nothing,
                          end_date::Union{Date,Nothing}=nothing,
                          kwargs...)
    throw(ErrorException("fetch_time_series not implemented for $(typeof(source))"))
end

"""
    list_available_series(source::DataSource; kwargs...) -> Vector{String}

List available data series from this source.

# Arguments
- `source::DataSource`: The data source instance
- `kwargs...`: Filter parameters (category, search term, etc.)

# Returns
- `Vector{String}`: List of available series identifiers

# Example
```julia
fred = FredSource("API_KEY")
series = list_available_series(fred, category="GDP")
```
"""
function list_available_series(source::DataSource; kwargs...)
    throw(ErrorException("list_available_series not implemented for $(typeof(source))"))
end

"""
    get_metadata(source::DataSource, identifier::String) -> Dict{String,Any}

Get metadata for a specific data series.

# Arguments
- `source::DataSource`: The data source instance
- `identifier::String`: Series identifier

# Returns
- `Dict{String,Any}`: Metadata (description, units, frequency, etc.)

# Example
```julia
metadata = get_metadata(fred, "GDPC1")
println(metadata["title"])
println(metadata["units"])
```
"""
function get_metadata(source::DataSource, identifier::String)
    throw(ErrorException("get_metadata not implemented for $(typeof(source))"))
end

"""
    validate_connection(source::DataSource) -> Bool

Validate that the data source connection is working.

# Arguments
- `source::DataSource`: The data source instance

# Returns
- `Bool`: true if connection is valid, false otherwise

# Example
```julia
if validate_connection(fred)
    println("FRED connection OK")
end
```
"""
function validate_connection(source::DataSource)
    throw(ErrorException("validate_connection not implemented for $(typeof(source))"))
end

"""
    supports_asset_type(source::DataSource, asset_type::Symbol) -> Bool

Check if this data source supports a given asset type.

# Arguments
- `source::DataSource`: The data source instance
- `asset_type::Symbol`: Asset type (:equity, :forex, :options, :index, :economic_indicator, etc.)

# Returns
- `Bool`: true if supported, false otherwise

# Example
```julia
if supports_asset_type(saxo, :options)
    options_data = fetch_options_chain(saxo, "VIX")
end
```
"""
function supports_asset_type(source::DataSource, asset_type::Symbol)
    # Default implementation: assume not supported unless overridden
    return false
end

# Include concrete implementations
include("FredSource.jl")
include("WorldBankSource.jl")
include("IMFSource.jl")
include("AlphaVantageSource.jl")
include("BankOfEnglandSource.jl")
include("YFinanceSource.jl")
include("BlsSource.jl")
include("ECBSource.jl")
include("EurostatSource.jl")
include("OECDSource.jl")
include("BojSource.jl")
include("EcosSource.jl")
include("MasSource.jl")
include("AdbSource.jl")
include("StatCanSource.jl")
include("OnsSource.jl")
include("SnbSource.jl")
include("NorgesSource.jl")
include("EiaSource.jl")
include("RiksbanksSource.jl")
include("SsbSource.jl")

end # module DataSources

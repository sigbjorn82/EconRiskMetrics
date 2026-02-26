"""
    IMFSource.jl

Concrete implementation of DataSource for IMF International Financial Statistics.
Wraps the IMFData.jl package to match the DataSource interface.
"""

using IMFData
using Dates
using DataFrames

"""
    IMFSource <: DataSource

Data source for IMF International Financial Statistics (IFS).

# Fields
- `default_area::String`: Default country/area code (e.g., "US", "GB")
- `default_frequency::String`: Default frequency ("A", "Q", or "M")

# Constructor
    IMFSource(; area::String="US", frequency::String="Q")

# Example
```julia
imf = IMFSource()
cpi = fetch_data(imf, "PCPI_IX")  # Consumer Price Index

imf_uk = IMFSource(area="GB", frequency="M")
```
"""
struct IMFSource <: DataSource
    default_area::String
    default_frequency::String
end

function IMFSource(; area::String="US", frequency::String="Q")
    return IMFSource(area, frequency)
end

function fetch_data(source::IMFSource, identifier::String;
                    area::String=source.default_area,
                    frequency::String=source.default_frequency, kwargs...)
    try
        data = IMFData.get_ifs_data(area, identifier, frequency, 1950, Dates.year(today()))
        return _extract_imf_series(data, identifier, area)
    catch e
        throw(DataSourceError("IMFSource",
            "Failed to fetch '$identifier' for area '$area': $(e)"))
    end
end

function fetch_time_series(source::IMFSource, identifier::String;
                           start_date::Union{Date,Nothing}=nothing,
                           end_date::Union{Date,Nothing}=nothing,
                           area::String=source.default_area,
                           frequency::String=source.default_frequency, kwargs...)
    try
        startyear = start_date !== nothing ? Dates.year(start_date) : 1950
        endyear = end_date !== nothing ? Dates.year(end_date) : Dates.year(today())

        data = IMFData.get_ifs_data(area, identifier, frequency, startyear, endyear)
        df = _extract_imf_series(data, identifier, area)

        if start_date !== nothing
            df = df[df.date .>= start_date, :]
        end
        if end_date !== nothing
            df = df[df.date .<= end_date, :]
        end

        return df
    catch e
        throw(DataSourceError("IMFSource",
            "Failed to fetch time series '$identifier' ($(start_date) to $(end_date)): $(e)"))
    end
end

"""Extract DataFrame from IMFData return types (IfsSeries, IfsNotDefined, IfsNoData)."""
function _extract_imf_series(data, identifier::String, area::String)
    if data isa IMFData.IfsSeries
        df = copy(data.series)
        sort!(df, :date)
        return df
    elseif data isa IMFData.IfsNotDefined
        throw(DataSourceError("IMFSource",
            "Indicator '$identifier' is not defined for area '$area'"))
    elseif data isa IMFData.IfsNoData
        throw(DataSourceError("IMFSource",
            "No data available for '$identifier' in area '$area'"))
    else
        throw(DataSourceError("IMFSource",
            "Unexpected response type for '$identifier': $(typeof(data))"))
    end
end

function validate_connection(source::IMFSource)
    try
        datasets = IMFData.get_imf_datasets()
        return length(datasets) > 0
    catch
        return false
    end
end

function supports_asset_type(source::IMFSource, asset_type::Symbol)
    return asset_type in [:economic_indicator, :monetary, :balance_of_payments]
end

function get_metadata(source::IMFSource, identifier::String)
    return Dict{String,Any}(
        "id" => identifier,
        "dataset" => "IFS",
        "source" => "International Monetary Fund",
        "description" => "IFS indicator: $identifier",
        "url" => "https://data.imf.org/",
    )
end

function list_available_series(source::IMFSource; kwargs...)
    @warn "IMF series listing is limited. Browse indicators at: https://data.imf.org/"

    common_series = [
        "PCPI_IX",    # Consumer Price Index
        "NGDP",       # Nominal GDP
        "NGDP_R",     # Real GDP
        "ENDA_XDC_USD_RATE", # Exchange Rate
        "FII_SA",     # Interest Rates
        "TMG_CIF_USD", # Imports
        "TXG_FOB_USD", # Exports
    ]
    return common_series
end

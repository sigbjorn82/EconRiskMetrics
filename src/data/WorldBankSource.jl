"""
    WorldBankSource.jl

Concrete implementation of DataSource for World Bank Development Indicators.
Wraps the WorldBankData.jl package to match the DataSource interface.
"""

using WorldBankData
using Dates
using DataFrames

"""
    WorldBankSource <: DataSource

Data source for World Bank Development Indicators.

# Fields
- `default_country::String`: Default ISO 2-letter country code

# Constructor
    WorldBankSource(; country::String="US")

# Example
```julia
wb = WorldBankSource()
pop = fetch_data(wb, "SP.POP.TOTL")

wb_uk = WorldBankSource(country="GB")
gdp = fetch_data(wb_uk, "NY.GDP.MKTP.CD")
```
"""
struct WorldBankSource <: DataSource
    default_country::String
end

function WorldBankSource(; country::String="US")
    return WorldBankSource(country)
end

function fetch_data(source::WorldBankSource, identifier::String;
                    country::String=source.default_country, kwargs...)
    try
        df = WorldBankData.wdi(identifier, country)

        # Column name has dots replaced with underscores
        value_col = replace(identifier, "." => "_")

        result = DataFrame(
            date = Date.(Int.(df.year), 1, 1),
            value = Float64.(coalesce.(df[!, value_col], NaN))
        )
        sort!(result, :date)
        filter!(r -> !isnan(r.value), result)
        return result
    catch e
        throw(DataSourceError("WorldBankSource",
            "Failed to fetch '$identifier' for country '$country': $(e)"))
    end
end

function fetch_time_series(source::WorldBankSource, identifier::String;
                           start_date::Union{Date,Nothing}=nothing,
                           end_date::Union{Date,Nothing}=nothing,
                           country::String=source.default_country, kwargs...)
    try
        startyear = start_date !== nothing ? Dates.year(start_date) : 1960
        endyear = end_date !== nothing ? Dates.year(end_date) : Dates.year(today())

        df = WorldBankData.wdi(identifier, country, startyear, endyear)
        value_col = replace(identifier, "." => "_")

        result = DataFrame(
            date = Date.(Int.(df.year), 1, 1),
            value = Float64.(coalesce.(df[!, value_col], NaN))
        )
        sort!(result, :date)
        filter!(r -> !isnan(r.value), result)
        return result
    catch e
        throw(DataSourceError("WorldBankSource",
            "Failed to fetch time series '$identifier' ($(start_date) to $(end_date)): $(e)"))
    end
end

function validate_connection(source::WorldBankSource)
    try
        df = WorldBankData.wdi("SP.POP.TOTL", "US", 2020, 2020)
        return nrow(df) > 0
    catch
        return false
    end
end

function supports_asset_type(source::WorldBankSource, asset_type::Symbol)
    return asset_type in [:economic_indicator, :demographic]
end

function get_metadata(source::WorldBankSource, identifier::String)
    try
        results = WorldBankData.search_wdi("indicators", identifier)
        if nrow(results) > 0
            row = results[1, :]
            return Dict{String,Any}(
                "id" => identifier,
                "title" => string(row[:name]),
                "source" => "World Bank",
            )
        else
            return Dict{String,Any}("id" => identifier, "title" => "Unknown", "source" => "World Bank")
        end
    catch e
        throw(DataSourceError("WorldBankSource", "Failed to fetch metadata for '$identifier': $(e)"))
    end
end

function list_available_series(source::WorldBankSource; search_text::String="GDP", kwargs...)
    try
        results = WorldBankData.search_wdi("indicators", search_text)
        return String[string(r) for r in results[!, 1]]
    catch e
        @warn "World Bank indicator search failed. Browse at: https://data.worldbank.org/indicator"
        return String[]
    end
end

"""
    BankOfEnglandSource.jl

Concrete implementation of DataSource for Bank of England statistical data.
Custom HTTP wrapper for the BoE Interactive Statistical Database (IADB).
"""

using HTTP
using Dates
using DataFrames

"""
    BankOfEnglandSource <: DataSource

Data source for Bank of England statistical data via the IADB HTTP API.

# Fields
- `base_url::String`: BoE IADB endpoint URL

# Constructor
    BankOfEnglandSource()

# Example
```julia
boe = BankOfEnglandSource()
bank_rate = fetch_data(boe, "IUDBEDR")  # Bank Rate
gbp_usd = fetch_data(boe, "XUMAUSS")   # GBP/USD spot rate
```
"""
struct BankOfEnglandSource <: DataSource
    base_url::String
end

function BankOfEnglandSource()
    return BankOfEnglandSource(
        "http://www.bankofengland.co.uk/boeapps/iadb/fromshowcolumns.asp"
    )
end

function fetch_data(source::BankOfEnglandSource, identifier::String; kwargs...)
    try
        # BoE requires date range; default to last 5 years for daily series
        today_str = Dates.format(today(), "dd/u/yyyy")
        five_years_ago = Dates.format(today() - Year(5), "dd/u/yyyy")

        params = Dict{String,String}(
            "csv.x"       => "yes",
            "SeriesCodes" => identifier,
            "UsingCodes"  => "Y",
            "CSVF"        => "TN",
            "Datefrom"    => five_years_ago,
            "Dateto"      => today_str,
        )

        response = HTTP.request("GET", source.base_url, []; query=params)
        body = String(copy(response.body))
        return _parse_boe_csv(body)
    catch e
        throw(DataSourceError("BankOfEnglandSource", "Failed to fetch '$identifier': $(e)"))
    end
end

function fetch_time_series(source::BankOfEnglandSource, identifier::String;
                           start_date::Union{Date,Nothing}=nothing,
                           end_date::Union{Date,Nothing}=nothing, kwargs...)
    try
        params = Dict{String,String}(
            "csv.x"       => "yes",
            "SeriesCodes" => identifier,
            "UsingCodes"  => "Y",
            "CSVF"        => "TN",
        )

        if start_date !== nothing
            params["Datefrom"] = Dates.format(start_date, "dd/u/yyyy")
        end
        if end_date !== nothing
            params["Dateto"] = Dates.format(end_date, "dd/u/yyyy")
        end

        response = HTTP.request("GET", source.base_url, []; query=params)
        body = String(copy(response.body))
        df = _parse_boe_csv(body)

        if start_date !== nothing
            df = df[df.date .>= start_date, :]
        end
        if end_date !== nothing
            df = df[df.date .<= end_date, :]
        end

        return df
    catch e
        throw(DataSourceError("BankOfEnglandSource",
            "Failed to fetch time series '$identifier' ($(start_date) to $(end_date)): $(e)"))
    end
end

"""Parse CSV response from Bank of England IADB. Format: \"DATE,CODE\\n02 Jan 2024,5.25\"."""
function _parse_boe_csv(body::String)
    date_format = DateFormat("dd u yyyy")
    dates = Date[]
    values = Float64[]

    for line in split(body, "\n")
        line = strip(line)
        isempty(line) && continue
        startswith(uppercase(line), "DATE") && continue  # Skip header

        parts = split(line, ",")
        length(parts) < 2 && continue

        try
            d = Date(strip(parts[1]), date_format)
            v = parse(Float64, strip(parts[2]))
            push!(dates, d)
            push!(values, v)
        catch
            continue
        end
    end

    return DataFrame(date=dates, value=values)
end

function validate_connection(source::BankOfEnglandSource)
    try
        params = Dict("csv.x" => "yes", "SeriesCodes" => "IUDBEDR", "UsingCodes" => "Y",
                       "CSVF" => "TN", "Datefrom" => "01/Jan/2024", "Dateto" => "31/Jan/2024")
        response = HTTP.request("GET", source.base_url, []; query=params)
        return response.status == 200
    catch
        return false
    end
end

function supports_asset_type(source::BankOfEnglandSource, asset_type::Symbol)
    return asset_type in [:economic_indicator, :interest_rate, :monetary]
end

function get_metadata(source::BankOfEnglandSource, identifier::String)
    return Dict{String,Any}(
        "id" => identifier,
        "source" => "Bank of England",
        "url" => "https://www.bankofengland.co.uk/boeapps/database/",
        "note" => "Detailed metadata not available via API. Visit the BoE Interactive Statistical Database.",
    )
end

function list_available_series(source::BankOfEnglandSource; kwargs...)
    @warn "Bank of England does not provide a series search API. " *
          "Browse at: https://www.bankofengland.co.uk/boeapps/database/"

    return [
        "IUDBEDR",   # Bank Rate
        "IUDMNZC",   # Monthly average of UK base rate
        "XUMAERD",   # GBP/EUR daily spot rate
        "XUMAUSS",   # GBP/USD daily spot rate
        "LPMAUZI",   # CPI annual rate
    ]
end

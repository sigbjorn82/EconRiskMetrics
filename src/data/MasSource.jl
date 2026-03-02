"""
    MasSource.jl

Concrete implementation of DataSource for Monetary Authority of Singapore (MAS)
statistical data.
Direct HTTP wrapper for the MAS CKAN-based datastore API.
No API key required.

API reference: https://eservices.mas.gov.sg/api
Note: The API may be under scheduled maintenance at times.

Datasets (resource IDs and column names) are available at:
https://eservices.mas.gov.sg/api/action/package_list
"""

using HTTP
using JSON3
using Dates
using DataFrames

"""
    MasSource <: DataSource

Data source for Monetary Authority of Singapore (MAS) statistics via the
CKAN datastore API. No API key required.

# Constructor
    MasSource()

# Identifier Format
`"RESOURCE_ID/column_name"` — e.g.
`"95932927-c8bc-4e7a-b484-68a66a24edfe/usd_sgd"` (weekly USD/SGD FX rate)

The RESOURCE_ID is the UUID of the dataset table; column_name is one
value column within that table (date column is auto-detected).

# Example
```julia
mas = MasSource()

# Weekly USD/SGD exchange rate (all history)
fx = fetch_data(mas, "95932927-c8bc-4e7a-b484-68a66a24edfe/usd_sgd")

# Daily SORA overnight rate with date range
sora = fetch_time_series(mas, "9a0bf149-308c-4bd2-832d-76c8e6cb47ed/sora",
                         start_date=Date(2020,1,1))

# Monthly M1 money supply
m1 = fetch_data(mas, "a3a9e992-8a3f-43a4-9b45-59a7de73a0cd/m1")
```

# Key Datasets and Resource IDs

| Resource ID | Date Field | Description |
|---|---|---|
| `95932927-c8bc-4e7a-b484-68a66a24edfe` | `end_of_week` | FX rates (weekly) — usd_sgd, gbp_sgd, eur_sgd, jpy_sgd, ... |
| `5f2b18a8-0883-4769-a635-879c63d3caac` | `end_of_month` | FX rates (monthly average) |
| `9a0bf149-308c-4bd2-832d-76c8e6cb47ed` | `end_of_day` | SORA, SIBOR, SGS benchmark yields (daily) |
| `678527c5-f5b8-41ef-8e7d-4ef3d8f0c9ac` | `end_of_month` | SGS benchmark yields (monthly) |
| `a3a9e992-8a3f-43a4-9b45-59a7de73a0cd` | `end_of_month` | Money supply M1/M2/M3 (monthly) |

# Key Series
- `"95932927-c8bc-4e7a-b484-68a66a24edfe/usd_sgd"` — Weekly USD/SGD FX
- `"95932927-c8bc-4e7a-b484-68a66a24edfe/eur_sgd"` — Weekly EUR/SGD FX
- `"95932927-c8bc-4e7a-b484-68a66a24edfe/jpy_sgd"` — Weekly JPY/SGD FX
- `"9a0bf149-308c-4bd2-832d-76c8e6cb47ed/sora"` — Daily SORA overnight rate
- `"9a0bf149-308c-4bd2-832d-76c8e6cb47ed/_1m_sibor"` — 1-month SIBOR
- `"a3a9e992-8a3f-43a4-9b45-59a7de73a0cd/m1"` — M1 money supply
- `"a3a9e992-8a3f-43a4-9b45-59a7de73a0cd/m2"` — M2 money supply
- `"a3a9e992-8a3f-43a4-9b45-59a7de73a0cd/m3"` — M3 money supply

# Column Discovery
Use `get_metadata(mas, "RESOURCE_ID/any_column")` to list all available
columns in a dataset, or browse https://eservices.mas.gov.sg/api.

# Availability
The MAS API at `eservices.mas.gov.sg` undergoes **scheduled maintenance**
during which all requests return an HTML maintenance page instead of JSON,
causing `fetch_data` to throw a `DataSourceError`. Use
`validate_connection(mas)` to check availability before fetching. When the
API is down, try again later — maintenance windows are typically short.
"""
struct MasSource <: DataSource
    base_url::String
end

MasSource() = MasSource("https://eservices.mas.gov.sg/api/action/datastore")

"""Parse a MAS identifier string into (resource_id, column) tuple."""
function _parse_mas_identifier(identifier::String)
    # UUID is 36 chars (8-4-4-4-12), then '/', then column name
    idx = findfirst('/', identifier)
    idx === nothing &&
        error("MAS identifier must be 'RESOURCE_ID/column_name', " *
              "e.g. '95932927-c8bc-4e7a-b484-68a66a24edfe/usd_sgd'")
    resource_id = identifier[1:idx-1]
    column      = identifier[idx+1:end]
    isempty(resource_id) || isempty(column) &&
        error("MAS identifier must be 'RESOURCE_ID/column_name'")
    return resource_id, column
end

"""
Parse a MAS date string to Date.
Handles YYYY-MM-DD (daily/weekly), YYYY-MM (monthly), and YYYY (annual).
"""
function _parse_mas_date(s::String)
    s = strip(s)
    if length(s) == 10      # YYYY-MM-DD
        return Date(s, dateformat"yyyy-mm-dd")
    elseif length(s) == 7   # YYYY-MM
        return Date(s * "-01", dateformat"yyyy-mm-dd")
    elseif length(s) == 4   # YYYY
        return Date(parse(Int, s), 1, 1)
    else
        error("Unknown MAS date format: '$s'")
    end
end

"""
Fetch a single page from the MAS datastore.
Returns (records, date_field, total).
"""
function _mas_fetch_page(source::MasSource, resource_id::String,
                          offset::Int, limit::Int=100)
    params = Pair{String,String}[
        "resource_id" => resource_id,
        "limit"       => string(limit),
        "offset"      => string(offset),
        "fields"      => "",           # get all fields to detect date col on first call
    ]
    # On subsequent pages we could restrict fields, but for simplicity fetch all
    url      = "$(source.base_url)/search.json"
    response = HTTP.request("GET", url; query=params)
    body     = String(copy(response.body))
    json     = JSON3.read(body)

    json.success == true ||
        error("MAS API returned error: $(get(json, :error, "unknown"))")

    result     = json.result
    total      = Int(result.total)
    records    = result.records
    fields_arr = result.fields

    # Auto-detect date field: first field whose name contains a date keyword
    date_field = ""
    for f in fields_arr
        fname = lowercase(string(f.id))
        if occursin("date", fname) || occursin("week", fname) ||
           occursin("month", fname) || occursin("day", fname) || occursin("year", fname)
            date_field = string(f.id)
            break
        end
    end
    isempty(date_field) && !isempty(fields_arr) && (date_field = string(fields_arr[1].id))

    return records, date_field, total
end

"""
Fetch all pages from MAS datastore for a given resource/column, return DataFrame.
Paginates automatically (API limit is 100 records per request).
"""
function _mas_fetch_all(source::MasSource, resource_id::String, column::String)
    offset     = 0
    limit      = 100
    date_field = ""
    all_dates  = Date[]
    all_values = Float64[]

    while true
        records, df_name, total = _mas_fetch_page(source, resource_id, offset, limit)
        isempty(date_field) && (date_field = df_name)

        for rec in records
            # Get value
            val_raw = get(rec, Symbol(column), nothing)
            val_raw === nothing && continue
            val_str = string(val_raw)
            isempty(val_str) && continue
            v = tryparse(Float64, val_str)
            v === nothing && continue

            # Get date
            date_raw = get(rec, Symbol(date_field), nothing)
            date_raw === nothing && continue
            date_str = string(date_raw)
            isempty(date_str) && continue
            d = try _parse_mas_date(date_str) catch; continue end

            push!(all_dates,  d)
            push!(all_values, v)
        end

        offset += limit
        offset >= total && break
    end

    result = DataFrame(date=all_dates, value=all_values)
    sort!(result, :date)
    return result
end

function fetch_data(source::MasSource, identifier::String; kwargs...)
    try
        resource_id, column = _parse_mas_identifier(identifier)
        return _mas_fetch_all(source, resource_id, column)
    catch e
        throw(DataSourceError("MasSource", "Failed to fetch '$identifier': $(e)"))
    end
end

function fetch_time_series(source::MasSource, identifier::String;
                           start_date::Union{Date,Nothing}=nothing,
                           end_date::Union{Date,Nothing}=nothing, kwargs...)
    try
        df = fetch_data(source, identifier)
        start_date !== nothing && (df = df[df.date .>= start_date, :])
        end_date   !== nothing && (df = df[df.date .<= end_date,   :])
        return df
    catch e
        throw(DataSourceError("MasSource",
            "Failed to fetch '$identifier' ($(start_date) to $(end_date)): $(e)"))
    end
end

function validate_connection(source::MasSource)
    try
        # Attempt to fetch 1 record from the weekly FX dataset
        params = Pair{String,String}[
            "resource_id" => "95932927-c8bc-4e7a-b484-68a66a24edfe",
            "limit"       => "1",
        ]
        response = HTTP.request("GET", "$(source.base_url)/search.json"; query=params)
        json = JSON3.read(String(copy(response.body)))
        return json.success == true
    catch
        return false
    end
end

function supports_asset_type(::MasSource, asset_type::Symbol)
    return asset_type in [:forex, :interest_rate, :monetary, :economic_indicator]
end

function get_metadata(source::MasSource, identifier::String)
    try
        resource_id, column = _parse_mas_identifier(identifier)
        # Fetch 1 record to discover field names
        params = Pair{String,String}[
            "resource_id" => resource_id,
            "limit"       => "1",
        ]
        response = HTTP.request("GET", "$(source.base_url)/search.json"; query=params)
        json     = JSON3.read(String(copy(response.body)))
        json.success == true || error("API returned error")
        all_cols = [string(f.id) for f in json.result.fields]
        return Dict{String,Any}(
            "id"          => identifier,
            "resource_id" => resource_id,
            "column"      => column,
            "source"      => "Monetary Authority of Singapore",
            "total_rows"  => Int(json.result.total),
            "all_columns" => all_cols,
            "url"         => "https://eservices.mas.gov.sg/api",
        )
    catch
        return Dict{String,Any}(
            "id" => identifier, "source" => "Monetary Authority of Singapore",
            "url" => "https://eservices.mas.gov.sg/api",
        )
    end
end

function list_available_series(::MasSource; kwargs...)
    @warn "MasSource: Browse all datasets at https://eservices.mas.gov.sg/api"
    return [
        # Weekly FX rates (resource 95932927-...)
        "95932927-c8bc-4e7a-b484-68a66a24edfe/usd_sgd",
        "95932927-c8bc-4e7a-b484-68a66a24edfe/eur_sgd",
        "95932927-c8bc-4e7a-b484-68a66a24edfe/gbp_sgd",
        "95932927-c8bc-4e7a-b484-68a66a24edfe/jpy_sgd",
        "95932927-c8bc-4e7a-b484-68a66a24edfe/aud_sgd",
        # Monthly FX rates (resource 5f2b18a8-...)
        "5f2b18a8-0883-4769-a635-879c63d3caac/usd_sgd",
        "5f2b18a8-0883-4769-a635-879c63d3caac/eur_sgd",
        # Daily interbank rates / SGS yields (resource 9a0bf149-...)
        "9a0bf149-308c-4bd2-832d-76c8e6cb47ed/sora",
        "9a0bf149-308c-4bd2-832d-76c8e6cb47ed/_1m_sibor",
        "9a0bf149-308c-4bd2-832d-76c8e6cb47ed/_3m_sibor",
        "9a0bf149-308c-4bd2-832d-76c8e6cb47ed/_6m_sibor",
        # Monthly SGS benchmark yields (resource 678527c5-...)
        "678527c5-f5b8-41ef-8e7d-4ef3d8f0c9ac/tndr_yld_3m",
        "678527c5-f5b8-41ef-8e7d-4ef3d8f0c9ac/tndr_yld_10y",
        # Money supply (resource a3a9e992-...)
        "a3a9e992-8a3f-43a4-9b45-59a7de73a0cd/m1",
        "a3a9e992-8a3f-43a4-9b45-59a7de73a0cd/m2",
        "a3a9e992-8a3f-43a4-9b45-59a7de73a0cd/m3",
    ]
end

"""
    SsbSource.jl

Concrete implementation of DataSource for Statistics Norway (SSB).
Direct HTTP wrapper for the PxWeb JSON-stat 2.0 API.
No API key required.
"""

using HTTP
using JSON3
using Dates
using DataFrames

"""
    SsbSource <: DataSource

Data source for Statistics Norway (Statistisk sentralbyrå) via PxWeb API.

# Fields
- `base_url::String`: SSB API base URL

# Constructor
    SsbSource()

# Identifier Format
`"tableId/ContentsCode"` e.g. `"08801/KPI"` (CPI monthly).

Use GET `https://data.ssb.no/api/v0/en/table/{tableId}` to inspect available
ContentsCode values for any given table.

# Key Tables
- 08801/KPI           — Consumer Price Index (CPI), monthly
- 09189/BNP           — GDP at current prices (NOK million), quarterly
- 07129/AKU_SYSAV     — Unemployment rate (LFS, seasonally adjusted), quarterly
- 06946/PRINDEKS      — Housing price index (2015=100), quarterly
- 10945/DRIFTSRESULTAT — Current account balance (NOK million), quarterly
"""
struct SsbSource <: DataSource
    base_url::String
end

function SsbSource()
    return SsbSource("https://data.ssb.no/api/v0/en/table")
end

"""Parse SSB PxWeb date string to Date.
Formats: "2024M01" monthly, "2024K1" quarterly (K=kvartal), "2024" annual."""
function _ssb_parse_date(s::String)
    if occursin('M', s)                   # "2024M01"
        yr = parse(Int, s[1:4])
        mo = parse(Int, s[6:end])
        return Date(yr, mo, 1)
    elseif occursin('K', s)               # "2024K1" (kvartal = quarter)
        yr = parse(Int, s[1:4])
        q  = parse(Int, s[end])
        return Date(yr, (q - 1) * 3 + 1, 1)
    else                                  # "2024" annual
        return Date(parse(Int, s), 1, 1)
    end
end

"""Parse identifier "tableId/ContentsCode" into (table_id, contents_code)."""
function _ssb_parse_id(identifier::String)
    parts = split(identifier, "/"; limit=2)
    length(parts) == 2 ||
        error("SsbSource: identifier must be 'tableId/ContentsCode', got '$identifier'")
    return String(parts[1]), String(parts[2])
end

"""
GET table metadata from SSB. Returns JSON3 object with `variables` array.
Each variable has `code`, `values`, `valueTexts`, and optional `time=true`.
"""
function _ssb_get_table_meta(source::SsbSource, table_id::String)
    url      = "$(source.base_url)/$(table_id)"
    response = HTTP.request("GET", url, ["Accept" => "application/json"])
    return JSON3.read(String(copy(response.body)))
end

"""
POST a PxWeb query to SSB and return the parsed JSON-stat 2.0 response.
All time periods are requested; caller filters by date range.
"""
function _ssb_request(source::SsbSource, table_id::String, contents_code::String)
    meta = _ssb_get_table_meta(source, table_id)

    # Identify dimension codes: time variable and contents variable
    time_var     = "Tid"          # SSB default; overridden if metadata says otherwise
    contents_var = "ContentsCode" # SSB default
    for v in meta.variables
        code = String(v.code)
        if get(v, :time, false) == true
            time_var = code
        elseif lowercase(code) in ("contentscode", "contents")
            contents_var = code
        end
    end

    body = JSON3.write(Dict(
        "query" => [
            Dict("code" => contents_var,
                 "selection" => Dict("filter" => "item", "values" => [contents_code])),
            Dict("code" => time_var,
                 "selection" => Dict("filter" => "all", "values" => ["*"])),
        ],
        "response" => Dict("format" => "json-stat2"),
    ))

    url      = "$(source.base_url)/$(table_id)"
    response = HTTP.request("POST", url,
                            ["Content-Type" => "application/json"], body)
    return JSON3.read(String(copy(response.body)))
end

"""
Parse SSB JSON-stat 2.0 response into a standardised DataFrame.

The time dimension is identified by the name "Tid"; if absent, the last
dimension is used (standard PxWeb convention). Non-time dimensions are
expected to be size 1 (i.e. a single ContentsCode was selected).
"""
function _ssb_parse_json(json)
    dim_ids   = [string(id) for id in json.id]
    dim_sizes = [Int(s) for s in json.size]

    # Locate time dimension — prefer "Tid", fall back to last
    time_pos = findfirst(==("Tid"), dim_ids)
    if time_pos === nothing
        time_pos = length(dim_ids)
    end
    n_times      = dim_sizes[time_pos]
    time_dim_sym = Symbol(dim_ids[time_pos])

    # Build ordered array of time labels from category.index (label → 0-based pos)
    time_cat_index = json.dimension[time_dim_sym].category.index
    time_labels    = Vector{String}(undef, n_times)
    for (label, pos) in pairs(time_cat_index)
        time_labels[Int(pos) + 1] = string(label)
    end

    # How many elements to skip per time step in the flat value array
    step_after = prod(view(dim_sizes, (time_pos + 1):length(dim_sizes)); init=1)

    # Build value map: 1-based flat index → Float64 (nulls omitted)
    raw = json.value
    value_map = if raw isa JSON3.Array
        Dict(i => Float64(raw[i]) for i in 1:length(raw) if raw[i] !== nothing)
    else
        Dict(parse(Int, string(k)) + 1 => Float64(v) for (k, v) in pairs(raw)
             if v !== nothing)
    end

    dates  = Date[]
    values = Float64[]
    for (i, label) in enumerate(time_labels)
        flat_idx = (i - 1) * step_after + 1
        haskey(value_map, flat_idx) || continue
        push!(dates,  _ssb_parse_date(label))
        push!(values, value_map[flat_idx])
    end

    result = DataFrame(date=dates, value=values)
    sort!(result, :date)
    return result
end

function fetch_data(source::SsbSource, identifier::String; kwargs...)
    try
        table_id, contents_code = _ssb_parse_id(identifier)
        json = _ssb_request(source, table_id, contents_code)
        return _ssb_parse_json(json)
    catch e
        throw(DataSourceError("SsbSource", "Failed to fetch '$identifier': $(e)"))
    end
end

function fetch_time_series(source::SsbSource, identifier::String;
                           start_date::Union{Date,Nothing}=nothing,
                           end_date::Union{Date,Nothing}=nothing,
                           kwargs...)
    try
        table_id, contents_code = _ssb_parse_id(identifier)
        json = _ssb_request(source, table_id, contents_code)
        df   = _ssb_parse_json(json)

        if start_date !== nothing
            df = df[df.date .>= start_date, :]
        end
        if end_date !== nothing
            df = df[df.date .<= end_date, :]
        end
        return df
    catch e
        throw(DataSourceError("SsbSource",
            "Failed to fetch time series '$identifier': $(e)"))
    end
end

function validate_connection(::SsbSource)
    try
        url      = "https://data.ssb.no/api/v0/en/table/08801"
        response = HTTP.request("GET", url, ["Accept" => "application/json"])
        json     = JSON3.read(String(copy(response.body)))
        return haskey(json, :variables)
    catch
        return false
    end
end

function supports_asset_type(::SsbSource, asset_type::Symbol)
    return asset_type in [:economic_indicator, :inflation, :labor, :housing]
end

function get_metadata(source::SsbSource, identifier::String)
    try
        table_id, contents_code = _ssb_parse_id(identifier)
        meta  = _ssb_get_table_meta(source, table_id)
        title = get(meta, :title, identifier)

        # Resolve the human-readable label for the ContentsCode
        series_label = contents_code
        for v in meta.variables
            code = String(v.code)
            if lowercase(code) in ("contentscode", "contents")
                vals = [String(x) for x in v.values]
                idx  = findfirst(==(contents_code), vals)
                if idx !== nothing
                    series_label = String(v.valueTexts[idx])
                end
                break
            end
        end

        return Dict{String,Any}(
            "id"     => identifier,
            "title"  => string(title),
            "series" => series_label,
            "source" => "Statistics Norway (SSB)",
            "url"    => "https://www.ssb.no/statbank/table/$(table_id)/",
        )
    catch
        return Dict{String,Any}(
            "id"     => identifier,
            "source" => "Statistics Norway (SSB)",
            "url"    => "https://www.ssb.no/statbank/",
        )
    end
end

function list_available_series(::SsbSource; kwargs...)
    return [
        "08801/KPI",            # Consumer Price Index (CPI), monthly
        "09189/BNP",            # GDP at current prices (NOK million), quarterly
        "07129/AKU_SYSAV",      # Unemployment rate (LFS, seasonally adjusted), quarterly
        "06946/PRINDEKS",       # Housing price index (2015=100), quarterly
        "10945/DRIFTSRESULTAT", # Current account balance, quarterly
    ]
end

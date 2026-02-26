"""
    EurostatSource.jl

Concrete implementation of DataSource for Eurostat EU statistics.
Direct HTTP wrapper for the Eurostat Data Browser Statistics API (JSON-stat 2.0).
No API key required.
"""

using HTTP
using JSON3
using Dates
using DataFrames

"""
    EurostatSource <: DataSource

Data source for Eurostat EU economic statistics via JSON-stat 2.0 API.

# Fields
- `base_url::String`: Eurostat API base URL

# Constructor
    EurostatSource()

# Identifier Format
Dataset code (e.g. `"prc_hicp_midx"`). Dimension filters are passed as keyword args
to narrow the response to a single time series.

# Example
```julia
es = EurostatSource()

# HICP inflation, EU27, all items, index 2015=100
hicp = fetch_data(es, "prc_hicp_midx"; geo="EU27_2020", coicop="CP00", unit="I15")

# GDP for Germany, current prices million EUR
gdp = fetch_data(es, "nama_10_gdp"; geo="DE", na_item="B1GQ", unit="CP_MEUR")

# Monthly unemployment rate, EU27, seasonally adjusted
unemp = fetch_data(es, "une_rt_m"; geo="EU27_2020", age="TOTAL", sex="T", unit="PC_ACT", s_adj="SA")
```

# Key Datasets
- prc_hicp_midx — HICP inflation indices
- nama_10_gdp   — GDP and main components (annual)
- une_rt_m      — Unemployment rates (monthly)
- irt_euryld_d  — Euro area yield curves (daily)
- demo_gind     — Population indicators
"""
struct EurostatSource <: DataSource
    base_url::String
end

function EurostatSource()
    return EurostatSource(
        "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data"
    )
end

"""Fetch JSON-stat 2.0 data from Eurostat API. Dimension filters passed as kwargs."""
function _eurostat_request(source::EurostatSource, dataset::String; kwargs...)
    url = "$(source.base_url)/$(dataset)"
    params = Pair{String,String}["lang" => "EN", "format" => "JSON"]
    for (k, v) in kwargs
        push!(params, string(k) => string(v))
    end

    response = HTTP.request("GET", url, []; query=params)
    return JSON3.read(String(copy(response.body)))
end

"""Parse Eurostat period string to Date: annual yyyy, monthly yyyyMmm, quarterly yyyyQn."""
function _parse_eurostat_date(s::String)
    if length(s) == 4
        return Date(parse(Int, s), 1, 1)
    elseif occursin('M', s)
        yr = parse(Int, s[1:4])
        mo = parse(Int, s[6:end])
        return Date(yr, mo, 1)
    elseif occursin('Q', s)
        yr = parse(Int, s[1:4])
        q  = parse(Int, s[end])
        return Date(yr, (q - 1) * 3 + 1, 1)
    else
        return Date(s)
    end
end

"""
Parse Eurostat JSON-stat 2.0 response into a standardized DataFrame.

Dimension IDs and sizes are at the JSON root level. When dimension filters
narrow all non-time dimensions to size 1, value[i] maps directly to time[i].
A warning is emitted if multiple dimension combinations are detected.
"""
function _parse_eurostat_json(json)
    dim_ids   = [string(id) for id in json.id]
    dim_sizes = [Int(s) for s in json.size]

    time_pos = findfirst(==("time"), dim_ids)
    time_pos === nothing && error("No 'time' dimension found in Eurostat response")
    n_times = dim_sizes[time_pos]

    # Get ordered time labels from category.index (maps label → 0-based position)
    time_cat_index = json.dimension.time.category.index
    time_labels = Vector{String}(undef, n_times)
    for (label, pos) in pairs(time_cat_index)
        time_labels[Int(pos) + 1] = string(label)
    end

    # Step size through the flat value array for the time dimension
    step_after = prod(view(dim_sizes, (time_pos + 1):length(dim_sizes)); init=1)
    non_time_total = prod(dim_sizes; init=1) ÷ n_times
    if non_time_total > 1
        @warn "Multiple non-time dimension values detected ($(non_time_total) combinations). " *
              "Add dimension filters (e.g. geo=, unit=) to select a single time series. " *
              "Returning the first combination only."
    end

    # Eurostat returns values in two formats:
    #   Dense array:  [v0, null, v2, ...]   — JSON3.Array, 1-based indexing
    #   Sparse dict:  {"0": v0, "2": v2, …} — JSON3.Object, 0-based string keys (nulls omitted)
    raw = json.value
    value_map = if raw isa JSON3.Array
        Dict(i => Float64(raw[i]) for i in 1:length(raw) if raw[i] !== nothing)
    else
        Dict(parse(Int, string(k)) + 1 => Float64(v) for (k, v) in pairs(raw) if v !== nothing)
    end

    dates  = Date[]
    values = Float64[]

    for (i, label) in enumerate(time_labels)
        flat_idx = (i - 1) * step_after + 1
        haskey(value_map, flat_idx) || continue
        push!(dates,  _parse_eurostat_date(label))
        push!(values, value_map[flat_idx])
    end

    result = DataFrame(date=dates, value=values)
    sort!(result, :date)
    return result
end

function fetch_data(source::EurostatSource, identifier::String; kwargs...)
    try
        json = _eurostat_request(source, identifier; kwargs...)
        return _parse_eurostat_json(json)
    catch e
        throw(DataSourceError("EurostatSource", "Failed to fetch '$identifier': $(e)"))
    end
end

function fetch_time_series(source::EurostatSource, identifier::String;
                           start_date::Union{Date,Nothing}=nothing,
                           end_date::Union{Date,Nothing}=nothing,
                           kwargs...)
    try
        filter_kwargs = Dict{Symbol,Any}(kwargs)
        if start_date !== nothing
            filter_kwargs[:sinceTimePeriod] = Dates.year(start_date)
        end

        json = _eurostat_request(source, identifier; filter_kwargs...)
        df = _parse_eurostat_json(json)

        if start_date !== nothing
            df = df[df.date .>= start_date, :]
        end
        if end_date !== nothing
            df = df[df.date .<= end_date, :]
        end
        return df
    catch e
        throw(DataSourceError("EurostatSource",
            "Failed to fetch time series '$identifier' ($(start_date) to $(end_date)): $(e)"))
    end
end

function validate_connection(source::EurostatSource)
    try
        json = _eurostat_request(source, "prc_hicp_midx";
                                 geo="EU27_2020", coicop="CP00", unit="I15",
                                 lastTimePeriod=2)
        return haskey(json, :value)
    catch
        return false
    end
end

function supports_asset_type(source::EurostatSource, asset_type::Symbol)
    return asset_type in [:economic_indicator, :demographic, :labor, :inflation]
end

function get_metadata(source::EurostatSource, identifier::String)
    try
        json = _eurostat_request(source, identifier; lastTimePeriod=1)
        label = get(json, :label, identifier)
        return Dict{String,Any}(
            "id"     => identifier,
            "title"  => string(label),
            "source" => "Eurostat",
            "url"    => "https://ec.europa.eu/eurostat/databrowser/view/$(identifier)/",
        )
    catch
        return Dict{String,Any}(
            "id"     => identifier,
            "source" => "Eurostat",
            "url"    => "https://ec.europa.eu/eurostat/databrowser/view/$(identifier)/",
        )
    end
end

function list_available_series(source::EurostatSource; kwargs...)
    @warn "Eurostat datasets require dimension filters. " *
          "Browse at: https://ec.europa.eu/eurostat/databrowser/"
    return [
        # Dataset code => suggested filters (as comments)
        "prc_hicp_midx",  # HICP: geo=EU27_2020, coicop=CP00, unit=I15
        "nama_10_gdp",    # GDP:  geo=DE, na_item=B1GQ, unit=CP_MEUR
        "une_rt_m",       # Unemployment: geo=EU27_2020, age=TOTAL, sex=T, unit=PC_ACT, s_adj=SA
        "irt_euryld_d",   # Euro yield curves: geo=EA, maturity=Y10, yld_curv=MROE
        "demo_gind",      # Population: geo=EU27_2020, indic_de=JAN
        "ei_bssi_m_r2",   # Business surveys: geo=EU, s_adj=SA, indic=BS-ESI-I
    ]
end

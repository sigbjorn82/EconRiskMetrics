"""
    AdbSource.jl

Concrete implementation of DataSource for Asian Development Bank (ADB)
Key Indicators Database (KIDB).
HTTP wrapper for the KIDB web API at kidb.adb.org.
No API key required.

The KIDB data export uses an async workflow (submit → poll → download)
protected by CSRF tokens. Data is primarily annual frequency.

KIDB portal: https://kidb.adb.org
"""

using HTTP
using JSON3
using Dates
using DataFrames

"""
    AdbSource <: DataSource

Data source for Asian Development Bank (ADB) Key Indicators Database (KIDB).
Covers 49 Asia-Pacific developing member economies with macroeconomic,
financial, and social indicators. Data is primarily annual frequency.
No API key required.

# Constructor
    AdbSource()

# Identifier Format
`"ECONOMY_CODE/INDICATOR_ID"` — e.g. `"PHI/1200004"` (Philippines GDP)

Economy codes are ADB-specific (not ISO); indicator IDs are numeric.
Use `list_available_series(adb)` or `search_indicators(adb, "GDP")` to
discover identifiers.

# Example
```julia
adb = AdbSource()

# Search for GDP indicators
hits = search_indicators(adb, "GDP")
# => [{name="GDP at current prices", code=1200004, type="indicator"}, ...]

# Fetch Philippines GDP (annual)
gdp = fetch_data(adb, "PHI/1200004")

# Date-filtered fetch (annual data, so dates snap to Jan 1)
gdp = fetch_time_series(adb, "PHI/1200004",
                         start_date=Date(2000,1,1),
                         end_date=Date(2023,1,1))
```

# Key Economy Codes
- `PHI` — Philippines      - `IND` — India
- `CHN` / `PRC` — China    - `INO` — Indonesia
- `JPN` — Japan             - `KOR` — Korea
- `BAN` — Bangladesh        - `PAK` — Pakistan
- `VIE` — Vietnam           - `THA` — Thailand
- `MAL` — Malaysia          - `SIN` — Singapore
- `SRI` — Sri Lanka         - `NEP` — Nepal

# Key Indicator IDs (search via `search_indicators`)
- `1200004`  — GDP at current prices
- `1200011`  — GDP per capita (current USD)
- `1200021`  — GDP growth rate (%)
- `1200140`  — Inflation / CPI (% change)
- `1200100`  — Population, total

# Notes
Data fetching uses an async CSRF-protected export workflow. If the
server-side job fails, a `DataSourceError` is thrown with guidance.
Use `search_indicators(adb, keyword)` and `get_metadata(adb, id)` for
reliable discovery even when data export is unavailable.
"""
struct AdbSource <: DataSource
    base_url::String
end

AdbSource() = AdbSource("https://kidb.adb.org")

"""Parse ADB identifier into (economy_code, indicator_id) tuple."""
function _parse_adb_identifier(identifier::String)
    idx = findfirst('/', identifier)
    idx === nothing &&
        error("ADB identifier must be 'ECONOMY_CODE/INDICATOR_ID', e.g. 'PHI/1200004'")
    eco = identifier[1:idx-1]
    ind = identifier[idx+1:end]
    (isempty(eco) || isempty(ind)) &&
        error("ADB identifier must be 'ECONOMY_CODE/INDICATOR_ID', e.g. 'PHI/1200004'")
    ind_id = tryparse(Int, ind)
    ind_id === nothing && error("Indicator ID must be numeric, got: '$ind'")
    return eco, ind_id
end

"""
Fetch CSRF token and session cookies from the ADB explore page.
Returns (csrf_token, cookie_header_string).
"""
function _adb_get_session(source::AdbSource)
    resp = HTTP.get("$(source.base_url)/explore",
                    ["User-Agent" => "Mozilla/5.0 (compatible; Julia/HTTP.jl)"])
    body = String(copy(resp.body))

    # Extract CSRF token from <meta name="csrf-token" content="...">
    m    = match(r"""name="csrf-token" content="([^"]+)""", body)
    csrf = m !== nothing ? m.captures[1] : ""

    # Collect Set-Cookie headers into a single Cookie header value
    cookies = String[]
    for h in resp.headers
        lowercase(h.first) == "set-cookie" || continue
        # Keep only the name=value part (before the first ';')
        push!(cookies, String(split(h.second, ';')[1]))
    end
    cookie_str = join(cookies, "; ")

    return csrf, cookie_str
end

"""
Submit an export job to the ADB KIDB.
Returns the job UUID, or throws on HTTP/CSRF error.
"""
function _adb_submit_export(source::AdbSource, economy_code::String, indicator_id::Int,
                             years::Vector{Int}, csrf::String, cookie_str::String)
    body = JSON3.write(Dict(
        "filter"   => Dict(
            "indicator_id" => [indicator_id],
            "economy_code" => [economy_code],
            "year"         => years,
        ),
        "grouping" => "indicators",
        "type"     => "csv",
    ))
    resp = HTTP.post(
        "$(source.base_url)/explore/export",
        [
            "Content-Type"     => "application/json",
            "Accept"           => "application/json",
            "X-CSRF-TOKEN"     => csrf,
            "X-Requested-With" => "XMLHttpRequest",
            "Cookie"           => cookie_str,
            "User-Agent"       => "Mozilla/5.0 (compatible; Julia/HTTP.jl)",
        ],
        body,
    )
    json = JSON3.read(String(copy(resp.body)))
    haskey(json, :uuid) || error("ADB export did not return a UUID: $(String(copy(resp.body)))")
    return String(json.uuid)
end

"""Poll export status; returns "ready", "pending", or "failed"."""
function _adb_poll_status(source::AdbSource, uuid::String)
    resp = HTTP.get(
        "$(source.base_url)/explore/status/$(uuid)",
        ["Accept" => "application/json",
         "User-Agent" => "Mozilla/5.0 (compatible; Julia/HTTP.jl)"],
    )
    json = JSON3.read(String(copy(resp.body)))
    return String(json.status)
end

"""Download the completed export as CSV text."""
function _adb_download(source::AdbSource, uuid::String)
    resp = HTTP.get(
        "$(source.base_url)/explore/download/$(uuid)",
        ["User-Agent" => "Mozilla/5.0 (compatible; Julia/HTTP.jl)"],
    )
    return String(copy(resp.body))
end

"""
Parse ADB CSV export into a DataFrame with [:date, :value] columns.

Expected CSV columns (ADB export format):
  Economy, Indicator, Unit, Year, Value
"""
function _parse_adb_csv(csv_text::String)
    lines  = filter(!isempty, strip.(split(csv_text, '\n')))
    length(lines) < 2 && return DataFrame(date=Date[], value=Float64[])

    # Find header line
    header = lowercase.(strip.(split(lines[1], ',')))
    year_col  = findfirst(==("year"),  header)
    value_col = findfirst(==("value"), header)
    (year_col === nothing || value_col === nothing) &&
        error("ADB CSV missing Year/Value columns. Header: $(lines[1])")

    dates  = Date[]
    values = Float64[]
    for line in lines[2:end]
        parts = strip.(split(line, ','))
        length(parts) < max(year_col, value_col) && continue
        yr  = tryparse(Int, parts[year_col])
        val = tryparse(Float64, parts[value_col])
        (yr === nothing || val === nothing) && continue
        push!(dates,  Date(yr, 1, 1))
        push!(values, val)
    end

    result = DataFrame(date=dates, value=values)
    sort!(result, :date)
    return result
end

"""
Full export workflow: get CSRF → submit → poll → download → parse.
Polls up to `max_polls` times with `poll_interval` seconds between attempts.
"""
function _adb_export_data(source::AdbSource, economy_code::String, indicator_id::Int,
                           years::Vector{Int}; max_polls::Int=20, poll_interval::Real=2)
    csrf, cookie_str = _adb_get_session(source)
    isempty(csrf) && error(
        "Could not extract CSRF token from ADB page. The site layout may have changed.")

    uuid = _adb_submit_export(source, economy_code, indicator_id, years, csrf, cookie_str)

    for _ in 1:max_polls
        sleep(poll_interval)
        status = _adb_poll_status(source, uuid)
        if status == "ready"
            csv_text = _adb_download(source, uuid)
            return _parse_adb_csv(csv_text)
        elseif status == "failed"
            error(
                "ADB server-side export job failed (uuid=$(uuid)). " *
                "This is a known limitation of the KIDB web API — the async export " *
                "infrastructure is designed for browser use and may reject programmatic " *
                "requests. Try again later or use search_indicators()/get_metadata() " *
                "for discovery without data retrieval.")
        end
        # status == "pending" → continue polling
    end
    error("ADB export job timed out after $(max_polls * poll_interval)s (uuid=$(uuid))")
end

"""Build a default year list covering start_date to end_date (annual data)."""
function _adb_year_range(start_date::Union{Date,Nothing}, end_date::Union{Date,Nothing})
    y1 = start_date !== nothing ? year(start_date) : 1960
    y2 = end_date   !== nothing ? year(end_date)   : year(today())
    return collect(y1:y2)
end

function fetch_data(source::AdbSource, identifier::String; kwargs...)
    try
        eco, ind_id = _parse_adb_identifier(identifier)
        years = _adb_year_range(nothing, nothing)
        return _adb_export_data(source, eco, ind_id, years)
    catch e
        throw(DataSourceError("AdbSource", "Failed to fetch '$identifier': $(e)"))
    end
end

function fetch_time_series(source::AdbSource, identifier::String;
                           start_date::Union{Date,Nothing}=nothing,
                           end_date::Union{Date,Nothing}=nothing, kwargs...)
    try
        eco, ind_id = _parse_adb_identifier(identifier)
        years = _adb_year_range(start_date, end_date)
        df    = _adb_export_data(source, eco, ind_id, years)
        start_date !== nothing && (df = df[df.date .>= start_date, :])
        end_date   !== nothing && (df = df[df.date .<= end_date,   :])
        return df
    catch e
        throw(DataSourceError("AdbSource",
            "Failed to fetch '$identifier' ($(start_date) to $(end_date)): $(e)"))
    end
end

function validate_connection(source::AdbSource)
    try
        resp = HTTP.get("$(source.base_url)/search?q=GDP",
                        ["Accept" => "application/json"])
        json = JSON3.read(String(copy(resp.body)))
        return json isa JSON3.Array && length(json) > 0
    catch
        return false
    end
end

function supports_asset_type(::AdbSource, asset_type::Symbol)
    return asset_type in [:gdp, :cpi, :economic_indicator, :monetary, :unemployment]
end

"""
    search_indicators(source::AdbSource, query::String) -> Vector{Dict}

Search for ADB indicators by name keyword. Returns a list of matching indicators
with their numeric `code` (use as indicator ID) and `name`.

```julia
adb = AdbSource()
hits = search_indicators(adb, "GDP")
# => [{name="GDP at current prices", code=1200004, type="indicator"}, ...]
gdp_id = hits[1]["code"]   # => 1200004
fetch_data(adb, "PHI/\$gdp_id")
```
"""
function search_indicators(source::AdbSource, query::String)
    try
        resp = HTTP.get("$(source.base_url)/search",
                        ["Accept" => "application/json"];
                        query=["q" => query])
        json = JSON3.read(String(copy(resp.body)))
        return [Dict{String,Any}("name" => string(r.name),
                                  "code" => r.code,
                                  "type" => string(r.type)) for r in json]
    catch e
        throw(DataSourceError("AdbSource", "Search failed for '$query': $(e)"))
    end
end

function get_metadata(source::AdbSource, identifier::String)
    try
        eco, ind_id = _parse_adb_identifier(identifier)
        resp = HTTP.get(
            "$(source.base_url)/metadata",
            ["Accept" => "application/json"];
            query=["economy_code" => eco, "indicator_id" => string(ind_id)],
        )
        json = JSON3.read(String(copy(resp.body)))
        meta = Dict{String,Any}(
            "id"           => identifier,
            "economy_code" => eco,
            "indicator_id" => ind_id,
            "source"       => "ADB Key Indicators Database",
            "url"          => "https://kidb.adb.org",
        )
        if json isa JSON3.Object
            for (k, v) in pairs(json)
                meta[string(k)] = v isa JSON3.Object ? Dict(pairs(v)) : v
            end
        end
        return meta
    catch
        return Dict{String,Any}(
            "id" => identifier, "source" => "ADB Key Indicators Database",
            "url" => "https://kidb.adb.org",
        )
    end
end

function list_available_series(source::AdbSource; kwargs...)
    @warn "AdbSource: use search_indicators(adb, \"keyword\") to find indicator IDs. " *
          "Economy codes: PHI IND PRC INO JPN KOR BAN PAK VIE THA MAL SIN SRI NEP ..."
    return [
        "PHI/1200004",    # Philippines GDP at current prices (annual)
        "IND/1200004",    # India GDP at current prices (annual)
        "PRC/1200004",    # China GDP at current prices (annual)
        "INO/1200004",    # Indonesia GDP at current prices (annual)
        "JPN/1200004",    # Japan GDP at current prices (annual)
        "KOR/1200004",    # Korea GDP at current prices (annual)
        "PHI/1200021",    # Philippines GDP growth (annual)
        "PHI/1200140",    # Philippines CPI inflation (annual)
        "PHI/1200100",    # Philippines population (annual)
        "SIN/1200004",    # Singapore GDP at current prices (annual)
    ]
end

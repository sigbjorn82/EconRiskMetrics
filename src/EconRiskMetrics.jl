module EconRiskMetrics

# Include submodules
include("utils/EnvLoader.jl")
include("data/DataSources.jl")

# Import and re-export from submodules
using .EnvLoader
using .DataSources

# Export from EnvLoader
export load_env

# Export from DataSources
export DataSources, FredSource
export fetch_data, fetch_time_series, get_metadata
export validate_connection, supports_asset_type, list_available_series
export DataSourceError

end # module

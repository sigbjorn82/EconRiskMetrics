module EconRiskMetrics

# Include submodules
include("utils/EnvLoader.jl")
include("data/DataSources.jl")
include("models/Options.jl")

# Import and re-export from submodules
using .EnvLoader
using .DataSources
using .Options

# Export from EnvLoader
export load_env

# Export from DataSources
export DataSources, FredSource, WorldBankSource, IMFSource, AlphaVantageSource, BankOfEnglandSource
export YFinanceSource, BlsSource, ECBSource, EurostatSource, OECDSource, BojSource
export EcosSource, MasSource, AdbSource
export StatCanSource, OnsSource, SnbSource, NorgesSource, EiaSource, RiksbanksSource
export SsbSource
export fetch_data, fetch_time_series, get_metadata
export validate_connection, supports_asset_type, list_available_series
export DataSourceError, search_indicators

# Export from Options
export Options
export OptionStyle, European, American
export OptionType, Call, Put
export OptionContract, OptionResult, OptionPricingError
export black_scholes
export bs_delta, bs_gamma, bs_vega, bs_theta, bs_rho, bs_phi
export bs_vanna, bs_volga, bs_charm, bs_speed
export bs_greeks, implied_vol
export baw_american, bjerksund_stensland_1993, bjerksund_stensland
export barrier_option
export geometric_asian, arithmetic_asian_approx
export cash_or_nothing, asset_or_nothing, gap_option
export lookback_fixed, lookback_floating
export chooser_option, complex_chooser, compound_option
export exchange_option, forward_start, supershare
export crr_tree, lr_tree, trinomial_tree
export mc_european, mc_asian, mc_barrier, mc_heston
export heston_price, heston_greeks
export price

end # module

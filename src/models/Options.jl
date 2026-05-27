"""
    Options

Comprehensive option pricing module implementing the full
Espen Gaarder Haug "Complete Guide to Option Pricing Formulas" framework.

# Unified cost-of-carry parameter `b`
  b = r      → Black-Scholes 1973 (no dividends)
  b = r - q  → Merton 1973 (continuous dividend yield q)
  b = 0      → Black 1976 (futures/forwards)
  b = r - rF → Garman-Kohlhagen 1983 (FX, rF = foreign risk-free rate)

# Quick start
```julia
using EconRiskMetrics

# European call via Black-Scholes (b = r, no dividends)
price = black_scholes(100.0, 100.0, 1.0, 0.05, 0.05, 0.20, Call())

# All Greeks in one pass
g = bs_greeks(100.0, 100.0, 1.0, 0.05, 0.05, 0.20, Call())

# Implied volatility
iv = implied_vol(price, 100.0, 100.0, 1.0, 0.05, 0.05, Call())

# American option (Bjerksund-Stensland 2002)
bjerksund_stensland(100.0, 100.0, 1.0, 0.05, 0.0, 0.20, Put())

# Barrier option (down-and-in call)
barrier_option(100.0, 90.0, 95.0, 0.0, 0.5, 0.10, 0.05, 0.25, 1, 1)

# Heston stochastic volatility
heston_price(100.0, 100.0, 1.0, 0.05, 0.02, 0.04, 2.0, 0.04, 0.5, -0.7, Call())
```
"""
module Options

using Statistics      # mean, std in MonteCarlo
using Random          # MersenneTwister in MonteCarlo
using LinearAlgebra   # eigen/SymTridiagonal for Heston GL quadrature
using Dates           # available if needed for expiry calculations

# ── Core infrastructure ────────────────────────────────────────────────────────
include("core/Types.jl")
include("core/Utils.jl")

# ── Analytic models ────────────────────────────────────────────────────────────
include("analytic/BlackScholes.jl")
include("analytic/American.jl")
include("analytic/Exotic.jl")

# ── Numerical models ───────────────────────────────────────────────────────────
include("numerical/Binomial.jl")
include("numerical/MonteCarlo.jl")

# ── Stochastic volatility ──────────────────────────────────────────────────────
include("stochastic/Heston.jl")

# ── Exports: Types ─────────────────────────────────────────────────────────────
export OptionStyle, European, American
export OptionType, Call, Put
export OptionContract, OptionResult, OptionPricingError

# ── Exports: Black-Scholes ─────────────────────────────────────────────────────
export black_scholes
export bs_delta, bs_gamma, bs_vega, bs_theta, bs_rho, bs_phi
export bs_vanna, bs_volga, bs_charm, bs_speed
export bs_greeks
export implied_vol

# ── Exports: American approximations ──────────────────────────────────────────
export baw_american
export bjerksund_stensland_1993, bjerksund_stensland

# ── Exports: Exotic options ────────────────────────────────────────────────────
export barrier_option
export geometric_asian, arithmetic_asian_approx
export cash_or_nothing, asset_or_nothing, gap_option
export lookback_fixed, lookback_floating
export chooser_option, complex_chooser
export compound_option
export exchange_option
export forward_start
export supershare

# ── Exports: Numerical trees ───────────────────────────────────────────────────
export crr_tree, lr_tree, trinomial_tree

# ── Exports: Monte Carlo ───────────────────────────────────────────────────────
export mc_european, mc_asian, mc_barrier, mc_heston

# ── Exports: Heston ────────────────────────────────────────────────────────────
export heston_price, heston_greeks

# ── Convenience dispatch: price(OptionContract, model) ────────────────────────

"""
    price(c::OptionContract, model::Symbol=:bs; kwargs...) -> Float64

Convenience wrapper — price an `OptionContract` with any supported model.

`model` options:
- `:bs`       — Black-Scholes (default)
- `:baw`      — Barone-Adesi Whaley (American)
- `:bs1993`   — Bjerksund-Stensland 1993 (American)
- `:bs2002`   — Bjerksund-Stensland 2002 (American, recommended)
- `:crr`      — Cox-Ross-Rubinstein tree; pass `n=N` for step count
- `:lr`       — Leisen-Reimer tree
- `:trinomial` — Trinomial tree
- `:mc`       — Monte Carlo GBM European

# Example
```julia
c = OptionContract(100.0, 100.0, 1.0, 0.05, 0.05, 0.20, American(), Put())
price(c, :bs2002)
price(c, :crr; n=200)
```
"""
function price(c::OptionContract, model::Symbol=:bs; kwargs...) :: Float64
    if model == :bs
        black_scholes(c.S, c.X, c.T, c.r, c.b, c.v, c.type)
    elseif model == :baw
        baw_american(c.S, c.X, c.T, c.r, c.b, c.v, c.type)
    elseif model == :bs1993
        bjerksund_stensland_1993(c.S, c.X, c.T, c.r, c.b, c.v, c.type)
    elseif model == :bs2002
        bjerksund_stensland(c.S, c.X, c.T, c.r, c.b, c.v, c.type)
    elseif model == :crr
        n = get(kwargs, :n, 100)
        crr_tree(c.S, c.X, c.T, c.r, c.b, c.v, c.type, c.style; n=n)
    elseif model == :lr
        n = get(kwargs, :n, 101)
        lr_tree(c.S, c.X, c.T, c.r, c.b, c.v, c.type, c.style; n=n)
    elseif model == :trinomial
        n = get(kwargs, :n, 100)
        trinomial_tree(c.S, c.X, c.T, c.r, c.b, c.v, c.type, c.style; n=n)
    elseif model == :mc
        n_paths = get(kwargs, :n_paths, 100_000)
        mc_european(c.S, c.X, c.T, c.r, c.b, c.v, c.type; n_paths=n_paths).price
    else
        throw(OptionPricingError("price", "Unknown model :$model. " *
              "Use :bs, :baw, :bs1993, :bs2002, :crr, :lr, :trinomial, or :mc"))
    end
end

export price

end # module Options

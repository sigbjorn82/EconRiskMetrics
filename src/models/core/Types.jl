# Types.jl — Core option types for the Options pricing module
# Following Haug "The Complete Guide to Option Pricing Formulas"

# ── Option Style ───────────────────────────────────────────────────────────────

"""
    OptionStyle

Abstract type for option exercise style. Concrete subtypes: `European`, `American`.
"""
abstract type OptionStyle end

"""
    European <: OptionStyle

European exercise style: can only be exercised at expiry.
"""
struct European <: OptionStyle end

"""
    American <: OptionStyle

American exercise style: can be exercised at any time up to and including expiry.
"""
struct American <: OptionStyle end

# ── Option Type ────────────────────────────────────────────────────────────────

"""
    OptionType

Abstract type for option payoff direction. Concrete subtypes: `Call`, `Put`.
"""
abstract type OptionType end

"""
    Call <: OptionType

Call option: right to buy the underlying at the strike price.
Payoff at expiry: max(S_T - X, 0)
"""
struct Call <: OptionType end

"""
    Put <: OptionType

Put option: right to sell the underlying at the strike price.
Payoff at expiry: max(X - S_T, 0)
"""
struct Put <: OptionType end

# ── Option Contract ────────────────────────────────────────────────────────────

"""
    OptionContract

Represents a vanilla option in Haug's unified cost-of-carry framework.

# Fields
- `S::Float64`           — current underlying asset price
- `X::Float64`           — strike price
- `T::Float64`           — time to expiry in years
- `r::Float64`           — continuously compounded risk-free rate (annualised)
- `b::Float64`           — cost-of-carry rate (annualised):
                             b = r      → Black-Scholes 1973 (no dividends)
                             b = r - q  → Merton 1973 (continuous dividend yield q)
                             b = 0      → Black 1976 (futures/forwards)
                             b = r - rF → Garman-Kohlhagen 1983 (FX, rF = foreign rate)
- `v::Float64`           — annualised volatility (σ)
- `style::OptionStyle`   — `European()` or `American()`
- `type::OptionType`     — `Call()` or `Put()`

# Example
```julia
# Black-Scholes European call (b = r, no dividends)
c = OptionContract(100.0, 100.0, 1.0, 0.05, 0.05, 0.20, European(), Call())

# Merton model (continuous dividend q = 0.02)
c = OptionContract(100.0, 100.0, 1.0, 0.05, 0.03, 0.20, European(), Call())

# Black-76 futures option (b = 0)
c = OptionContract(19.0, 19.0, 0.75, 0.10, 0.0, 0.28, European(), Call())
```
"""
struct OptionContract
    S::Float64
    X::Float64
    T::Float64
    r::Float64
    b::Float64
    v::Float64
    style::OptionStyle
    type::OptionType

    function OptionContract(S::Float64, X::Float64, T::Float64,
                            r::Float64, b::Float64, v::Float64,
                            style::OptionStyle, type::OptionType)
        S > 0.0  || throw(ArgumentError("Underlying price S must be positive, got $S"))
        X > 0.0  || throw(ArgumentError("Strike X must be positive, got $X"))
        T > 0.0  || throw(ArgumentError("Time to expiry T must be positive, got $T"))
        v > 0.0  || throw(ArgumentError("Volatility v must be positive, got $v"))
        new(S, X, T, r, b, v, style, type)
    end
end

# Convenience constructor with keyword arguments
function OptionContract(; S::Float64, X::Float64, T::Float64,
                         r::Float64, b::Float64, v::Float64,
                         style::OptionStyle=European(), type::OptionType=Call())
    OptionContract(S, X, T, r, b, v, style, type)
end

# ── Option Result ──────────────────────────────────────────────────────────────

"""
    OptionResult

Holds the price and Greeks for a priced option. Fields not computed by the
model are set to `NaN`.

# Fields
- `price::Float64`  — option fair value
- `delta::Float64`  — ∂V/∂S (sensitivity to underlying price)
- `gamma::Float64`  — ∂²V/∂S² (rate of change of delta)
- `vega::Float64`   — ∂V/∂v (sensitivity to volatility); units: per 1.0 vol point
- `theta::Float64`  — ∂V/∂T (time value; positive = value increases with more time)
                       Divide by 365 to get per-calendar-day decay.
- `rho::Float64`    — ∂V/∂r (sensitivity to risk-free rate)
- `vanna::Float64`  — ∂²V/∂S∂v = ∂Δ/∂v
- `volga::Float64`  — ∂²V/∂v² (vomma; convexity of price w.r.t. volatility)
- `charm::Float64`  — ∂Δ/∂T (delta bleed; rate of change of delta w.r.t. time)
- `speed::Float64`  — ∂Γ/∂S (rate of change of gamma w.r.t. underlying price)
"""
struct OptionResult
    price::Float64
    delta::Float64
    gamma::Float64
    vega::Float64
    theta::Float64
    rho::Float64
    vanna::Float64
    volga::Float64
    charm::Float64
    speed::Float64
end

function OptionResult(price::Float64;
                      delta::Float64=NaN, gamma::Float64=NaN,
                      vega::Float64=NaN,  theta::Float64=NaN,
                      rho::Float64=NaN,   vanna::Float64=NaN,
                      volga::Float64=NaN, charm::Float64=NaN,
                      speed::Float64=NaN)
    OptionResult(price, delta, gamma, vega, theta, rho, vanna, volga, charm, speed)
end

function Base.show(io::IO, r::OptionResult)
    println(io, "OptionResult:")
    println(io, "  price  = $(round(r.price,  digits=6))")
    isnan(r.delta) || println(io, "  delta  = $(round(r.delta,  digits=6))")
    isnan(r.gamma) || println(io, "  gamma  = $(round(r.gamma,  digits=6))")
    isnan(r.vega)  || println(io, "  vega   = $(round(r.vega,   digits=6))")
    isnan(r.theta) || println(io, "  theta  = $(round(r.theta,  digits=6))")
    isnan(r.rho)   || println(io, "  rho    = $(round(r.rho,    digits=6))")
    isnan(r.vanna) || println(io, "  vanna  = $(round(r.vanna,  digits=6))")
    isnan(r.volga) || println(io, "  volga  = $(round(r.volga,  digits=6))")
    isnan(r.charm) || println(io, "  charm  = $(round(r.charm,  digits=6))")
    isnan(r.speed) || println(io, "  speed  = $(round(r.speed,  digits=6))")
end

# ── Exception Type ─────────────────────────────────────────────────────────────

"""
    OptionPricingError <: Exception

Exception raised when an option pricing computation fails.

# Fields
- `model::String`    — name of the model that raised the error
- `message::String`  — description of the error
"""
struct OptionPricingError <: Exception
    model::String
    message::String
end

Base.showerror(io::IO, e::OptionPricingError) =
    print(io, "OptionPricingError [$(e.model)]: $(e.message)")

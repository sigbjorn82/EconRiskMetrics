# BlackScholes.jl — Generalised Black-Scholes-Merton pricing and Greeks
# Implements the unified cost-of-carry framework from Haug Chapter 1.
#
# Cost-of-carry b parameter:
#   b = r      → Black-Scholes 1973 (no dividends)
#   b = r - q  → Merton 1973 (continuous dividend yield q)
#   b = 0      → Black 1976 (futures/forwards)
#   b = r - rF → Garman-Kohlhagen 1983 (FX, rF = foreign risk-free rate)

# ── Core Price Formula ─────────────────────────────────────────────────────────

"""
    black_scholes(S, X, T, r, b, v, ::Call) -> Float64
    black_scholes(S, X, T, r, b, v, ::Put)  -> Float64

Haug unified generalised Black-Scholes-Merton formula (Chapter 1, eq. 1.1).

    c = S·exp((b-r)T)·N(d₁) - X·exp(-rT)·N(d₂)
    p = X·exp(-rT)·N(-d₂) - S·exp((b-r)T)·N(-d₁)

# Arguments
- `S::Float64` — current underlying price
- `X::Float64` — strike price
- `T::Float64` — time to expiry in years
- `r::Float64` — continuously compounded risk-free rate
- `b::Float64` — cost of carry (see module header)
- `v::Float64` — annualised volatility (σ)

# Example
```julia
# European call, Black-Scholes (b = r)
black_scholes(60.0, 65.0, 0.25, 0.08, 0.08, 0.30, Call())   # → 2.1334

# Futures option, Black-76 (b = 0)
black_scholes(19.0, 19.0, 0.75, 0.10, 0.0,  0.28, Call())   # → 1.7011
```
"""
function black_scholes(S::Float64, X::Float64, T::Float64,
                       r::Float64, b::Float64, v::Float64,
                       ::Call) :: Float64
    d1  = _d1(S, X, T, b, v)
    d2  = d1 - v * sqrt(T)
    S * exp((b - r) * T) * Nd(d1) - X * exp(-r * T) * Nd(d2)
end

function black_scholes(S::Float64, X::Float64, T::Float64,
                       r::Float64, b::Float64, v::Float64,
                       ::Put) :: Float64
    d1  = _d1(S, X, T, b, v)
    d2  = d1 - v * sqrt(T)
    X * exp(-r * T) * Nd(-d2) - S * exp((b - r) * T) * Nd(-d1)
end

# Dispatch on OptionContract
black_scholes(c::OptionContract) =
    black_scholes(c.S, c.X, c.T, c.r, c.b, c.v, c.type)

# ── Greeks ─────────────────────────────────────────────────────────────────────

"""
    bs_delta(S, X, T, r, b, v, ::Call) -> Float64
    bs_delta(S, X, T, r, b, v, ::Put)  -> Float64

Option delta ∂V/∂S. (Haug p.25)

    Δ_call = exp((b-r)T) · N(d₁)
    Δ_put  = exp((b-r)T) · (N(d₁) - 1)
"""
function bs_delta(S::Float64, X::Float64, T::Float64,
                  r::Float64, b::Float64, v::Float64, ::Call) :: Float64
    exp((b - r) * T) * Nd(_d1(S, X, T, b, v))
end

function bs_delta(S::Float64, X::Float64, T::Float64,
                  r::Float64, b::Float64, v::Float64, ::Put) :: Float64
    exp((b - r) * T) * (Nd(_d1(S, X, T, b, v)) - 1.0)
end

"""
    bs_gamma(S, X, T, r, b, v) -> Float64

Option gamma ∂²V/∂S² — identical for calls and puts. (Haug p.26)

    Γ = exp((b-r)T) · N'(d₁) / (S·v·√T)
"""
function bs_gamma(S::Float64, X::Float64, T::Float64,
                  r::Float64, b::Float64, v::Float64) :: Float64
    d1 = _d1(S, X, T, b, v)
    exp((b - r) * T) * nd(d1) / (S * v * sqrt(T))
end

"""
    bs_vega(S, X, T, r, b, v) -> Float64

Option vega ∂V/∂v — identical for calls and puts. (Haug p.27)
Units: value change per 1.0 (100%) move in volatility.

    vega = S · exp((b-r)T) · N'(d₁) · √T
"""
function bs_vega(S::Float64, X::Float64, T::Float64,
                 r::Float64, b::Float64, v::Float64) :: Float64
    d1 = _d1(S, X, T, b, v)
    S * exp((b - r) * T) * nd(d1) * sqrt(T)
end

"""
    bs_theta(S, X, T, r, b, v, ::Call) -> Float64
    bs_theta(S, X, T, r, b, v, ::Put)  -> Float64

Option theta ∂V/∂T (per year; positive = more time means more value). (Haug p.27-28)
Divide by 365 for per-calendar-day decay (theta is then negative for long options).

    θ_call = -S·exp((b-r)T)·N'(d₁)·v/(2√T) - (b-r)·S·exp((b-r)T)·N(d₁) - r·X·exp(-rT)·N(d₂)
    θ_put  = -S·exp((b-r)T)·N'(d₁)·v/(2√T) + (b-r)·S·exp((b-r)T)·N(-d₁) + r·X·exp(-rT)·N(-d₂)
"""
function bs_theta(S::Float64, X::Float64, T::Float64,
                  r::Float64, b::Float64, v::Float64, ::Call) :: Float64
    sqrtT  = sqrt(T)
    d1     = _d1(S, X, T, b, v)
    d2     = d1 - v * sqrtT
    ebrT   = exp((b - r) * T)
    erT    = exp(-r * T)
    -S * ebrT * nd(d1) * v / (2.0 * sqrtT) -
     (b - r) * S * ebrT * Nd(d1) -
     r * X * erT * Nd(d2)
end

function bs_theta(S::Float64, X::Float64, T::Float64,
                  r::Float64, b::Float64, v::Float64, ::Put) :: Float64
    sqrtT  = sqrt(T)
    d1     = _d1(S, X, T, b, v)
    d2     = d1 - v * sqrtT
    ebrT   = exp((b - r) * T)
    erT    = exp(-r * T)
    -S * ebrT * nd(d1) * v / (2.0 * sqrtT) +
     (b - r) * S * ebrT * Nd(-d1) +
     r * X * erT * Nd(-d2)
end

"""
    bs_rho(S, X, T, r, b, v, ::Call) -> Float64
    bs_rho(S, X, T, r, b, v, ::Put)  -> Float64

Option rho ∂V/∂r (sensitivity to the risk-free rate). (Haug p.29)

    ρ_call = T · X · exp(-rT) · N(d₂)
    ρ_put  = -T · X · exp(-rT) · N(-d₂)

Note: for futures options (b = 0) this is the rho w.r.t. the discount rate only.
See `bs_phi` for the cost-of-carry rho ∂V/∂b.
"""
function bs_rho(S::Float64, X::Float64, T::Float64,
                r::Float64, b::Float64, v::Float64, ::Call) :: Float64
    d2 = _d2(S, X, T, b, v)
    T * X * exp(-r * T) * Nd(d2)
end

function bs_rho(S::Float64, X::Float64, T::Float64,
                r::Float64, b::Float64, v::Float64, ::Put) :: Float64
    d2 = _d2(S, X, T, b, v)
    -T * X * exp(-r * T) * Nd(-d2)
end

"""
    bs_phi(S, X, T, r, b, v, ::Call) -> Float64
    bs_phi(S, X, T, r, b, v, ::Put)  -> Float64

Cost-of-carry rho φ = ∂V/∂b. (Haug p.29)
Equals the sensitivity to the dividend yield for Merton model (φ_call = -T·S·exp((b-r)T)·N(d₁)).

    φ_call = T · S · exp((b-r)T) · N(d₁)
    φ_put  = -T · S · exp((b-r)T) · N(-d₁)
"""
function bs_phi(S::Float64, X::Float64, T::Float64,
                r::Float64, b::Float64, v::Float64, ::Call) :: Float64
    d1 = _d1(S, X, T, b, v)
    T * S * exp((b - r) * T) * Nd(d1)
end

function bs_phi(S::Float64, X::Float64, T::Float64,
                r::Float64, b::Float64, v::Float64, ::Put) :: Float64
    d1 = _d1(S, X, T, b, v)
    -T * S * exp((b - r) * T) * Nd(-d1)
end

# ── Higher-Order Greeks ────────────────────────────────────────────────────────

"""
    bs_vanna(S, X, T, r, b, v) -> Float64

Vanna = ∂²V/∂S∂v = ∂Δ/∂v — identical for calls and puts. (Haug p.30)

    vanna = -exp((b-r)T) · N'(d₁) · d₂/v
"""
function bs_vanna(S::Float64, X::Float64, T::Float64,
                  r::Float64, b::Float64, v::Float64) :: Float64
    sqrtT = sqrt(T)
    d1    = _d1(S, X, T, b, v)
    d2    = d1 - v * sqrtT
    -exp((b - r) * T) * nd(d1) * d2 / v
end

"""
    bs_volga(S, X, T, r, b, v) -> Float64

Volga (Vomma) = ∂²V/∂v² — identical for calls and puts. (Haug p.30)

    volga = vega · d₁·d₂/v
"""
function bs_volga(S::Float64, X::Float64, T::Float64,
                  r::Float64, b::Float64, v::Float64) :: Float64
    sqrtT  = sqrt(T)
    d1     = _d1(S, X, T, b, v)
    d2     = d1 - v * sqrtT
    vega_v = S * exp((b - r) * T) * nd(d1) * sqrtT
    vega_v * d1 * d2 / v
end

"""
    bs_charm(S, X, T, r, b, v, ::Call) -> Float64
    bs_charm(S, X, T, r, b, v, ::Put)  -> Float64

Charm = ∂Δ/∂T (delta bleed — rate of change of delta with respect to time). (Haug p.31)

    charm_call = exp((b-r)T) · [N'(d₁)·(2(b-r)T - d₂·v·√T)/(2T·v·√T) + (b-r)·N(d₁)]
    charm_put  = charm_call + (b-r)·exp((b-r)T)
"""
function bs_charm(S::Float64, X::Float64, T::Float64,
                  r::Float64, b::Float64, v::Float64, ::Call) :: Float64
    sqrtT = sqrt(T)
    d1    = _d1(S, X, T, b, v)
    d2    = d1 - v * sqrtT
    ebrT  = exp((b - r) * T)
    ebrT * (nd(d1) * (2.0 * (b - r) * T - d2 * v * sqrtT) / (2.0 * T * v * sqrtT) +
            (b - r) * Nd(d1))
end

function bs_charm(S::Float64, X::Float64, T::Float64,
                  r::Float64, b::Float64, v::Float64, ::Put) :: Float64
    sqrtT = sqrt(T)
    d1    = _d1(S, X, T, b, v)
    d2    = d1 - v * sqrtT
    ebrT  = exp((b - r) * T)
    ebrT * (nd(d1) * (2.0 * (b - r) * T - d2 * v * sqrtT) / (2.0 * T * v * sqrtT) -
            (b - r) * Nd(-d1))
end

"""
    bs_speed(S, X, T, r, b, v) -> Float64

Speed = ∂Γ/∂S — identical for calls and puts. (Haug p.31)

    speed = -Γ/S · (1 + d₁/(v·√T))
"""
function bs_speed(S::Float64, X::Float64, T::Float64,
                  r::Float64, b::Float64, v::Float64) :: Float64
    sqrtT = sqrt(T)
    d1    = _d1(S, X, T, b, v)
    γ     = bs_gamma(S, X, T, r, b, v)
    -γ / S * (1.0 + d1 / (v * sqrtT))
end

# ── Aggregate Greeks ───────────────────────────────────────────────────────────

"""
    bs_greeks(S, X, T, r, b, v, ot::OptionType) -> NamedTuple
    bs_greeks(c::OptionContract) -> NamedTuple

Compute all Black-Scholes Greeks in a single pass, reusing intermediate
values (d₁, d₂, N(d₁), N(d₂), N'(d₁), exp((b-r)T), exp(-rT)).

Returns a NamedTuple with fields:
`price, delta, gamma, vega, theta, rho, phi, vanna, volga, charm, speed`

# Example
```julia
g = bs_greeks(100.0, 100.0, 1.0, 0.05, 0.05, 0.20, Call())
g.delta  # → 0.6368...
g.vega   # → 37.52...
```
"""
function bs_greeks(S::Float64, X::Float64, T::Float64,
                   r::Float64, b::Float64, v::Float64,
                   ot::OptionType) :: NamedTuple
    sqrtT = sqrt(T)
    d1    = _d1(S, X, T, b, v)
    d2    = d1 - v * sqrtT
    ebrT  = exp((b - r) * T)
    erT   = exp(-r * T)
    nd1   = nd(d1)
    Nd1   = Nd(d1)
    Nd2   = Nd(d2)

    is_call = ot isa Call
    φ       = is_call ? 1.0 : -1.0      # payoff direction sign

    price  = is_call ? S * ebrT * Nd1 - X * erT * Nd2 :
                       X * erT * (1.0 - Nd2) - S * ebrT * (1.0 - Nd1)
    delta  = ebrT * (is_call ? Nd1 : Nd1 - 1.0)
    gamma  = ebrT * nd1 / (S * v * sqrtT)
    vega_v = S * ebrT * nd1 * sqrtT

    theta  = is_call ?
        (-S * ebrT * nd1 * v / (2.0 * sqrtT) -
          (b - r) * S * ebrT * Nd1 -
          r * X * erT * Nd2) :
        (-S * ebrT * nd1 * v / (2.0 * sqrtT) +
          (b - r) * S * ebrT * (1.0 - Nd1) +
          r * X * erT * (1.0 - Nd2))

    rho_v  = φ * T * X * erT * (is_call ? Nd2 : 1.0 - Nd2)
    phi_v  = φ * T * S * ebrT * (is_call ? Nd1 : 1.0 - Nd1)
    vanna  = -ebrT * nd1 * d2 / v
    volga  = vega_v * d1 * d2 / v

    charm  = is_call ?
        ebrT * (nd1 * (2.0 * (b - r) * T - d2 * v * sqrtT) /
                (2.0 * T * v * sqrtT) + (b - r) * Nd1) :
        ebrT * (nd1 * (2.0 * (b - r) * T - d2 * v * sqrtT) /
                (2.0 * T * v * sqrtT) - (b - r) * (1.0 - Nd1))

    speed  = -gamma / S * (1.0 + d1 / (v * sqrtT))

    (price=price, delta=delta, gamma=gamma, vega=vega_v, theta=theta,
     rho=rho_v, phi=phi_v, vanna=vanna, volga=volga, charm=charm, speed=speed)
end

bs_greeks(c::OptionContract) = bs_greeks(c.S, c.X, c.T, c.r, c.b, c.v, c.type)

# ── Implied Volatility ─────────────────────────────────────────────────────────

"""
    implied_vol(market_price, S, X, T, r, b, ot::OptionType;
                tol=1e-8, max_iter=200) -> Float64

Newton-Raphson solver for implied volatility (IV).

Initial guess uses the Corrado-Miller (1996) approximation for a fast start.
Falls back to bisection when vega is near zero (deep ITM/OTM).

# Arguments
- `market_price::Float64` — observed option price to invert
- `S, X, T, r, b`        — option parameters (see `black_scholes`)
- `ot::OptionType`        — `Call()` or `Put()`
- `tol::Float64`          — convergence tolerance (default 1e-8)
- `max_iter::Int`         — maximum Newton iterations (default 200)

# Returns
Implied volatility σ as a Float64.
Throws `OptionPricingError` if convergence is not achieved.

# Example
```julia
price = black_scholes(100.0, 100.0, 1.0, 0.05, 0.05, 0.25, Call())
iv    = implied_vol(price, 100.0, 100.0, 1.0, 0.05, 0.05, Call())
# iv ≈ 0.25
```
"""
function implied_vol(market_price::Float64, S::Float64, X::Float64,
                     T::Float64, r::Float64, b::Float64,
                     ot::OptionType;
                     tol::Float64=1e-8,
                     max_iter::Int=200) :: Float64
    # Validate input
    intrinsic = max(ot isa Call ? S * exp((b - r) * T) - X * exp(-r * T) : X * exp(-r * T) - S * exp((b - r) * T), 0.0)
    if market_price < intrinsic - tol
        throw(OptionPricingError("implied_vol",
              "market price $market_price below intrinsic value $intrinsic"))
    end

    # Corrado-Miller (1996) initial guess — works for a wide range of moneyness
    erT  = exp(-r * T)
    ebrT = exp((b - r) * T)
    F    = S * ebrT              # forward price

    # CM1996: σ₀ ≈ √(2π/T) · (C - (F-X)*erT/2) / (F+X)*erT  for call
    mid   = (market_price - max(F * erT - X * erT, 0.0))   # moneyness-adjusted price
    inner = mid * mid - (F * erT - X * erT)^2 / π
    σ = if inner >= 0.0
        sqrt(2.0 * π / T) * (mid + sqrt(inner)) / (F * erT + X * erT)
    else
        # Fallback: Brenner-Subrahmanyam (1988) ATM approximation
        sqrt(2.0 * π / T) * market_price / (S * erT)
    end
    σ = clamp(σ, 1e-4, 10.0)

    # Newton-Raphson
    for _ in 1:max_iter
        price_σ = black_scholes(S, X, T, r, b, σ, ot)
        diff    = price_σ - market_price
        if abs(diff) < tol
            return σ
        end
        vega_σ = bs_vega(S, X, T, r, b, σ)
        if abs(vega_σ) < 1e-12
            break   # near-zero vega → fall through to bisection
        end
        σ_new = σ - diff / vega_σ
        σ = clamp(σ_new, 1e-6, 10.0)
    end

    # Bisection fallback for stubborn cases (deep ITM/OTM, flat vega)
    lo, hi = 1e-6, 10.0
    for _ in 1:200
        mid_σ = (lo + hi) / 2.0
        if hi - lo < tol
            return mid_σ
        end
        f_mid = black_scholes(S, X, T, r, b, mid_σ, ot) - market_price
        if f_mid > 0.0
            hi = mid_σ
        else
            lo = mid_σ
        end
    end

    throw(OptionPricingError("implied_vol",
          "failed to converge for market_price=$market_price, S=$S, X=$X, T=$T"))
end

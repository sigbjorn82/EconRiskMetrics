# Exotic.jl — Closed-form exotic option pricing formulas
# Implements models from Haug Chapters 4-7.
# All formulas use the unified cost-of-carry b parameter.

# ── Barrier Options (Haug Chapter 4) ──────────────────────────────────────────

# Internal building blocks for barrier options (Rubinstein-Reiner 1991)
# μ and λ appear in all barrier formulas (Haug p.97-98)
_bar_mu(b::Float64, v::Float64)  = (b - v^2 / 2.0) / v^2
_bar_lam(r::Float64, b::Float64, v::Float64) = sqrt(_bar_mu(b, v)^2 + 2.0 * r / v^2)

function _bar_x1(S::Float64, X::Float64, T::Float64, b::Float64, v::Float64) :: Float64
    log(S / X) / (v * sqrt(T)) + (1.0 + _bar_mu(b, v)) * v * sqrt(T)
end
function _bar_x2(S::Float64, H::Float64, T::Float64, b::Float64, v::Float64) :: Float64
    log(S / H) / (v * sqrt(T)) + (1.0 + _bar_mu(b, v)) * v * sqrt(T)
end
function _bar_y1(S::Float64, X::Float64, H::Float64, T::Float64, b::Float64, v::Float64) :: Float64
    log(H^2 / (S * X)) / (v * sqrt(T)) + (1.0 + _bar_mu(b, v)) * v * sqrt(T)
end
function _bar_y2(S::Float64, H::Float64, T::Float64, b::Float64, v::Float64) :: Float64
    log(H / S) / (v * sqrt(T)) + (1.0 + _bar_mu(b, v)) * v * sqrt(T)
end
function _bar_z(S::Float64, H::Float64, T::Float64, r::Float64, b::Float64, v::Float64) :: Float64
    log(H / S) / (v * sqrt(T)) + _bar_lam(r, b, v) * v * sqrt(T)
end

# The A, B, C, D, E, F building blocks (Haug p.97-100)
function _bar_A(S::Float64, X::Float64, ::Float64, T::Float64,
                r::Float64, b::Float64, v::Float64, φ::Int) :: Float64
    x1 = _bar_x1(S, X, T, b, v)
    φ * S * exp((b - r) * T) * Nd(φ * x1) -
    φ * X * exp(-r * T)      * Nd(φ * (x1 - v * sqrt(T)))
end

function _bar_B(S::Float64, X::Float64, H::Float64, T::Float64,
                r::Float64, b::Float64, v::Float64, φ::Int) :: Float64
    x2 = _bar_x2(S, H, T, b, v)
    φ * S * exp((b - r) * T) * Nd(φ * x2) -
    φ * X * exp(-r * T)      * Nd(φ * (x2 - v * sqrt(T)))
end

function _bar_C(S::Float64, X::Float64, H::Float64, T::Float64,
                r::Float64, b::Float64, v::Float64, η::Int, φ::Int) :: Float64
    μ  = _bar_mu(b, v)
    y1 = _bar_y1(S, X, H, T, b, v)
    φ * S * exp((b - r) * T) * (H / S)^(2.0 * (μ + 1.0)) * Nd(η * y1) -
    φ * X * exp(-r * T)      * (H / S)^(2.0 * μ)          * Nd(η * (y1 - v * sqrt(T)))
end

function _bar_D(S::Float64, X::Float64, H::Float64, T::Float64,
                r::Float64, b::Float64, v::Float64, η::Int, φ::Int) :: Float64
    μ  = _bar_mu(b, v)
    y2 = _bar_y2(S, H, T, b, v)
    φ * S * exp((b - r) * T) * (H / S)^(2.0 * (μ + 1.0)) * Nd(η * y2) -
    φ * X * exp(-r * T)      * (H / S)^(2.0 * μ)          * Nd(η * (y2 - v * sqrt(T)))
end

function _bar_E(S::Float64, H::Float64, K::Float64, T::Float64,
                r::Float64, b::Float64, v::Float64, η::Int) :: Float64
    μ  = _bar_mu(b, v)
    x2 = _bar_x2(S, H, T, b, v)
    y2 = _bar_y2(S, H, T, b, v)
    K * exp(-r * T) * (Nd(η * (x2 - v * sqrt(T))) -
                       (H / S)^(2.0 * μ) * Nd(η * (y2 - v * sqrt(T))))
end

function _bar_F(S::Float64, H::Float64, K::Float64, T::Float64,
                r::Float64, b::Float64, v::Float64, η::Int) :: Float64
    μ  = _bar_mu(b, v)
    λ  = _bar_lam(r, b, v)
    z  = _bar_z(S, H, T, r, b, v)
    K * ((H / S)^(μ + λ) * Nd(η * z) +
         (H / S)^(μ - λ) * Nd(η * (z - 2.0 * λ * v * sqrt(T))))
end

"""
    barrier_option(S, X, H, K, T, r, b, v, η::Int, φ::Int) -> Float64

Standard barrier option using the Rubinstein-Reiner (1991) closed form.
(Haug Chapter 4, pp. 95-101)

All 8 barrier types are covered through the sign parameters:
- `η = +1`: down options (H < S)  — barrier is below the current price
- `η = -1`: up options (H > S)    — barrier is above the current price
- `φ = +1`: call payoff
- `φ = -1`: put payoff

Barrier type lookup:
| η  | φ  | Description          |
|----|----|--------------------- |
| +1 | +1 | down-and-in call     |
| +1 | -1 | down-and-in put      |
| -1 | +1 | up-and-in call       |
| -1 | -1 | up-and-in put        |
| +1 | +1 | down-and-out call*   |
| +1 | -1 | down-and-out put*    |
| -1 | +1 | up-and-out call*     |
| -1 | -1 | up-and-out put*      |
(*out options: use `knock_out=true`, default false)

# Arguments
- `S::Float64` — current underlying price
- `X::Float64` — strike price
- `H::Float64` — barrier level
- `K::Float64` — cash rebate paid if barrier is hit (set 0 for no rebate)
- `T::Float64` — time to expiry in years
- `r, b, v`    — risk-free rate, cost of carry, volatility
- `η::Int`     — +1 for down, -1 for up
- `φ::Int`     — +1 for call, -1 for put

# Example
```julia
# Down-and-in call: S=100, X=90, H=95, K=3, T=0.5, r=0.10, b=0.05, v=0.25
barrier_option(100.0, 90.0, 95.0, 3.0, 0.5, 0.10, 0.05, 0.25,  1, 1)  # → ≈ 7.7627
```
"""
function barrier_option(S::Float64, X::Float64, H::Float64, K::Float64,
                        T::Float64, r::Float64, b::Float64, v::Float64,
                        η::Int, φ::Int;
                        knock_out::Bool=false) :: Float64
    if φ == 1 && η == 1  # down-and-in/out call (H < S, X > H common case)
        if X > H
            val_in  = _bar_C(S, X, H, T, r, b, v, η, φ) + _bar_E(S, H, K, T, r, b, v, η)
            val_out = _bar_A(S, X, H, T, r, b, v, φ) - _bar_C(S, X, H, T, r, b, v, η, φ) +
                      _bar_F(S, H, K, T, r, b, v, η)
        else
            val_in  = _bar_A(S, X, H, T, r, b, v, φ) - _bar_B(S, X, H, T, r, b, v, φ) +
                      _bar_D(S, X, H, T, r, b, v, η, φ) + _bar_E(S, H, K, T, r, b, v, η)
            val_out = _bar_B(S, X, H, T, r, b, v, φ) - _bar_D(S, X, H, T, r, b, v, η, φ) +
                      _bar_F(S, H, K, T, r, b, v, η)
        end
    elseif φ == 1 && η == -1  # up-and-in/out call (H > S)
        if X > H
            val_in  = _bar_A(S, X, H, T, r, b, v, φ) + _bar_F(S, H, K, T, r, b, v, η)
            val_out = _bar_F(S, H, K, T, r, b, v, η)   # out = rebate only
        else
            val_in  = _bar_B(S, X, H, T, r, b, v, φ) - _bar_C(S, X, H, T, r, b, v, η, φ) +
                      _bar_D(S, X, H, T, r, b, v, η, φ) + _bar_F(S, H, K, T, r, b, v, η)
            val_out = _bar_A(S, X, H, T, r, b, v, φ) - _bar_B(S, X, H, T, r, b, v, φ) +
                      _bar_C(S, X, H, T, r, b, v, η, φ) - _bar_D(S, X, H, T, r, b, v, η, φ) +
                      _bar_F(S, H, K, T, r, b, v, η)
        end
    elseif φ == -1 && η == 1  # down-and-in/out put (H < S)
        if X > H
            val_in  = _bar_B(S, X, H, T, r, b, v, φ) - _bar_D(S, X, H, T, r, b, v, η, φ) +
                      _bar_E(S, H, K, T, r, b, v, η)
            val_out = _bar_A(S, X, H, T, r, b, v, φ) - _bar_B(S, X, H, T, r, b, v, φ) +
                      _bar_C(S, X, H, T, r, b, v, η, φ) - _bar_D(S, X, H, T, r, b, v, η, φ) +
                      _bar_F(S, H, K, T, r, b, v, η)
        else
            val_in  = _bar_A(S, X, H, T, r, b, v, φ) - _bar_C(S, X, H, T, r, b, v, η, φ) +
                      _bar_E(S, H, K, T, r, b, v, η)
            val_out = _bar_C(S, X, H, T, r, b, v, η, φ) - _bar_D(S, X, H, T, r, b, v, η, φ) +  # simplified
                      _bar_F(S, H, K, T, r, b, v, η)
        end
    else  # φ == -1 && η == -1: up-and-in/out put (H > S)
        if X > H
            val_in  = _bar_A(S, X, H, T, r, b, v, φ) - _bar_B(S, X, H, T, r, b, v, φ) +
                      _bar_D(S, X, H, T, r, b, v, η, φ) + _bar_E(S, H, K, T, r, b, v, η)
            val_out = _bar_B(S, X, H, T, r, b, v, φ) - _bar_D(S, X, H, T, r, b, v, η, φ) +
                      _bar_F(S, H, K, T, r, b, v, η)
        else
            val_in  = _bar_C(S, X, H, T, r, b, v, η, φ) + _bar_E(S, H, K, T, r, b, v, η)
            val_out = _bar_A(S, X, H, T, r, b, v, φ) - _bar_C(S, X, H, T, r, b, v, η, φ) +
                      _bar_F(S, H, K, T, r, b, v, η)
        end
    end

    knock_out ? val_out : val_in
end

# ── Asian Options (Haug Chapter 7) ────────────────────────────────────────────

"""
    geometric_asian(S, X, T, r, b, v, ::Call/Put; avg_start=0.0) -> Float64

Kemna-Vorst (1990) closed-form for geometric average price options.
(Haug Chapter 7, eq. 7.1)

For a continuous geometric average from time `avg_start` to T, the option
is equivalent to a standard BS option with adjusted parameters:
  b_A = (b - v²/6) / 2
  v_A = v / √3
  (for avg_start = 0; partial-average case adjusts T accordingly)

# Example
```julia
geometric_asian(100.0, 100.0, 1.0, 0.10, 0.05, 0.20, Call())  # → ≈ 5.4
```
"""
function geometric_asian(S::Float64, X::Float64, T::Float64,
                         r::Float64, b::Float64, v::Float64,
                         ot::OptionType; avg_start::Float64=0.0) :: Float64
    T_avg = T - avg_start    # remaining averaging period
    v_A = v * sqrt(T_avg / T) / sqrt(3.0)
    b_A = 0.5 * (b - v^2 / 6.0) * T_avg / T
    black_scholes(S, X, T, r, b_A, v_A, ot)
end

"""
    arithmetic_asian_approx(S, X, T, r, b, v, ::Call/Put;
                            n_steps=100) -> Float64

Haug-Haug-Margrabe (2000) moment-matching approximation for arithmetic
average price options. (Haug Chapter 7, pp. 181-183)

Matches the first two moments of the discrete arithmetic average to a
lognormal distribution, then prices as a standard BS option.

# Arguments
- `n_steps::Int` — number of averaging periods (default 100, approximating continuous)
"""
function arithmetic_asian_approx(S::Float64, X::Float64, T::Float64,
                                 r::Float64, b::Float64, v::Float64,
                                 ot::OptionType; n_steps::Int=100) :: Float64
    dt = T / n_steps
    # First moment: E[A] = S/n * Σ exp(b·i·dt)
    # For continuous: E[A] = S * exp(bT/2) * sinh(bT/2) / (bT/2) if b≠0
    m1 = if abs(b) < 1e-10
        S
    else
        S * (exp(b * T) - 1.0) / (b * T)
    end

    # Second moment: E[A²] uses the sum of exp((2b+v²)·i·dt)
    m2 = if abs(2.0 * b + v^2) < 1e-10
        S^2 * exp(v^2 * dt) * dt / T
    else
        2.0 * S^2 * exp((2.0 * b + v^2) * dt) *
        (exp((2.0 * b + v^2) * T) - 1.0) / ((2.0 * b + v^2) * T)^2
    end

    v_A = sqrt(log(m2 / m1^2) / T)
    b_A = log(m1 / S) / T
    black_scholes(S, X, T, r, b_A, v_A, ot)
end

# ── Binary / Digital Options (Haug Chapter 5) ─────────────────────────────────

"""
    cash_or_nothing(S, X, T, r, b, v, K_cash::Float64, ::Call/Put) -> Float64

Cash-or-nothing digital option. Pays `K_cash` if S_T > X (call) or S_T < X (put).
(Haug Chapter 5, eq. 5.1)

  Call: K·exp(-rT)·N(d₂)
  Put:  K·exp(-rT)·N(-d₂)

# Example
```julia
cash_or_nothing(100.0, 80.0, 0.75, 0.06, 0.0, 0.35, 10.0, Call())  # → ≈ 5.6733
```
"""
function cash_or_nothing(S::Float64, X::Float64, T::Float64,
                         r::Float64, b::Float64, v::Float64,
                         K_cash::Float64, ot::OptionType) :: Float64
    d2 = _d2(S, X, T, b, v)
    K_cash * exp(-r * T) * (ot isa Call ? Nd(d2) : Nd(-d2))
end

"""
    asset_or_nothing(S, X, T, r, b, v, ::Call/Put) -> Float64

Asset-or-nothing option. Pays S_T if S_T > X (call) or S_T < X (put).
(Haug Chapter 5, eq. 5.2)

  Call: S·exp((b-r)T)·N(d₁)
  Put:  S·exp((b-r)T)·N(-d₁)
"""
function asset_or_nothing(S::Float64, X::Float64, T::Float64,
                          r::Float64, b::Float64, v::Float64,
                          ot::OptionType) :: Float64
    d1 = _d1(S, X, T, b, v)
    S * exp((b - r) * T) * (ot isa Call ? Nd(d1) : Nd(-d1))
end

"""
    gap_option(S, X1, X2, T, r, b, v, ::Call/Put) -> Float64

Gap option. Trigger at X₁, payment S_T - X₂ (call) or X₂ - S_T (put).
(Haug Chapter 5, eq. 5.3)

  Call: S·exp((b-r)T)·N(d₁) - X₂·exp(-rT)·N(d₂)
where d₁, d₂ are computed using X₁ as the strike.
"""
function gap_option(S::Float64, X1::Float64, X2::Float64, T::Float64,
                    r::Float64, b::Float64, v::Float64,
                    ot::OptionType) :: Float64
    d1 = _d1(S, X1, T, b, v)
    d2 = d1 - v * sqrt(T)
    ebrT = exp((b - r) * T)
    erT  = exp(-r * T)
    if ot isa Call
        S * ebrT * Nd(d1) - X2 * erT * Nd(d2)
    else
        X2 * erT * Nd(-d2) - S * ebrT * Nd(-d1)
    end
end

# ── Lookback Options (Haug Chapter 5) ─────────────────────────────────────────

"""
    lookback_fixed(S, X, S_min, S_max, T, r, b, v, ::Call/Put) -> Float64

Fixed-strike lookback option (Conze & Viswanathan 1991).
(Haug Chapter 5, pp. 132-136)

  Call pays: max(S_max - X, 0)   (based on maximum price over life)
  Put  pays: max(X - S_min, 0)   (based on minimum price over life)

# Arguments
- `S_min::Float64` — minimum price observed so far (use S if option just started)
- `S_max::Float64` — maximum price observed so far (use S if option just started)
"""
function lookback_fixed(S::Float64, X::Float64, S_min::Float64, S_max::Float64,
                        T::Float64, r::Float64, b::Float64, v::Float64,
                        ot::OptionType) :: Float64
    v2  = v * v
    ebrT = exp((b - r) * T)
    erT  = exp(-r * T)

    if ot isa Call
        m = S_max
        if X > m
            d1 = _d1(S, X, T, b, v)
            d2 = d1 - v * sqrt(T)
            val = S * ebrT * Nd(d1) - X * erT * Nd(d2)
            if b != 0.0
                val += S * ebrT * v2 / (2.0 * b) *
                       (-(S / X)^(-2.0 * b / v2) * Nd(d1 - 2.0 * b * sqrt(T) / v) +
                        exp(b * T) * Nd(d1))
            end
            val
        else
            d1 = (log(S / m) + (b + v2 / 2.0) * T) / (v * sqrt(T))
            d2 = d1 - v * sqrt(T)
            d1a = (log(S / X) + (b + v2 / 2.0) * T) / (v * sqrt(T))
            bs_call = black_scholes(S, X, T, r, b, v, Call())
            bs_call + (m - X) * erT +
            S * ebrT * v2 / (2.0 * b) *
            (-(S / m)^(-2.0 * b / v2) * Nd(d1 - 2.0 * b * sqrt(T) / v) +
              exp(b * T) * Nd(d1a))
        end
    else  # Put
        m = S_min
        if X < m
            d1 = _d1(S, X, T, b, v)
            d2 = d1 - v * sqrt(T)
            val = X * erT * Nd(-d2) - S * ebrT * Nd(-d1)
            if b != 0.0
                val += S * ebrT * v2 / (2.0 * b) *
                       ((S / X)^(-2.0 * b / v2) * Nd(-d1 + 2.0 * b * sqrt(T) / v) -
                        exp(b * T) * Nd(-d1))
            end
            val
        else
            d1 = (log(S / m) + (b + v2 / 2.0) * T) / (v * sqrt(T))
            d1a = (log(S / X) + (b + v2 / 2.0) * T) / (v * sqrt(T))
            bs_put = black_scholes(S, X, T, r, b, v, Put())
            bs_put + (X - m) * erT +
            S * ebrT * v2 / (2.0 * b) *
            ((S / m)^(-2.0 * b / v2) * Nd(-d1 + 2.0 * b * sqrt(T) / v) -
              exp(b * T) * Nd(-d1a))
        end
    end
end

"""
    lookback_floating(S, S_min, S_max, T, r, b, v, ::Call/Put) -> Float64

Floating-strike lookback option (Goldman, Sosin & Gatto 1979).
(Haug Chapter 5, pp. 136-140)

  Call pays: S_T - min(S over [0,T])   → price based on S_min so far
  Put  pays: max(S over [0,T]) - S_T   → price based on S_max so far

# Arguments
- `S_min::Float64` — minimum price observed so far
- `S_max::Float64` — maximum price observed so far
"""
function lookback_floating(S::Float64, S_min::Float64, S_max::Float64,
                           T::Float64, r::Float64, b::Float64, v::Float64,
                           ot::OptionType) :: Float64
    v2   = v * v
    ebrT = exp((b - r) * T)
    erT  = exp(-r * T)

    if ot isa Call
        m  = S_min
        a1 = (log(S / m) + (b + v2 / 2.0) * T) / (v * sqrt(T))
        a2 = a1 - v * sqrt(T)
        a3 = (log(S / m) + (-b + v2 / 2.0) * T) / (v * sqrt(T))
        S * ebrT * Nd(a1) - m * erT * Nd(a2) -
        S * erT * v2 / (2.0 * b) * (Nd(-a1) - (m / S)^(-2.0 * b / v2) * Nd(-a3))
    else  # Put
        m  = S_max
        b1 = (log(S / m) + (b + v2 / 2.0) * T) / (v * sqrt(T))
        b2 = b1 - v * sqrt(T)
        b3 = (log(S / m) + (-b + v2 / 2.0) * T) / (v * sqrt(T))
        m * erT * Nd(-b2) - S * ebrT * Nd(-b1) +
        S * erT * v2 / (2.0 * b) * (Nd(b1) - (m / S)^(-2.0 * b / v2) * Nd(b3))
    end
end

# ── Chooser Options (Haug Chapter 5) ──────────────────────────────────────────

"""
    chooser_option(S, X, T1, T2, r, b, v) -> Float64

Simple (symmetric) chooser option (Rubinstein 1991).
(Haug Chapter 5, pp. 140-143)

At time T₁ the holder chooses between a European call and put, both with
strike X and expiry T₂ > T₁. Value at time 0:

  Value = C(S,X,T₂,r,b,v) + P(S, X·exp((b-r)(T₂-T₁)), T₁, r, b, v)

# Example
```julia
chooser_option(50.0, 50.0, 0.25, 0.5, 0.08, 0.08, 0.25)  # → ≈ 6.15
```
"""
function chooser_option(S::Float64, X::Float64, T1::Float64, T2::Float64,
                        r::Float64, b::Float64, v::Float64) :: Float64
    # The call with full expiry T2
    call_val = black_scholes(S, X, T2, r, b, v, Call())
    # The put with expiry T1 and adjusted strike
    X_adj = X * exp(-(b - r) * (T2 - T1))  # present value of strike at T1
    put_val = black_scholes(S, X_adj, T1, r, b, v, Put())
    call_val + put_val
end

"""
    complex_chooser(S, Xc, Xp, T, Tc, Tp, r, b, v) -> Float64

Complex chooser option with different strikes and expiries for the embedded
call and put (Rubinstein 1991). (Haug Chapter 5, pp. 143)

Uses the bivariate normal CDF with ρ = √(T / Tc) or √(T / Tp).

# Arguments
- `Xc::Float64` — strike of the embedded call
- `Xp::Float64` — strike of the embedded put
- `T::Float64`  — time to choice date
- `Tc::Float64` — time to expiry of the call (Tc ≥ T)
- `Tp::Float64` — time to expiry of the put  (Tp ≥ T)
"""
function complex_chooser(S::Float64, Xc::Float64, Xp::Float64,
                         T::Float64, Tc::Float64, Tp::Float64,
                         r::Float64, b::Float64, v::Float64) :: Float64
    # Critical asset price I* at T (where embedded call = embedded put)
    # Found via Newton-Raphson
    I = S  # initial guess
    for _ in 1:500
        fc = black_scholes(I, Xc, Tc - T, r, b, v, Call())
        fp = black_scholes(I, Xp, Tp - T, r, b, v, Put())
        diff = fc - fp
        dfdI = bs_delta(I, Xc, Tc - T, r, b, v, Call()) -
               bs_delta(I, Xp, Tp - T, r, b, v, Put())
        abs(dfdI) < 1e-12 && break
        I_new = I - diff / dfdI
        I_new = max(I_new, 1e-4)
        abs(I_new - I) < 1e-8 && (I = I_new; break)
        I = I_new
    end

    d    = (log(S / I) + (b + v^2 / 2.0) * T) / (v * sqrt(T))
    d1c  = (log(S / Xc) + (b + v^2 / 2.0) * Tc) / (v * sqrt(Tc))
    d2c  = d1c - v * sqrt(Tc)
    d1p  = (log(S / Xp) + (b + v^2 / 2.0) * Tp) / (v * sqrt(Tp))
    d2p  = d1p - v * sqrt(Tp)
    ρc   = sqrt(T / Tc)
    ρp   = sqrt(T / Tp)
    ebrT = exp((b - r) * T)
    erTc = exp(-r * Tc)
    erTp = exp(-r * Tp)

    S * ebrT * cbnd(d, d1c, ρc) -
    Xc * erTc * cbnd(d - v * sqrt(T), d2c, ρc) -
    S * ebrT * cbnd(-d, d1p, ρp) +
    Xp * erTp * cbnd(-d + v * sqrt(T), d2p, ρp)
end

# ── Compound Options (Haug Chapter 5) ─────────────────────────────────────────

"""
    compound_option(S, X1, X2, T1, T2, r, b, v, type::Symbol) -> Float64

Compound option (option on an option) using the Geske (1979) formula.
(Haug Chapter 5, pp. 143-149)

`type` must be one of:
- `:call_on_call`  — right to buy a call at time T₁ for price X₁
- `:call_on_put`   — right to buy a put at time T₁ for price X₁
- `:put_on_call`   — right to sell a call at time T₁ for price X₁
- `:put_on_put`    — right to sell a put at time T₁ for price X₁

# Arguments
- `X1::Float64` — strike of the outer option (price paid/received at T₁)
- `X2::Float64` — strike of the inner option (the underlying option's strike)
- `T1::Float64` — expiry of the outer option
- `T2::Float64` — expiry of the inner option (T₂ > T₁)

# Example
```julia
compound_option(50.0, 10.0, 50.0, 0.25, 0.5, 0.10, 0.10, 0.35, :call_on_call)
```
"""
function compound_option(S::Float64, X1::Float64, X2::Float64,
                         T1::Float64, T2::Float64,
                         r::Float64, b::Float64, v::Float64,
                         type::Symbol) :: Float64
    is_call_inner = type in (:call_on_call, :put_on_call)
    inner_ot = is_call_inner ? Call() : Put()

    # Find critical asset price S* at T1 where inner option = X1
    S_star = S
    for _ in 1:500
        inner_val = black_scholes(S_star, X2, T2 - T1, r, b, v, inner_ot)
        diff = inner_val - X1
        delta_s = bs_delta(S_star, X2, T2 - T1, r, b, v, inner_ot)
        abs(delta_s) < 1e-12 && break
        S_new = S_star - diff / delta_s
        S_new = max(S_new, 1e-4)
        abs(S_new - S_star) < 1e-8 && (S_star = S_new; break)
        S_star = S_new
    end

    d1  = (log(S / S_star) + (b + v^2 / 2.0) * T1) / (v * sqrt(T1))
    d2  = d1 - v * sqrt(T1)
    d3  = (log(S / X2) + (b + v^2 / 2.0) * T2) / (v * sqrt(T2))
    d4  = d3 - v * sqrt(T2)
    ρ   = sqrt(T1 / T2)
    ebrT2 = exp((b - r) * T2)
    erT1  = exp(-r * T1)
    erT2  = exp(-r * T2)

    if type == :call_on_call
        S * ebrT2 * cbnd(d1, d3, ρ) -
        X2 * erT2 * cbnd(d2, d4, ρ) -
        X1 * erT1 * Nd(d2)
    elseif type == :put_on_call
        X2 * erT2 * cbnd(-d2, d4, ρ) -
        S  * ebrT2 * cbnd(-d1, d3, ρ) +
        X1 * erT1 * Nd(-d2)
    elseif type == :call_on_put
        X2 * erT2 * cbnd(-d2, -d4, ρ) -
        S  * ebrT2 * cbnd(-d1, -d3, ρ) -
        X1 * erT1 * Nd(-d2)
    else  # :put_on_put
        S * ebrT2 * cbnd(d1, -d3, ρ) -
        X2 * erT2 * cbnd(d2, -d4, ρ) +
        X1 * erT1 * Nd(d2)
    end
end

# ── Exchange Options (Haug Chapter 5) ─────────────────────────────────────────

"""
    exchange_option(S1, S2, T, r, b1, b2, v1, v2, ρ) -> Float64

Margrabe (1978) formula for the option to exchange asset 2 for asset 1.
Payoff: max(S1_T - S2_T, 0).
(Haug Chapter 5, eq. 5.13)

The combined volatility is: v = √(v₁² - 2ρv₁v₂ + v₂²)

# Example
```julia
exchange_option(22.0, 20.0, 0.5, 0.10, 0.0, 0.0, 0.20, 0.15, 0.50)  # → ≈ 2.19
```
"""
function exchange_option(S1::Float64, S2::Float64, T::Float64,
                         r::Float64, b1::Float64, b2::Float64,
                         v1::Float64, v2::Float64, ρ::Float64) :: Float64
    v  = sqrt(v1^2 - 2.0 * ρ * v1 * v2 + v2^2)
    d1 = (log(S1 / S2) + (b1 - b2 + v^2 / 2.0) * T) / (v * sqrt(T))
    d2 = d1 - v * sqrt(T)
    S1 * exp((b1 - r) * T) * Nd(d1) - S2 * exp((b2 - r) * T) * Nd(d2)
end

# ── Forward Start Options (Haug Chapter 5) ────────────────────────────────────

"""
    forward_start(S, α, T1, T2, r, b, v, ::Call/Put) -> Float64

Rubinstein (1990) forward-start option.
(Haug Chapter 5, eq. 5.14)

At time T₁ the strike is set to α·S_{T₁} (α < 1 → ITM call, α > 1 → OTM call).
The option then expires at T₂.

# Arguments
- `α::Float64` — strike-setting fraction (e.g. α=1 → ATM, α=1.05 → 5% OTM call)
- `T1::Float64` — time until strike is set
- `T2::Float64` — time to expiry of the option (T₂ > T₁)

# Example
```julia
forward_start(60.0, 1.1, 0.25, 1.0, 0.08, 0.08, 0.30, Call())  # → ≈ 4.40
```
"""
function forward_start(S::Float64, α::Float64, T1::Float64, T2::Float64,
                       r::Float64, b::Float64, v::Float64,
                       ot::OptionType) :: Float64
    # Value = exp((b-r)T₁) · BS(S, αS, T₂-T₁, r, b, v, ot) evaluated at S/X = 1/α
    τ  = T2 - T1
    # Equivalent to pricing a normalised option at moneyness 1/α
    d1 = (log(1.0 / α) + (b + v^2 / 2.0) * τ) / (v * sqrt(τ))
    d2 = d1 - v * sqrt(τ)
    ebrT1 = exp((b - r) * T1)
    ebrτ  = exp((b - r) * τ)
    erτ   = exp(-r * τ)
    if ot isa Call
        S * ebrT1 * (ebrτ * Nd(d1) - α * erτ * Nd(d2))
    else
        S * ebrT1 * (α * erτ * Nd(-d2) - ebrτ * Nd(-d1))
    end
end

# ── Supershare Options (Haug Chapter 5) ───────────────────────────────────────

"""
    supershare(S, XL, XH, T, r, b, v) -> Float64

Supershare option (Hakansson 1976). Pays S_T / X_L if X_L ≤ S_T < X_H, else 0.
(Haug Chapter 5, eq. 5.17)

  Value = (S/X_L)·exp((b-r)T)·[N(d₁(X_L)) - N(d₁(X_H))]

# Example
```julia
supershare(100.0, 90.0, 110.0, 0.25, 0.10, 0.10, 0.20)  # → ≈ 0.97
```
"""
function supershare(S::Float64, XL::Float64, XH::Float64, T::Float64,
                    r::Float64, b::Float64, v::Float64) :: Float64
    d1L = _d1(S, XL, T, b, v)
    d1H = _d1(S, XH, T, b, v)
    (S / XL) * exp((b - r) * T) * (Nd(d1L) - Nd(d1H))
end

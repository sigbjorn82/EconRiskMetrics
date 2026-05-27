# American.jl — Analytic approximations for American option pricing
# Implements three methods from Haug Chapter 3:
#   1. Barone-Adesi & Whaley (1987) quadratic approximation
#   2. Bjerksund-Stensland (1993) flat-boundary approximation
#   3. Bjerksund-Stensland (2002) two-piece piecewise boundary

# ── Barone-Adesi & Whaley (1987) ──────────────────────────────────────────────

# Internal: BAW quadratic coefficient q for call (M, N, k from Haug p.68)
function _baw_q2(r::Float64, b::Float64, v::Float64, T::Float64) :: Float64
    M = 2.0 * r / (v * v)
    N = 2.0 * b / (v * v)
    k = 1.0 - exp(-r * T)
    (-(N - 1.0) + sqrt((N - 1.0)^2 + 4.0 * M / k)) / 2.0
end

function _baw_q1(r::Float64, b::Float64, v::Float64, T::Float64) :: Float64
    M = 2.0 * r / (v * v)
    N = 2.0 * b / (v * v)
    k = 1.0 - exp(-r * T)
    (-(N - 1.0) - sqrt((N - 1.0)^2 + 4.0 * M / k)) / 2.0
end

# Internal: Newton-Raphson for BAW critical price S*
# For call: find S* where BS_call(S*) + (1 - exp((b-r)T)*N(d1(S*)))*S*/q2 = S* - X
function _baw_critical_call(X::Float64, T::Float64, r::Float64,
                            b::Float64, v::Float64, q2::Float64) :: Float64
    # Seed: Haug p.69
    S_seed = X / (1.0 - 1.0 / q2)
    S_seed = max(S_seed, X * 0.5)   # ensure positive seed

    S = S_seed
    for _ in 1:1000
        d1_s = _d1(S, X, T, b, v)
        ebrT  = exp((b - r) * T)
        bs_c  = black_scholes(S, X, T, r, b, v, Call())
        lhs   = bs_c + (1.0 - ebrT * Nd(d1_s)) * S / q2 - (S - X)
        dlhs  = ebrT * Nd(d1_s) * (1.0 - 1.0 / q2) +
                (1.0 - ebrT * nd(d1_s) / (v * sqrt(T))) / q2 - 1.0
        S_new = S - lhs / dlhs
        S_new = max(S_new, X * 0.01)
        if abs(S_new - S) < 1e-8
            return S_new
        end
        S = S_new
    end
    S
end

# For put: find S** where BS_put(S**) + (exp((b-r)T)*N(-d1(S**)) - 1)*S**/q1 = X - S**
function _baw_critical_put(X::Float64, T::Float64, r::Float64,
                           b::Float64, v::Float64, q1::Float64) :: Float64
    S_seed = X / (1.0 - 1.0 / q1)
    S_seed = min(S_seed, X * 2.0)

    S = S_seed
    for _ in 1:1000
        d1_s = _d1(S, X, T, b, v)
        ebrT  = exp((b - r) * T)
        bs_p  = black_scholes(S, X, T, r, b, v, Put())
        lhs   = bs_p - (1.0 - ebrT * Nd(-d1_s)) * S / q1 - (X - S)
        dlhs  = -ebrT * Nd(-d1_s) * (1.0 - 1.0 / q1) +
                1.0 - (1.0 + ebrT * nd(d1_s) / (v * sqrt(T))) / q1
        S_new = S - lhs / dlhs
        S_new = max(S_new, X * 0.01)
        if abs(S_new - S) < 1e-8
            return S_new
        end
        S = S_new
    end
    S
end

"""
    baw_american(S, X, T, r, b, v, ::Call) -> Float64
    baw_american(S, X, T, r, b, v, ::Put)  -> Float64

Barone-Adesi and Whaley (1987) quadratic approximation for American options.
(Haug Chapter 3, pp. 67-70)

For calls, the American premium over the European price is:
  A₂·(S/S*)^q₂  if S < S*
  intrinsic      if S ≥ S*

where S* is the critical asset price found by Newton-Raphson, and q₂ and A₂
are derived from the quadratic approximation to the early exercise boundary.

# Note
When b ≥ r for calls, early exercise of an American call is never optimal;
the function returns the European Black-Scholes price in that case.

# Example
```julia
baw_american(100.0, 100.0, 0.5, 0.10, 0.0, 0.25, Call())  # → ≈ 6.7611  (Haug p.68)
```
"""
function baw_american(S::Float64, X::Float64, T::Float64,
                      r::Float64, b::Float64, v::Float64, ::Call) :: Float64
    # American call with b ≥ r is never optimal to exercise early
    if b >= r
        return black_scholes(S, X, T, r, b, v, Call())
    end

    q2    = _baw_q2(r, b, v, T)
    Sstar = _baw_critical_call(X, T, r, b, v, q2)
    d1s   = _d1(Sstar, X, T, b, v)
    A2    = (Sstar / q2) * (1.0 - exp((b - r) * T) * Nd(d1s))

    if S >= Sstar
        return S - X
    end
    black_scholes(S, X, T, r, b, v, Call()) + A2 * (S / Sstar)^q2
end

function baw_american(S::Float64, X::Float64, T::Float64,
                      r::Float64, b::Float64, v::Float64, ::Put) :: Float64
    q1    = _baw_q1(r, b, v, T)
    Sstar = _baw_critical_put(X, T, r, b, v, q1)
    d1s   = _d1(Sstar, X, T, b, v)
    A1    = -(Sstar / q1) * (1.0 - exp((b - r) * T) * Nd(-d1s))

    if S <= Sstar
        return X - S
    end
    black_scholes(S, X, T, r, b, v, Put()) + A1 * (S / Sstar)^q1
end

baw_american(c::OptionContract) =
    baw_american(c.S, c.X, c.T, c.r, c.b, c.v, c.type)

# ── Bjerksund-Stensland 1993 ───────────────────────────────────────────────────

# Internal: the phi function used in BS1993 (Haug p.72)
function _bs93_phi(S::Float64, T::Float64, γ::Float64, H::Float64,
                   X::Float64, r::Float64, b::Float64, v::Float64) :: Float64
    # Haug p.71 / VBA: negative sign in d1 and d2 (matches Haug appendix VBA)
    d1  = -(log(S / H)       + (b + (γ - 0.5) * v^2) * T) / (v * sqrt(T))
    d2  = -(log(X^2 / (S*H)) + (b + (γ - 0.5) * v^2) * T) / (v * sqrt(T))
    λ   = -r + γ * b + 0.5 * γ * (γ - 1.0) * v^2
    eλT = exp(λ * T)
    S^γ * eλT * (Nd(d1) - (X / S)^(2.0 * (γ + b / v^2 - 0.5)) * Nd(d2))
end

"""
    bjerksund_stensland_1993(S, X, T, r, b, v, ::Call) -> Float64
    bjerksund_stensland_1993(S, X, T, r, b, v, ::Put)  -> Float64

Bjerksund and Stensland (1993) analytic approximation for American options.
(Haug Chapter 3, pp. 70-75)

Uses a flat early-exercise boundary β = X·r/(r-b) as an approximation.
The put is computed via the exact call-put symmetry transformation:
  P(S,X,T,r,b,v) = C(X,S,T,r-b,-b,v)

# Example
```julia
bjerksund_stensland_1993(100.0, 100.0, 0.5, 0.10, 0.0, 0.25, Call())  # → ≈ 6.76
```
"""
function bjerksund_stensland_1993(S::Float64, X::Float64, T::Float64,
                                  r::Float64, b::Float64, v::Float64,
                                  ::Call) :: Float64
    if b >= r
        return black_scholes(S, X, T, r, b, v, Call())
    end

    β  = (0.5 - b / v^2) + sqrt((b / v^2 - 0.5)^2 + 2.0 * r / v^2)
    B∞ = β / (β - 1.0) * X
    B0 = max(X, r / (r - b) * X)
    ht = -(b * T + 2.0 * v * sqrt(T)) * B0 / (B∞ - B0)
    I  = B0 + (B∞ - B0) * (1.0 - exp(ht))

    if S >= I
        return S - X
    end

    α = (I - X) * I^(-β)
    α * S^β -
    α * _bs93_phi(S, T, β, I, I, r, b, v) +
    _bs93_phi(S, T, 1.0, I, X, r, b, v) -
    _bs93_phi(S, T, 1.0, X, X, r, b, v) -
    X * _bs93_phi(S, T, 0.0, I, X, r, b, v) +
    X * _bs93_phi(S, T, 0.0, X, X, r, b, v)
end

function bjerksund_stensland_1993(S::Float64, X::Float64, T::Float64,
                                  r::Float64, b::Float64, v::Float64,
                                  ::Put) :: Float64
    # Exact symmetry: P(S,X,T,r,b,v) = C(X,S,T,r-b,-b,v)
    bjerksund_stensland_1993(X, S, T, r - b, -b, v, Call())
end

bjerksund_stensland_1993(c::OptionContract) =
    bjerksund_stensland_1993(c.S, c.X, c.T, c.r, c.b, c.v, c.type)

# ── Bjerksund-Stensland 2002 ───────────────────────────────────────────────────

# Internal: the psi function used in BS2002 (Haug p.77-78)
function _bs02_psi(S::Float64, T2::Float64, γ::Float64, H::Float64,
                   X2::Float64, T1::Float64, r::Float64, b::Float64,
                   v::Float64) :: Float64
    e1  = (log(S / H) + (b + (γ - 0.5) * v^2) * T1) / (v * sqrt(T1))
    e2  = (log(X2^2 / (S * H)) + (b + (γ - 0.5) * v^2) * T1) / (v * sqrt(T1))
    e3  = (log(S / H) - (b + (γ - 0.5) * v^2) * T1) / (v * sqrt(T1))
    e4  = (log(X2^2 / (S * H)) - (b + (γ - 0.5) * v^2) * T1) / (v * sqrt(T1))
    f1  = (log(S / X2) + (b + (γ - 0.5) * v^2) * T2) / (v * sqrt(T2))
    f2  = (log(H^2 / (S * X2)) + (b + (γ - 0.5) * v^2) * T2) / (v * sqrt(T2))
    ρ   = sqrt(T1 / T2)
    λ   = -r + γ * b + 0.5 * γ * (γ - 1.0) * v^2
    eλT2 = exp(λ * T2)

    S^γ * eλT2 * (
        cbnd(-e1, -f1,  ρ) -
        (H / S)^(2.0 * (γ + b / v^2 - 0.5)) * cbnd(-e2, -f2,  ρ) -
        (H / X2)^(2.0 * (γ + b / v^2 - 0.5)) * cbnd(-e3, -f1, -ρ) +
        (S / H)^(2.0 * (γ + b / v^2 - 0.5)) * cbnd(-e4, -f2, -ρ)
    )
end

# Internal: compute the two-piece BS2002 call price given trigger prices I1, I2
function _bs02_call_value(S::Float64, X::Float64, T::Float64,
                          r::Float64, b::Float64, v::Float64,
                          I1::Float64, I2::Float64) :: Float64
    T1 = T / 2.0
    # BS2002 uses the perpetual β∞ (T-independent), not the BAW T-dependent q2.
    β = (0.5 - b / v^2) + sqrt((b / v^2 - 0.5)^2 + 2.0 * r / v^2)
    β1 = β
    β2 = β
    α1 = (I1 - X) * I1^(-β)
    α2 = (I2 - X) * I2^(-β)

    if S >= I2
        return S - X
    end

    α2 * S^β2 -
    α2 * _bs93_phi(S, T, β2, I2, I2, r, b, v) +
    _bs93_phi(S, T, 1.0, I2, X, r, b, v) -
    _bs93_phi(S, T, 1.0, I1, X, r, b, v) +
    _bs93_phi(S, T1, 1.0, I1, X, r, b, v) -
    X * _bs93_phi(S, T, 0.0, I2, X, r, b, v) +
    X * _bs93_phi(S, T, 0.0, I1, X, r, b, v) -
    X * _bs93_phi(S, T1, 0.0, I1, X, r, b, v) +
    α1 * _bs93_phi(S, T, β1, I1, I2, r, b, v) -
    α1 * _bs02_psi(S, T, β1, I1, I2, T1, r, b, v) +
    _bs02_psi(S, T, 1.0, I1, I2, T1, r, b, v) -
    X * _bs02_psi(S, T, 0.0, I1, I2, T1, r, b, v)
end

"""
    bjerksund_stensland(S, X, T, r, b, v, ::Call) -> Float64
    bjerksund_stensland(S, X, T, r, b, v, ::Put)  -> Float64

Bjerksund and Stensland (2002) improved approximation for American options.
(Haug Chapter 3, pp. 75-79)

Uses a two-piece piecewise linear early-exercise boundary: trigger prices I₁
(for half-life T/2) and I₂ (for full expiry T). This generally outperforms
BAW and BS1993 in accuracy, especially for long-dated options.

The put is computed via the exact call-put symmetry transformation:
  P(S,X,T,r,b,v) = C(X,S,T,r-b,-b,v)

# Example
```julia
bjerksund_stensland(100.0, 100.0, 0.5, 0.10, 0.0, 0.25, Call())  # → ≈ 6.7627
```
"""
function bjerksund_stensland(S::Float64, X::Float64, T::Float64,
                             r::Float64, b::Float64, v::Float64,
                             ::Call) :: Float64
    if b >= r
        return black_scholes(S, X, T, r, b, v, Call())
    end

    T1 = T / 2.0

    # Trigger price at T: same seed as BS1993
    β∞ = (0.5 - b / v^2) + sqrt((b / v^2 - 0.5)^2 + 2.0 * r / v^2)
    B∞ = β∞ / (β∞ - 1.0) * X
    B0 = max(X, r / (r - b) * X)
    h1 = -(b * T1 + 2.0 * v * sqrt(T1)) * B0 / (B∞ - B0)
    h2 = -(b * T  + 2.0 * v * sqrt(T))  * B0 / (B∞ - B0)
    I1 = B0 + (B∞ - B0) * (1.0 - exp(h1))
    I2 = B0 + (B∞ - B0) * (1.0 - exp(h2))

    _bs02_call_value(S, X, T, r, b, v, I1, I2)
end

function bjerksund_stensland(S::Float64, X::Float64, T::Float64,
                             r::Float64, b::Float64, v::Float64,
                             ::Put) :: Float64
    # Exact symmetry: P(S,X,T,r,b,v) = C(X,S,T,r-b,-b,v)
    bjerksund_stensland(X, S, T, r - b, -b, v, Call())
end

bjerksund_stensland(c::OptionContract) =
    bjerksund_stensland(c.S, c.X, c.T, c.r, c.b, c.v, c.type)

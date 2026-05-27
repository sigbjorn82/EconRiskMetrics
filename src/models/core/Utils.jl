# Utils.jl — Mathematical utilities for option pricing
# All functions are internal (not exported from the Options module).
# @inline is applied to hot-path functions called inside tree and MC loops.

# ── Standard Normal Distribution ───────────────────────────────────────────────

"""
    nd(x::Float64) -> Float64

Standard normal probability density function N'(x).

    N'(x) = exp(-x²/2) / √(2π)
"""
@inline nd(x::Float64) = exp(-0.5 * x * x) / sqrt(2.0 * π)

"""
    Nd(x::Float64) -> Float64

Standard normal cumulative distribution function N(x).

Uses the Abramowitz & Stegun (1964) polynomial approximation (eq. 26.2.17),
accurate to |ε| < 7.5×10⁻⁸ — no external packages required.

# Examples
```julia
Nd(0.0)   # → 0.5
Nd(1.96)  # → ≈ 0.975
```
"""
@inline function Nd(x::Float64) :: Float64
    if x < 0.0
        return 1.0 - Nd(-x)
    end
    # A&S 26.2.17 coefficients
    t    = 1.0 / (1.0 + 0.2316419 * x)
    poly = t * (0.319381530 +
           t * (-0.356563782 +
           t * (1.781477937 +
           t * (-1.821255978 +
           t * 1.330274429))))
    1.0 - nd(x) * poly
end

# ── Black-Scholes d₁ and d₂ ────────────────────────────────────────────────────

"""
    _d1(S, X, T, b, v) -> Float64

Haug unified d₁ formula:

    d₁ = [log(S/X) + (b + v²/2)·T] / (v·√T)

where `b` is the cost-of-carry parameter.
"""
@inline function _d1(S::Float64, X::Float64, T::Float64,
                     b::Float64, v::Float64) :: Float64
    (log(S / X) + (b + 0.5 * v * v) * T) / (v * sqrt(T))
end

"""
    _d2(S, X, T, b, v) -> Float64

Haug unified d₂ formula:

    d₂ = d₁ - v·√T
"""
@inline function _d2(S::Float64, X::Float64, T::Float64,
                     b::Float64, v::Float64) :: Float64
    _d1(S, X, T, b, v) - v * sqrt(T)
end

# ── Bivariate Normal CDF ───────────────────────────────────────────────────────

# 6-point Gauss-Legendre nodes and weights on [-1, 1].
# Using 3 symmetric pairs, matching Haug Appendix B / Drezner-Wesolowsky (1990).
const _GL6_X = ( 0.9324695142031522,  0.6612093864662647,  0.2386191860831970,
                -0.9324695142031522, -0.6612093864662647, -0.2386191860831970)
const _GL6_W = ( 0.1713244923791705,  0.3607615730481384,  0.4679139345726904,
                 0.1713244923791705,  0.3607615730481384,  0.4679139345726904)

# Internal: computes P(X > dh, Y > dk) for bivariate standard normal with correlation r.
# Faithfully translated from Haug Appendix B (Drezner-Wesolowsky 1990 algorithm).
function _cbnd_upper(dh::Float64, dk::Float64, r::Float64) :: Float64
    hk  = dh * dk
    bvn = 0.0

    if abs(r) < 0.925
        hs  = (dh * dh + dk * dk) / 2.0
        asr = asin(r)
        for k in 1:6
            sn   = sin(asr * (_GL6_X[k] + 1.0) / 2.0)
            bvn += _GL6_W[k] * exp((sn * hk - hs) / (1.0 - sn * sn))
        end
        return bvn * asr / (4.0 * π) + Nd(-dh) * Nd(-dk)
    end

    # High |r| branch — Drezner-Wesolowsky transformation.
    # Transform: if r < 0, negate dk (and hk accordingly) so we work with r ≥ 0.
    dk2 = r < 0.0 ? -dk : dk
    hk2 = dh * dk2          # = -hk when r < 0

    if abs(r) < 1.0
        as_ = (1.0 - r) * (1.0 + r)   # = 1 - r²
        a   = sqrt(as_)
        bs  = (dh - dk2)^2
        c   = (4.0 - hk2) / 8.0
        d   = (12.0 - hk2) / 16.0
        asr = -(bs / as_ + hk2) / 2.0
        if asr > -100.0
            bvn = a * exp(asr) *
                  (1.0 - c * (bs - as_) * (1.0 - d * bs / 5.0) / 3.0 +
                   c * d * as_ * as_ / 5.0)
        end
        if -hk2 < 100.0
            b    = sqrt(bs)
            bvn -= exp(-hk2 / 2.0) * sqrt(2.0 * π) *
                   Nd(-b / a) * b * (1.0 - c * bs * (1.0 - d * bs / 5.0) / 3.0)
        end
        a2 = a / 2.0
        for k in 1:6
            xs   = (a2 * _GL6_X[k])^2
            rs   = sqrt(1.0 - xs)
            asr2 = -(bs / xs + hk2) / 2.0
            if asr2 > -100.0
                bvn += a2 * _GL6_W[k] * exp(asr2) *
                       (exp(-hk2 * (1.0 - rs) / (2.0 * (1.0 + rs))) / rs -
                        (1.0 + c * xs * (1.0 + d * xs)))
            end
        end
        bvn = -bvn / (2.0 * π)
    end

    # Final correction (uses original-sign r, but transformed dk2)
    if r > 0.0
        bvn += Nd(-max(dh, dk2))
    else
        bvn = -bvn
        if dk2 > dh
            bvn += Nd(dk2) - Nd(dh)
        end
    end

    return bvn
end

"""
    cbnd(a::Float64, b::Float64, ρ::Float64) -> Float64

Bivariate standard normal CDF: P(X ≤ a, Y ≤ b) for (X, Y) jointly standard
normal with correlation ρ.

Implements the Drezner-Wesolowsky (1990) algorithm with 6-point
Gauss-Legendre quadrature, as presented in Haug Appendix B.
Accurate to approximately 1e-7 for |ρ| < 1.

Used for compound options, complex choosers, and partial-time barrier options.
"""
function cbnd(a::Float64, b::Float64, ρ::Float64) :: Float64
    # P(X ≤ a, Y ≤ b) = P(-X ≥ -a, -Y ≥ -b) = P(X > -a, Y > -b)  [by symmetry of std normal]
    _cbnd_upper(-a, -b, ρ)
end

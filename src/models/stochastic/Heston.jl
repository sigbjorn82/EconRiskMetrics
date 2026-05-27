# Heston.jl — Semi-analytic Heston (1993) stochastic volatility model
#
# The Heston model:
#   dS = (r-q)S dt + √V · S dW₁
#   dV = κ(θ-V)dt + σᵥ√V dW₂,   corr(dW₁, dW₂) = ρ dt
#
# Parameters:
#   v0  — initial variance (σ₀² = v0)
#   κ   — mean reversion speed
#   θ   — long-run variance
#   σ_v — vol-of-vol
#   ρ   — correlation between asset and variance processes
#
# Feller condition for non-zero variance: 2κθ > σᵥ²
#
# Pricing via characteristic function Fourier inversion.
# Uses the Albrecher-Mayer-Schachermayer-Teugels (2007) sign convention
# to avoid the branch-cut discontinuity of the original Heston (1993) formula.

# ── Gauss-Laguerre Quadrature (32-point) ─────────────────────────────────────
# Nodes and weights computed via Golub-Welsch algorithm from the symmetric
# Jacobi (tridiagonal) matrix for Laguerre polynomials L_n^(0):
#   diagonal  = [2k-1  for k=1:n]
#   off-diag  = [√k    for k=1:n-1]
# Nodes = eigenvalues; weights = (first eigenvector component)^2.
const (_GL32_X, _GL32_W) = let n = 32
    d = [Float64(2k - 1) for k in 1:n]
    e = [sqrt(Float64(k)) for k in 1:n-1]
    F = LinearAlgebra.eigen(LinearAlgebra.SymTridiagonal(d, e))
    Tuple(F.values), Tuple(F.vectors[1, :] .^ 2)
end

# ── Characteristic Function ───────────────────────────────────────────────────

"""
    _heston_cf(u, S, X, T, r, q, v0, κ, θ, σ_v, ρ) -> Complex{Float64}

Heston (1993) characteristic function φ(u) for the log price log(S_T/X).

Uses the Albrecher et al. (2007) formulation (sign convention for d and g)
which avoids the branch-cut discontinuity present in the original Heston (1993)
paper for long maturities or large |u|.

The characteristic function is:
  φ(u) = exp(C(T,u) + D(T,u)·v0 + i·u·log(S/X))

where:
  d = √((ρ·σᵥ·u·i - κ)² + σᵥ²·(u·i + u²))
  g = (κ - ρ·σᵥ·u·i - d) / (κ - ρ·σᵥ·u·i + d)    ← AMST sign convention
  C = (r-q)·u·i·T + (κθ/σᵥ²)·[(κ - ρσᵥui - d)·T - 2·log((1 - g·exp(-dT))/(1-g))]
  D = [(κ - ρσᵥui - d)/σᵥ²]·[(1 - exp(-dT))/(1 - g·exp(-dT))]
"""
function _heston_cf(u::Number, S::Float64, X::Float64, T::Float64,
                    r::Float64, q::Float64,
                    v0::Float64, κ::Float64, θ::Float64,
                    σ_v::Float64, ρ::Float64) :: Complex{Float64}
    ui   = complex(u) * im                                  # i·u (works for real or complex u)
    xi   = κ - ρ * σ_v * ui                                # κ - ρσᵥ·iu
    d    = sqrt(xi^2 + σ_v^2 * (ui + complex(u)^2))       # discriminant
    # AMST sign convention: use (xi - d) in numerator of g
    g    = (xi - d) / (xi + d)
    expdT = exp(-d * T)
    C    = (r - q) * ui * T +
           (κ * θ / σ_v^2) * ((xi - d) * T - 2.0 * log((1.0 - g * expdT) / (1.0 - g)))
    D    = ((xi - d) / σ_v^2) * (1.0 - expdT) / (1.0 - g * expdT)
    exp(C + D * v0 + ui * log(S / X))
end

# ── Semi-analytic Price via Fourier Inversion ────────────────────────────────

"""
    heston_price(S, X, T, r, q, v0, κ, θ, σ_v, ρ, ::Call/Put;
                 n_quad=32) -> Float64

Semi-analytic Heston (1993) option price via characteristic function
Fourier inversion using 32-point Gauss-Laguerre quadrature.

The integral form (Lewis 2001 / simplified single-integral):
  Call = S·exp(-qT) - (√(S·X·exp(-(r+q)T)) / π) · ∫₀^∞ Re[φ(u - i/2) / (u² + 1/4)] du

Put via put-call parity: Put = Call - S·exp(-qT) + X·exp(-rT)

# Arguments
- `v0::Float64`  — initial variance (annualised; e.g. 0.04 = 20% vol)
- `κ::Float64`   — mean-reversion speed
- `θ::Float64`   — long-run variance
- `σ_v::Float64` — vol-of-vol
- `ρ::Float64`   — asset/vol correlation (typically negative, e.g. -0.7)

# Note
Feller condition 2κθ > σᵥ² ensures the variance process stays positive.
A warning is emitted (but computation continues) if it is violated.

# Example
```julia
# Lewis (2000) Table 1 benchmark:
heston_price(100.0, 100.0, 0.5, 0.05, 0.02, 0.04, 2.0, 0.04, 0.5, -0.7, Call())
# → ≈ 5.79
```
"""
function heston_price(S::Float64, X::Float64, T::Float64,
                      r::Float64, q::Float64,
                      v0::Float64, κ::Float64, θ::Float64,
                      σ_v::Float64, ρ::Float64,
                      ot::OptionType;
                      n_quad::Int=32) :: Float64
    if 2.0 * κ * θ <= σ_v^2
        @warn "Heston: Feller condition 2κθ > σᵥ² violated (2κθ=$(2κ*θ), σᵥ²=$(σ_v^2)). Results may be inaccurate."
    end

    # Lewis (2001) single-integral formula:
    # Call = S·exp(-qT) - (√(S·X)·exp(-(r+q)T/2) / π) · I
    # where I = ∫₀^∞ Re[φ(u - i/2) / (u² + 1/4)] du
    #
    # We shift u → u - i/2 in the CF, which dampens the integrand
    # and avoids the singularity at u=0.

    F   = S * exp((r - q) * T)    # forward price
    df  = exp(-r * T)
    k   = log(X / F)               # log-moneyness k = log(X/F); negative for ITM calls

    # Lewis (2001) single-integral formula:
    #   Call = df·(F-X)/2 + df·√(F·X)/π · ∫₀^∞ Re[φ_fwd(u-i/2)·e^{-iuk}] / (u²+1/4) du
    #
    # φ_fwd(u) = CF of log(S_T/F) under T-forward measure
    #          = _heston_cf(u, F, F, T, 0, 0, ...) with X=F so log(F/F)=0 drops out
    #
    # Evaluate φ_fwd at the half-plane shift u → u - i/2:
    #   When u_c = u - i/2:  u_c·i = u·i + 1/2
    #   ui + u_c² = (u·i + 1/2) + (u-i/2)² = u² + 1/4  (cancels denominator)
    #
    # GL quadrature: ∫₀^∞ f(u) du = ∫₀^∞ e^{-u}·[f(u)·e^u] du ≈ Σ wⱼ·f(uⱼ)·exp(uⱼ)
    integral = 0.0
    for j in 1:min(n_quad, length(_GL32_X))
        u_gl  = _GL32_X[j]
        w_gl  = _GL32_W[j]
        u_c   = complex(u_gl) - 0.5im          # half-plane shift
        # φ_fwd(u - i/2): call CF with X=F so the log(S/X) term = log(F/F) = 0
        cf_val    = _heston_cf(u_c, F, F, T, 0.0, 0.0, v0, κ, θ, σ_v, ρ)
        # Lewis integrand (real part only): Re[φ(u-i/2)·e^{-iuk}] / (u²+1/4)
        integrand = real(cf_val * exp(-im * u_gl * k)) / (u_gl^2 + 0.25) * exp(u_gl)
        integral += w_gl * integrand
    end

    call_price = df * (F - X) / 2.0 + df * sqrt(F * X) / π * integral

    # Clamp to no-arbitrage bounds
    call_lower = max(S * exp(-q * T) - X * df, 0.0)
    call_price  = max(call_price, call_lower)

    if ot isa Call
        return call_price
    else
        # Put-call parity
        return call_price - S * exp(-q * T) + X * df
    end
end

# ── Numerical Greeks via Finite Differences ──────────────────────────────────

"""
    heston_greeks(S, X, T, r, q, v0, κ, θ, σ_v, ρ, ::OptionType;
                  bump_s=0.01, bump_v=0.01) -> NamedTuple

Numerical Greeks for the Heston model via central finite differences.

- `delta`  ≈ (P(S+ε) - P(S-ε)) / 2ε
- `gamma`  ≈ (P(S+ε) - 2P(S) + P(S-ε)) / ε²
- `vega`   ≈ (P(v0+δ) - P(v0-δ)) / 2δ    (w.r.t. initial variance v0)
- `theta`  ≈ (P(T+h) - P(T-h)) / 2h       (per year; positive for longer T)

# Arguments
- `bump_s::Float64` — absolute bump size for delta/gamma (default 0.01 × S)
- `bump_v::Float64` — absolute bump for vega (default 0.01)

# Returns
NamedTuple with fields: `price, delta, gamma, vega, theta`
"""
function heston_greeks(S::Float64, X::Float64, T::Float64,
                       r::Float64, q::Float64,
                       v0::Float64, κ::Float64, θ::Float64,
                       σ_v::Float64, ρ::Float64,
                       ot::OptionType;
                       bump_s::Float64=0.01,
                       bump_v::Float64=0.01) :: NamedTuple
    εS  = S * bump_s
    εV  = bump_v
    εT  = max(T * 0.01, 1.0 / 365.0)

    p0  = heston_price(S,      X, T,    r, q, v0,    κ, θ, σ_v, ρ, ot)
    pSu = heston_price(S + εS, X, T,    r, q, v0,    κ, θ, σ_v, ρ, ot)
    pSd = heston_price(S - εS, X, T,    r, q, v0,    κ, θ, σ_v, ρ, ot)
    pVu = heston_price(S,      X, T,    r, q, v0+εV, κ, θ, σ_v, ρ, ot)
    pVd = heston_price(S,      X, T,    r, q, max(v0-εV, 1e-6), κ, θ, σ_v, ρ, ot)
    pTu = heston_price(S,      X, T+εT, r, q, v0,    κ, θ, σ_v, ρ, ot)
    pTd = heston_price(S,      X, T-εT, r, q, v0,    κ, θ, σ_v, ρ, ot)

    delta = (pSu - pSd) / (2.0 * εS)
    gamma = (pSu - 2.0 * p0 + pSd) / εS^2
    vega  = (pVu - pVd) / (2.0 * εV)
    theta = (pTu - pTd) / (2.0 * εT)

    (price=p0, delta=delta, gamma=gamma, vega=vega, theta=theta)
end

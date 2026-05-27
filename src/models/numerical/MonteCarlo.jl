# MonteCarlo.jl — Monte Carlo simulation for option pricing
# Uses only Base Julia: Random (seeding), Statistics (mean/std).
# All functions return NamedTuples (price, std_err).

# ── European Options via GBM ───────────────────────────────────────────────────

"""
    mc_european(S, X, T, r, b, v, ::OptionType;
                n_paths=100_000, seed=42) -> NamedTuple

Monte Carlo price for a European option using exact GBM terminal simulation.

  S_T = S · exp((b - v²/2)·T + v·√T·Z),   Z ~ N(0,1)

Uses antithetic variates (Z and -Z) for variance reduction — halves the
standard error at no extra cost for symmetric payoffs.

# Returns
`(price::Float64, std_err::Float64)` — point estimate and 1-σ MC error.

# Example
```julia
r = mc_european(100.0, 100.0, 1.0, 0.05, 0.05, 0.20, Call())
r.price    # ≈ Black-Scholes price
r.std_err  # Monte Carlo standard error
```
"""
function mc_european(S::Float64, X::Float64, T::Float64,
                     r::Float64, b::Float64, v::Float64,
                     ot::OptionType;
                     n_paths::Int=100_000,
                     seed::Int=42) :: NamedTuple
    rng    = Random.MersenneTwister(seed)
    n_half = n_paths ÷ 2
    drift  = (b - 0.5 * v^2) * T
    vvT    = v * sqrt(T)
    erT    = exp(-r * T)

    payoffs = Vector{Float64}(undef, 2 * n_half)
    for i in 1:n_half
        z         = Random.randn(rng)
        S_T_pos   = S * exp(drift + vvT * z)
        S_T_neg   = S * exp(drift - vvT * z)
        payoffs[2i-1] = ot isa Call ? max(S_T_pos - X, 0.0) : max(X - S_T_pos, 0.0)
        payoffs[2i]   = ot isa Call ? max(S_T_neg - X, 0.0) : max(X - S_T_neg, 0.0)
    end

    mean_pv  = Statistics.mean(payoffs) * erT
    std_pv   = Statistics.std(payoffs)  * erT / sqrt(Float64(2 * n_half))
    (price=mean_pv, std_err=std_pv)
end

# ── Asian Options ──────────────────────────────────────────────────────────────

"""
    mc_asian(S, X, T, r, b, v, ::OptionType;
             avg_type=:arithmetic, n_paths=50_000, n_steps=252, seed=42)
    -> NamedTuple

Monte Carlo price for Asian (average price) options.

Simulates GBM paths with `n_steps` timesteps per path.
Payoff based on the arithmetic or geometric average of S along the path.

Uses the geometric Asian closed-form price (`geometric_asian`) as a
control variate when `avg_type = :arithmetic`, which substantially reduces
Monte Carlo variance.

# Returns
`(price::Float64, std_err::Float64)`
"""
function mc_asian(S::Float64, X::Float64, T::Float64,
                  r::Float64, b::Float64, v::Float64,
                  ot::OptionType;
                  avg_type::Symbol=:arithmetic,
                  n_paths::Int=50_000,
                  n_steps::Int=252,
                  seed::Int=42) :: NamedTuple
    rng   = Random.MersenneTwister(seed)
    dt    = T / n_steps
    drift = (b - 0.5 * v^2) * dt
    vvdt  = v * sqrt(dt)
    erT   = exp(-r * T)

    # Control variate: geometric Asian analytic price
    use_cv     = (avg_type == :arithmetic)
    cv_price   = use_cv ? geometric_asian(S, X, T, r, b, v, ot) : 0.0

    raw_payoffs = Vector{Float64}(undef, n_paths)
    cv_payoffs  = Vector{Float64}(undef, n_paths)   # control variate payoffs

    for i in 1:n_paths
        S_cur    = S
        log_sum  = 0.0         # for geometric average
        arith_sum = 0.0        # for arithmetic average
        for _ in 1:n_steps
            z       = Random.randn(rng)
            S_cur  *= exp(drift + vvdt * z)
            arith_sum += S_cur
            log_sum   += log(S_cur)
        end
        arith_avg = arith_sum / n_steps
        geom_avg  = exp(log_sum / n_steps)

        if avg_type == :arithmetic
            raw_payoffs[i] = ot isa Call ? max(arith_avg - X, 0.0) :
                                           max(X - arith_avg, 0.0)
        else
            raw_payoffs[i] = ot isa Call ? max(geom_avg - X, 0.0) :
                                           max(X - geom_avg, 0.0)
        end
        cv_payoffs[i] = ot isa Call ? max(geom_avg - X, 0.0) :
                                      max(X - geom_avg, 0.0)
    end

    if use_cv
        # Optimal control variate coefficient β* = cov(raw, cv) / var(cv)
        cv_mean = Statistics.mean(cv_payoffs)
        raw_mean = Statistics.mean(raw_payoffs)
        cov_rc  = Statistics.mean((raw_payoffs .- raw_mean) .* (cv_payoffs .- cv_mean))
        var_cv  = Statistics.var(cv_payoffs)
        β       = abs(var_cv) > 1e-12 ? cov_rc / var_cv : 0.0
        adjusted = raw_payoffs .- β .* (cv_payoffs .- cv_price / erT)
        mean_pv  = Statistics.mean(adjusted) * erT
        std_pv   = Statistics.std(adjusted)  * erT / sqrt(Float64(n_paths))
    else
        mean_pv  = Statistics.mean(raw_payoffs) * erT
        std_pv   = Statistics.std(raw_payoffs)  * erT / sqrt(Float64(n_paths))
    end

    (price=mean_pv, std_err=std_pv)
end

# ── Barrier Options ────────────────────────────────────────────────────────────

"""
    mc_barrier(S, X, H, T, r, b, v, barrier_type::Symbol, ::OptionType;
               n_paths=50_000, n_steps=252, seed=42) -> NamedTuple

Monte Carlo price for barrier options with discrete monitoring.

`barrier_type` must be one of:
  `:down_and_out`, `:down_and_in`, `:up_and_out`, `:up_and_in`

Applies a Brownian bridge correction for barrier crossing between timesteps
(Beaglehole, Dybvig & Zhou 1997), improving accuracy for coarse time grids.

# Returns
`(price::Float64, std_err::Float64)`
"""
function mc_barrier(S::Float64, X::Float64, H::Float64, T::Float64,
                    r::Float64, b::Float64, v::Float64,
                    barrier_type::Symbol, ot::OptionType;
                    n_paths::Int=50_000,
                    n_steps::Int=252,
                    seed::Int=42) :: NamedTuple
    rng   = Random.MersenneTwister(seed)
    dt    = T / n_steps
    drift = (b - 0.5 * v^2) * dt
    vvdt  = v * sqrt(dt)
    erT   = exp(-r * T)

    is_down = barrier_type in (:down_and_out, :down_and_in)
    is_out  = barrier_type in (:down_and_out, :up_and_out)

    payoffs = Vector{Float64}(undef, n_paths)

    for i in 1:n_paths
        S_cur   = S
        crossed = false

        for _ in 1:n_steps
            S_prev = S_cur
            z      = Random.randn(rng)
            S_cur  = S_prev * exp(drift + vvdt * z)

            # Brownian bridge barrier crossing probability (Beaglehole et al.)
            if is_down
                if S_cur < H || S_prev < H
                    crossed = true; break
                end
                # BB correction: probability of crossing H between S_prev and S_cur
                if S_prev > H && S_cur > H
                    log_ratio = log(S_prev / H) * log(S_cur / H)
                    p_cross   = exp(-2.0 * log_ratio / (v^2 * dt))
                    if Random.rand(rng) < p_cross
                        crossed = true; break
                    end
                end
            else  # up barrier
                if S_cur > H || S_prev > H
                    crossed = true; break
                end
                if S_prev < H && S_cur < H
                    log_ratio = log(H / S_prev) * log(H / S_cur)
                    p_cross   = exp(-2.0 * log_ratio / (v^2 * dt))
                    if Random.rand(rng) < p_cross
                        crossed = true; break
                    end
                end
            end
        end

        alive = is_out ? !crossed : crossed
        if alive
            payoffs[i] = ot isa Call ? max(S_cur - X, 0.0) : max(X - S_cur, 0.0)
        else
            payoffs[i] = 0.0
        end
    end

    mean_pv = Statistics.mean(payoffs) * erT
    std_pv  = Statistics.std(payoffs)  * erT / sqrt(Float64(n_paths))
    (price=mean_pv, std_err=std_pv)
end

# ── Heston Stochastic Volatility (Euler-Maruyama) ─────────────────────────────

"""
    mc_heston(S, X, T, r, q, v0, κ, θ, σ_v, ρ, ::OptionType;
              n_paths=50_000, n_steps=200, seed=42) -> NamedTuple

Euler-Maruyama discretisation of the Heston (1993) stochastic volatility model.

  dS = (r - q)·S·dt + √V·S·dW₁
  dV = κ·(θ - V)·dt + σᵥ·√V·dW₂,   corr(dW₁, dW₂) = ρ·dt

Correlated Brownians:  dW₂ = ρ·dW₁ + √(1-ρ²)·dW₃
Variance floor (Euler truncation): V = max(V, 0)

# Arguments
- `v0::Float64` — initial variance (= σ²)
- `κ::Float64`  — mean-reversion speed
- `θ::Float64`  — long-run variance (= σ_∞²)
- `σ_v::Float64`— volatility of variance
- `ρ::Float64`  — correlation between asset and variance processes

# Returns
`(price::Float64, std_err::Float64)`
"""
function mc_heston(S::Float64, X::Float64, T::Float64,
                   r::Float64, q::Float64,
                   v0::Float64, κ::Float64, θ::Float64,
                   σ_v::Float64, ρ::Float64,
                   ot::OptionType;
                   n_paths::Int=50_000,
                   n_steps::Int=200,
                   seed::Int=42) :: NamedTuple
    rng       = Random.MersenneTwister(seed)
    dt        = T / n_steps
    sqrt_ρ2   = sqrt(max(1.0 - ρ * ρ, 0.0))
    erT       = exp(-r * T)

    payoffs = Vector{Float64}(undef, n_paths)

    for i in 1:n_paths
        S_cur = S
        V_cur = v0

        for _ in 1:n_steps
            z1    = Random.randn(rng)
            z2    = Random.randn(rng)
            dW1   = z1
            dW2   = ρ * z1 + sqrt_ρ2 * z2

            sqrt_V = sqrt(max(V_cur, 0.0))
            S_cur  = S_cur * exp((r - q - 0.5 * max(V_cur, 0.0)) * dt +
                                  sqrt_V * sqrt(dt) * dW1)
            V_cur  = max(V_cur + κ * (θ - V_cur) * dt +
                          σ_v * sqrt_V * sqrt(dt) * dW2, 0.0)
        end

        payoffs[i] = ot isa Call ? max(S_cur - X, 0.0) : max(X - S_cur, 0.0)
    end

    mean_pv = Statistics.mean(payoffs) * erT
    std_pv  = Statistics.std(payoffs)  * erT / sqrt(Float64(n_paths))
    (price=mean_pv, std_err=std_pv)
end

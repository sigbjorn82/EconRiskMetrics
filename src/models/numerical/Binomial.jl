# Binomial.jl — Binomial and trinomial tree option pricing
# Implements CRR, Leisen-Reimer, and trinomial trees from Haug Chapter 3.
# All trees use O(n) rolling-vector memory (not O(n²) full matrix).

# ── Shared Back-propagation Engine ────────────────────────────────────────────

# Internal: backward induction on a terminal payoff vector.
# Modifies `V` in place; returns the time-0 price.
function _tree_backprop!(V::Vector{Float64}, p_u::Float64, p_d::Float64,
                         df::Float64, S_layer::Vector{Float64},
                         X::Float64, ot::OptionType,
                         os::OptionStyle) :: Float64
    n = length(V) - 1
    for step in n:-1:1
        for j in 1:step
            V[j] = df * (p_u * V[j+1] + p_d * V[j])
            # Early exercise check for American options
            if os isa American
                intrinsic = ot isa Call ? max(S_layer[j] - X, 0.0) :
                                          max(X - S_layer[j], 0.0)
                V[j] = max(V[j], intrinsic)
            end
        end
    end
    V[1]
end

# ── Cox-Ross-Rubinstein (1979) ─────────────────────────────────────────────────

"""
    crr_tree(S, X, T, r, b, v, ::OptionType, ::OptionStyle; n=100) -> Float64

Cox-Ross-Rubinstein (1979) binomial tree. (Haug Chapter 3, p.43)

Tree parameters:
  dt = T/n,  u = exp(v√dt),  d = 1/u,  p = (exp(b·dt) - d) / (u - d)

Uses O(n) memory via a rolling single-layer vector.
Supports both European and American options (early exercise via backward induction).

# Example
```julia
# Compare with Black-Scholes
crr_tree(100.0, 100.0, 0.5, 0.10, 0.10, 0.25, Call(), European(); n=500)
# → converges to black_scholes(100.0, 100.0, 0.5, 0.10, 0.10, 0.25, Call())
```
"""
function crr_tree(S::Float64, X::Float64, T::Float64,
                  r::Float64, b::Float64, v::Float64,
                  ot::OptionType, os::OptionStyle;
                  n::Int=100) :: Float64
    dt  = T / n
    u   = exp(v * sqrt(dt))
    d   = 1.0 / u
    p_u = (exp(b * dt) - d) / (u - d)
    p_d = 1.0 - p_u
    df  = exp(-r * dt)      # single-step discount factor

    # Terminal asset prices (ascending): S·d^n, S·d^(n-2)·u^2, ...
    # S_j = S · u^j · d^(n-j)  for j = 0..n
    S_terminal = Vector{Float64}(undef, n + 1)
    for j in 0:n
        S_terminal[j+1] = S * u^j * d^(n - j)
    end

    # Terminal payoffs
    V = if ot isa Call
        max.(S_terminal .- X, 0.0)
    else
        max.(X .- S_terminal, 0.0)
    end

    # For American back-propagation we need asset prices at each layer.
    # We rebuild S_layer at each step using: S_layer_j = S_terminal[j+1] / (u or d).
    # Simpler: track current layer S prices separately.
    # At step k from the end, there are k+1 nodes with S_j = S·u^j·d^(k-j)
    S_layer = Vector{Float64}(undef, n + 1)
    copyto!(S_layer, S_terminal)

    # Backward induction
    for step in n:-1:1
        # Update S_layer for the parent nodes (divide out one d factor from each node)
        for j in 1:step
            S_layer[j] = S_layer[j] / d     # = S · u^(j-1) · d^(step-j)
        end
        for j in 1:step
            V[j] = df * (p_u * V[j+1] + p_d * V[j])
            if os isa American
                intrinsic = ot isa Call ? max(S_layer[j] - X, 0.0) :
                                          max(X - S_layer[j], 0.0)
                V[j] = max(V[j], intrinsic)
            end
        end
    end
    V[1]
end

# ── Leisen-Reimer (1996) ───────────────────────────────────────────────────────

# Peizer-Pratt inversion function h(z, n) — Haug p.52
function _pp2_inversion(z::Float64, n::Int) :: Float64
    n_f = Float64(n)
    exponent = -(z / (n_f + 1.0/3.0 + 0.1/(n_f + 1.0)))^2 * (n_f + 1.0/6.0)
    0.5 + sign(z) * sqrt(max(0.25 - 0.25 * exp(exponent), 0.0))
end

"""
    lr_tree(S, X, T, r, b, v, ::OptionType, ::OptionStyle; n=101) -> Float64

Leisen-Reimer (1996) binomial tree. (Haug Chapter 3, p.51)

Uses the Peizer-Pratt (1983) inversion h(z, n) to set risk-neutral probabilities
directly from d₁ and d₂, dramatically reducing the odd-even oscillation of CRR.

`n` should be odd for best accuracy (automatically incremented by 1 if even).

# Example
```julia
lr_tree(100.0, 100.0, 0.5, 0.10, 0.10, 0.25, Call(), European(); n=101)
# → very close to Black-Scholes with only 101 steps
```
"""
function lr_tree(S::Float64, X::Float64, T::Float64,
                 r::Float64, b::Float64, v::Float64,
                 ot::OptionType, os::OptionStyle;
                 n::Int=101) :: Float64
    n = iseven(n) ? n + 1 : n   # ensure odd for LR accuracy
    dt  = T / n
    d1v = _d1(S, X, T, b, v)
    d2v = d1v - v * sqrt(T)
    p_u = _pp2_inversion(d1v, n)    # probability hitting up from d1 perspective
    p_d = _pp2_inversion(d2v, n)    # probability hitting up from d2 perspective
    df  = exp(-r * dt)

    # Implied u and d from LR probabilities
    # u uses the ratio p_u/p_d; d is solved from the martingale condition with p_d
    u   = exp(b * dt) * p_u / p_d
    d   = (exp(b * dt) - p_d * u) / (1.0 - p_d)

    # Terminal asset prices
    S_terminal = Vector{Float64}(undef, n + 1)
    for j in 0:n
        S_terminal[j+1] = S * u^j * d^(n - j)
    end

    # Terminal payoffs
    V = if ot isa Call
        max.(S_terminal .- X, 0.0)
    else
        max.(X .- S_terminal, 0.0)
    end

    S_layer = Vector{Float64}(undef, n + 1)
    copyto!(S_layer, S_terminal)

    for step in n:-1:1
        for j in 1:step
            S_layer[j] = S_layer[j] / d
        end
        for j in 1:step
            V[j] = df * (p_d * V[j+1] + (1.0 - p_d) * V[j])
            if os isa American
                intrinsic = ot isa Call ? max(S_layer[j] - X, 0.0) :
                                          max(X - S_layer[j], 0.0)
                V[j] = max(V[j], intrinsic)
            end
        end
    end
    V[1]
end

# ── Trinomial Tree (Boyle 1986) ────────────────────────────────────────────────

"""
    trinomial_tree(S, X, T, r, b, v, ::OptionType, ::OptionStyle;
                   n=100, λ=1.2) -> Float64

Trinomial tree (Boyle 1986). (Haug Chapter 3, p.58)

Three possible moves per step: up (u), middle (m=1), down (d=1/u).

  dt = T/n,  u = exp(λ·v·√dt),  d = 1/u
  p_u = ((exp(b·dt/2) - exp(-v·√dt/2)) / (exp(v·√dt/2) - exp(-v·√dt/2)))²  [simplified]

The stretch parameter λ ∈ [1, 1.5] controls the size of up/down moves.
λ = 1.2 is recommended by Haug as a good default.

Uses O(2n+1) memory via a rolling single-layer vector.

# Example
```julia
trinomial_tree(100.0, 100.0, 0.5, 0.10, 0.10, 0.25, Call(), American(); n=100)
```
"""
function trinomial_tree(S::Float64, X::Float64, T::Float64,
                        r::Float64, b::Float64, v::Float64,
                        ot::OptionType, os::OptionStyle;
                        n::Int=100, λ::Float64=1.2) :: Float64
    dt  = T / n
    u   = exp(λ * v * sqrt(dt))
    # Risk-neutral probabilities via moment matching:
    # E[S'/S] = exp(b·dt),  E[(S'/S)²] = exp((2b+v²)·dt),  pu+pm+pd=1
    # Solved analytically:
    #   pd = u² · (e2 - e1·(u+1)) / ((u-1)²·(u+1))
    #   pu = e1/(u-1) + pd/u
    #   pm = 1 - pu - pd
    e1     = exp(b * dt) - 1.0
    e2     = exp((2.0 * b + v^2) * dt) - 1.0
    p_dn   = u^2 * (e2 - e1 * (u + 1.0)) / ((u - 1.0)^2 * (u + 1.0))
    p_up   = e1 / (u - 1.0) + p_dn / u
    p_mid  = 1.0 - p_up - p_dn
    # Clamp to [0,1] and renormalise (robustness for extreme parameters)
    p_up   = max(p_up,  0.0)
    p_dn   = max(p_dn,  0.0)
    p_mid  = max(p_mid, 0.0)
    total  = p_up + p_mid + p_dn
    p_up  /= total; p_dn /= total; p_mid /= total

    df = exp(-r * dt)

    # Node count at maturity: 2n+1 nodes (j = -n..n)
    n_nodes = 2 * n + 1

    # Terminal asset prices: S_j = S · u^j for j = -n..n (index 1 = j=-n)
    S_layer = Vector{Float64}(undef, n_nodes)
    V       = Vector{Float64}(undef, n_nodes)
    for k in 1:n_nodes
        j         = k - (n + 1)     # j ranges from -n to n
        S_layer[k] = S * u^j
        payoff     = ot isa Call ? max(S_layer[k] - X, 0.0) :
                                   max(X - S_layer[k], 0.0)
        V[k]       = payoff
    end

    # Backward induction: at each step, the parent layer has 2 fewer nodes than the child layer.
    # At loop variable `step`, child layer has 2*step+1 nodes; parent layer has 2*step-1 nodes.
    S_prev = Vector{Float64}(undef, n_nodes)
    for step in n:-1:1
        n_active = 2 * step - 1   # parent node count at level step-1 from origin
        # Parent S prices: j ranges from -(step-1) to step-1
        for k in 1:n_active
            j = k - step           # k=1 → j=-(step-1), k=step → j=0, k=2*step-1 → j=step-1
            S_prev[k] = S * u^j
        end
        for k in 1:n_active
            V_up  = V[k + 2]    # child going up   (j+1 → index k+2 in current)
            V_mid = V[k + 1]    # child staying    (j   → index k+1)
            V_dn  = V[k]        # child going down (j-1 → index k)
            val   = df * (p_up * V_up + p_mid * V_mid + p_dn * V_dn)
            if os isa American
                intrinsic = ot isa Call ? max(S_prev[k] - X, 0.0) :
                                          max(X - S_prev[k], 0.0)
                val = max(val, intrinsic)
            end
            V[k] = val
        end
        # Shift the parent layer into V for next iteration
        # (already stored in V[1..n_active])
    end
    V[1]
end

#!/usr/bin/env julia
# test_options.jl — Validation tests for the Options pricing module
# All benchmark values are taken from Haug "Complete Guide to Option Pricing Formulas"

using Pkg
Pkg.activate(joinpath(@__DIR__, ".."))

using EconRiskMetrics

println("=" ^ 65)
println("  Option Pricing Module — Validation Tests (Haug benchmarks)")
println("=" ^ 65)

pass = 0
fail = 0

function check(label::String, got::Float64, expected::Float64; tol::Float64=0.01)
    global pass, fail
    ok = abs(got - expected) <= tol
    status = ok ? "✓" : "✗"
    if ok
        pass += 1
        println("  $status  $label")
        println("       got=$(round(got, digits=5))  expected=$(round(expected, digits=5))")
    else
        fail += 1
        println("  $status  $label  FAIL")
        println("       got=$(round(got, digits=5))  expected=$(round(expected, digits=5))  diff=$(round(abs(got-expected), digits=6))")
    end
end

# ── 1. Black-Scholes Core Prices (Haug p.8-9) ─────────────────────────────────
println("\n── 1. Black-Scholes (Haug p.8-9) ──────────────────────────────")

check("BS call:  S=60,X=65,T=0.25,r=0.08,b=0.08,v=0.30",
    black_scholes(60.0, 65.0, 0.25, 0.08, 0.08, 0.30, Call()), 2.1334; tol=0.001)

check("BS put:   S=60,X=65,T=0.25,r=0.08,b=0.08,v=0.30",
    black_scholes(60.0, 65.0, 0.25, 0.08, 0.08, 0.30, Put()), 5.8460; tol=0.001)

check("BS call:  S=100,X=95,T=0.5,r=0.10,b=0.10,v=0.20",
    black_scholes(100.0, 95.0, 0.5, 0.10, 0.10, 0.20, Call()), 11.499; tol=0.005)

# Black-76 futures option (b=0) — Haug p.10
check("Black-76 call: F=19,X=19,T=0.75,r=0.10,b=0,v=0.28",
    black_scholes(19.0, 19.0, 0.75, 0.10, 0.0, 0.28, Call()), 1.7011; tol=0.001)

# Merton continuous dividend (b = r - q) — Haug p.9
check("Merton call: S=100,X=95,T=0.5,r=0.10,q=0.05,v=0.20",
    black_scholes(100.0, 95.0, 0.5, 0.10, 0.05, 0.20, Call()), 9.629; tol=0.01)

# ── 2. Put-Call Parity ─────────────────────────────────────────────────────────
println("\n── 2. Put-Call Parity ──────────────────────────────────────────")

let S=100.0, X=100.0, T=1.0, r=0.05, b=0.05, v=0.25
    c = black_scholes(S, X, T, r, b, v, Call())
    p = black_scholes(S, X, T, r, b, v, Put())
    parity = c - p - (S * exp((b-r)*T) - X * exp(-r*T))
    check("Put-call parity holds (diff ≈ 0)", parity, 0.0; tol=1e-8)
end

# ── 3. Greeks (Haug p.26-30) ──────────────────────────────────────────────────
println("\n── 3. Black-Scholes Greeks (Haug p.26-30) ──────────────────────")

let S=105.0, X=100.0, T=0.5, r=0.10, b=0.10, v=0.36
    g = bs_greeks(S, X, T, r, b, v, Call())
    check("Delta call: S=105,X=100,T=0.5,r=0.10,b=0.10,v=0.36",
        g.delta, 0.6968; tol=0.001)
    check("Gamma: same params",
        g.gamma, 0.0131; tol=0.001)
    check("Vega (per 1.0 vol): same params",
        g.vega, 25.94; tol=0.10)
end

let S=75.0, X=70.0, T=0.5, r=0.10, b=0.05, v=0.35
    g = bs_greeks(S, X, T, r, b, v, Put())
    check("Delta put: S=75,X=70,T=0.5,r=0.10,b=0.05,v=0.35",
        g.delta, -0.2997; tol=0.001)
end

# ── 4. Implied Volatility Round-trip ─────────────────────────────────────────
println("\n── 4. Implied Volatility Round-trip ────────────────────────────")

for (S,X,T,r,b,v) in [(100.0,100.0,1.0,0.05,0.05,0.25),
                       (80.0, 100.0,0.5,0.10,0.10,0.30),
                       (120.0, 100.0,0.25,0.08,0.08,0.20)]
    c = black_scholes(S, X, T, r, b, v, Call())
    iv = implied_vol(c, S, X, T, r, b, Call())
    check("IV round-trip call: S=$S,X=$X,v=$v", iv, v; tol=1e-6)
    p = black_scholes(S, X, T, r, b, v, Put())
    iv_p = implied_vol(p, S, X, T, r, b, Put())
    check("IV round-trip put:  S=$S,X=$X,v=$v", iv_p, v; tol=1e-6)
end

# ── 5. American Options (Haug p.68-69, p.76) ─────────────────────────────────
println("\n── 5. American Option Approximations ───────────────────────────")

# BAW (Haug p.68, Table 3-1)
check("BAW call: S=100,X=100,T=0.5,r=0.10,b=0,v=0.25",
    baw_american(100.0, 100.0, 0.5, 0.10, 0.0, 0.25, Call()), 6.7611; tol=0.05)

# BS2002 (Haug p.76)
check("BS2002 call: S=100,X=100,T=0.5,r=0.10,b=0,v=0.25",
    bjerksund_stensland(100.0, 100.0, 0.5, 0.10, 0.0, 0.25, Call()), 6.7627; tol=0.05)

# American put > European put (early exercise premium)
let S=100.0, X=105.0, T=1.0, r=0.08, b=0.0, v=0.25
    eur = black_scholes(S, X, T, r, b, v, Put())
    am  = bjerksund_stensland(S, X, T, r, b, v, Put())
    check("American put ≥ European put (early exercise premium > 0)",
        Float64(am >= eur - 1e-8), 1.0; tol=0.0)
end

# ── 6. Binomial Trees vs Black-Scholes ───────────────────────────────────────
println("\n── 6. Binomial Trees convergence to Black-Scholes ──────────────")

let S=100.0, X=100.0, T=0.5, r=0.10, b=0.10, v=0.25
    bs_ref = black_scholes(S, X, T, r, b, v, Call())
    crr500 = crr_tree(S, X, T, r, b, v, Call(), European(); n=500)
    check("CRR n=500 European call vs BS (tol=0.05)",
        crr500, bs_ref; tol=0.05)

    lr101 = lr_tree(S, X, T, r, b, v, Call(), European(); n=101)
    check("LR  n=101 European call vs BS (tol=0.02)",
        lr101, bs_ref; tol=0.02)

    tri200 = trinomial_tree(S, X, T, r, b, v, Call(), European(); n=200)
    check("Trinomial n=200 European call vs BS (tol=0.05)",
        tri200, bs_ref; tol=0.05)
end

# ── 7. Monte Carlo vs Black-Scholes ──────────────────────────────────────────
println("\n── 7. Monte Carlo convergence to Black-Scholes ─────────────────")

let S=100.0, X=100.0, T=0.5, r=0.10, b=0.10, v=0.25
    bs_ref = black_scholes(S, X, T, r, b, v, Call())
    mc = mc_european(S, X, T, r, b, v, Call(); n_paths=200_000, seed=42)
    check("MC European call n=200k vs BS (tol=0.15)",
        mc.price, bs_ref; tol=0.15)

    mc_p = mc_european(S, X, T, r, b, v, Put(); n_paths=200_000, seed=42)
    bs_put = black_scholes(S, X, T, r, b, v, Put())
    check("MC European put  n=200k vs BS (tol=0.15)",
        mc_p.price, bs_put; tol=0.15)
end

# ── 8. Barrier Options (Haug p.100-101, Table 4-1) ───────────────────────────
println("\n── 8. Barrier Options (Haug p.100-101) ─────────────────────────")
# S=100, X=90, H=95, K=3, T=0.5, r=0.10, b=0.05, v=0.25
let S=100.0, X=90.0, H=95.0, K=3.0, T=0.5, r=0.10, b=0.05, v=0.25
    check("Down-and-in call  (η=1, φ=1, knock_out=false)",
        barrier_option(S, X, H, K, T, r, b, v,  1,  1; knock_out=false), 7.8413; tol=0.10)
    check("Down-and-out call (η=1, φ=1, knock_out=true)",
        barrier_option(S, X, H, K, T, r, b, v,  1,  1; knock_out=true),  9.1813; tol=0.10)
end

# ── 9. Digital / Binary Options (Haug p.121) ─────────────────────────────────
println("\n── 9. Digital Options (Haug p.121) ─────────────────────────────")

check("Cash-or-nothing call: S=100,X=80,T=0.75,r=0.06,b=0,v=0.35,K=10",
    cash_or_nothing(100.0, 80.0, 0.75, 0.06, 0.0, 0.35, 10.0, Call()), 5.6733; tol=0.01)

check("Asset-or-nothing call: S=70,X=65,T=0.5,r=0.07,b=0.07,v=0.27",
    asset_or_nothing(70.0, 65.0, 0.5, 0.07, 0.07, 0.27, Call()), 41.18; tol=0.10)

# ── 10. Asian Options ────────────────────────────────────────────────────────
println("\n── 10. Asian Options ───────────────────────────────────────────")

check("Geometric Asian call: S=100,X=100,T=1,r=0.10,b=0.05,v=0.20",
    geometric_asian(100.0, 100.0, 1.0, 0.10, 0.05, 0.20, Call()), 5.40; tol=0.10)

# ── 11. Exchange Option (Margrabe) ───────────────────────────────────────────
println("\n── 11. Exchange Option (Margrabe 1978) ─────────────────────────")

check("Exchange option: S1=22,S2=20,T=0.5,r=0.10,b1=b2=0,v1=0.20,v2=0.15,ρ=0.5",
    exchange_option(22.0, 20.0, 0.5, 0.10, 0.0, 0.0, 0.20, 0.15, 0.50), 2.19; tol=0.05)

# ── 12. Chooser Option (Haug p.141) ─────────────────────────────────────────
println("\n── 12. Chooser Option ──────────────────────────────────────────")

check("Simple chooser: S=50,X=50,T1=0.25,T2=0.5,r=0.08,b=0.08,v=0.25",
    chooser_option(50.0, 50.0, 0.25, 0.5, 0.08, 0.08, 0.25), 6.1071; tol=0.05)

# ── 13. Lookback Options (Haug p.132-140) ────────────────────────────────────
println("\n── 13. Lookback Options ────────────────────────────────────────")

# Floating-strike lookback call (at inception, S_min = S)
let S=120.0, T=0.5, r=0.10, b=0.10, v=0.30
    val = lookback_floating(S, S, S, T, r, b, v, Call())
    # Should be > European call ATM price
    bs_atm = black_scholes(S, S, T, r, b, v, Call())
    check("Floating lookback call ≥ ATM call (S_min=S at inception)",
        Float64(val >= bs_atm - 1e-8), 1.0; tol=0.0)
end

# ── 14. Forward Start Option ─────────────────────────────────────────────────
println("\n── 14. Forward Start Option (Haug p.152) ───────────────────────")

check("Forward start call: S=60,α=1.1,T1=0.25,T2=1.0,r=0.08,b=0.08,v=0.30",
    forward_start(60.0, 1.1, 0.25, 1.0, 0.08, 0.08, 0.30, Call()), 4.4037; tol=0.05)

# ── 15. Heston Model ─────────────────────────────────────────────────────────
println("\n── 15. Heston Stochastic Volatility ────────────────────────────")

# Benchmark from Lewis (2000): S=X=100, T=0.5, r=0.05, q=0.02,
#   v0=0.04 (=20% vol), κ=2, θ=0.04, σᵥ=0.5, ρ=-0.7
check("Heston call (Lewis 2000 benchmark)",
    heston_price(100.0, 100.0, 0.5, 0.05, 0.02, 0.04, 2.0, 0.04, 0.5, -0.7, Call()),
    5.79; tol=0.30)

# Heston should agree with BS when σᵥ→0 (deterministic vol = √v0)
let S=100.0, X=100.0, T=1.0, r=0.05, q=0.02, v0=0.04
    bs_ref = black_scholes(S, X, T, r, r-q, sqrt(v0), Call())
    heston_lim = heston_price(S, X, T, r, q, v0, 5.0, v0, 0.001, 0.0, Call())
    check("Heston → BS when σᵥ≈0, κ large, θ=v0",
        heston_lim, bs_ref; tol=0.10)
end

# ── 16. OptionContract convenience API ──────────────────────────────────────
println("\n── 16. OptionContract convenience dispatch ─────────────────────")

let c = OptionContract(100.0, 100.0, 1.0, 0.05, 0.05, 0.20, European(), Call())
    p_bs  = price(c, :bs)
    p_crr = price(c, :crr; n=200)
    p_mc  = price(c, :mc; n_paths=50_000)
    check("price(:bs) matches black_scholes directly",
        p_bs, black_scholes(100.0, 100.0, 1.0, 0.05, 0.05, 0.20, Call()); tol=1e-10)
    check("price(:crr, n=200) within 0.05 of BS",
        p_crr, p_bs; tol=0.05)
    check("price(:mc, n=50k) within 0.20 of BS",
        p_mc, p_bs; tol=0.20)
end

# ── Summary ───────────────────────────────────────────────────────────────────
total = pass + fail
println()
println("=" ^ 65)
println("  Results: $pass / $total passed" * (fail > 0 ? "  ($fail failed)" : "  — all green!"))
println("=" ^ 65)

fail > 0 && exit(1)

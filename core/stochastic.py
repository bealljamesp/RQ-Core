import numpy as np
import pandas as pd


def generate_gbm(
    s0: float, mu: float, sigma: float, dt: float, steps: int
) -> pd.Series:
    """
    Generates a single GBM path using the exact solution to the SDE.
    S_t = S_0 * exp((mu - 0.5 * sigma**2) * t + sigma * W_t)
    """
    # 1. Generate the Wiener Process (Standard Normal Shocks)
    # Scaled by sqrt(dt) because Variance is linear with time
    shocks = np.random.standard_normal(steps)

    # 2. Calculate components
    drift = (mu - 0.5 * sigma**2) * dt
    diffusion = sigma * np.sqrt(dt) * shocks

    # 3. Vectorized Path Calculation
    # np.cumsum handles the aggregation of log-returns over time
    path_returns = np.exp(np.cumsum(drift + diffusion))

    # 4. Prepend the starting price S0
    full_path = np.insert(path_returns, 0, 1.0) * s0

    return pd.Series(full_path)

import numpy as np


def calculate_tail_metrics(returns, confidence=0.95):
    """
    Axiomatic calculation of VaR and Expected Shortfall.
    """
    # Convert confidence (0.95) to percentile rank (5)
    alpha = (1 - confidence) * 100

    # Calculate VaR (The Quantile)
    var = np.percentile(returns, alpha)

    # Calculate Expected Shortfall (The Conditional Mean)
    tail_events = returns[returns <= var]
    es = tail_events.mean()

    return {
        "confidence": confidence,
        "var": var,
        "es": es,
        "severity_ratio": es / var if var != 0 else 0,
    }

"""HTTP API endpoint for variance detection."""

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse

from src.detect.detector import detect_variances, print_results

app = FastAPI(title="Variance Detection API")


@app.get("/detect")
def detect_endpoint(time_key: str = None):
    """
    Trigger variance detection and return results.

    Args:
        time_key: Optional time key to filter data (e.g., "202501")

    Returns:
        JSON response with detection results
    """
    try:
        # Settings is optional, pass None
        results = detect_variances(None, time_key)

        # Print results to console
        print_results(results)

        # Convert results to JSON-serializable format
        results_json = [
            {
                "account": r.account,
                "description": r.description,
                "actual_value": r.actual_value,
                "comparison_value": r.comparison_value,
                "variance": r.variance,
                "variance_percent": r.variance_percent,
                "is_alert": r.is_alert,
                "threshold_percent": r.threshold_percent,
                "detected_at": r.detected_at.isoformat(),
            }
            for r in results
        ]

        return JSONResponse(
            status_code=200,
            content={
                "status": "success",
                "total_metrics": len(results),
                "alerts_triggered": sum(1 for r in results if r.is_alert),
                "threshold_percent": 5.0,
                "results": results_json,
            },
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Detection failed: {str(e)}")


@app.get("/health")
def health_check():
    """Health check endpoint."""
    return {"status": "healthy"}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)

from fastapi import FastAPI

app = FastAPI(title="Delivery Service")


@app.get("/health/live")
async def health() -> dict[str, str]:
    return {"status": "ok"}

from fastapi import FastAPI

app = FastAPI()

@app.get("/")
def root():
    return {"message": "Hello from FastAPI + Docker!"}

@app.get("/api/v1/health")
def apiHealth():
    return {"status": 200}

@app.get("/_cluster/v1/health")
def clusterHealth():
    return {"status": 200}

@app.get("/api/status")
def statusHealth():
    return {"status": 200}

@app.get("localhost:8080/health")
def localhostHealth():
    return {"status": 200}

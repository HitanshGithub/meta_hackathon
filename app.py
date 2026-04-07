import sys
import os

# Add the current directory so server module can be found
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from server.app import app
import uvicorn

if __name__ == "__main__":
    uvicorn.run("app:app", host="0.0.0.0", port=7860, reload=False)

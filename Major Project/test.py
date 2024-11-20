from fastapi import FastAPI, UploadFile, Form, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import requests
from typing import List

app = FastAPI()

SLAVE_SERVICE_URL = "https://transcode.pvehome.me/api/v1/slave/chunk"  #scheduler URL

class UploadChunkDetails(BaseModel):
    chunkId: str
    chunkIndex: int
    podNames: List[str]


@app.post("/upload/chunk/")
async def upload_chunk(
    file: UploadFile,
    chunkId: str = Form(...),
    chunkIndex: int = Form(...),
    podNames: List[str] = Form(...),
):
    """
    Upload a file chunk to multiple replicas.

    Args:
        file (UploadFile): The chunk file to upload.
        chunkId (str): Unique identifier for the chunk.
        chunkIndex (int): Index of the chunk.
        podNames (List[str]): List of pod names to upload the chunk.

    Returns:
        JSONResponse: Upload success or failure for each pod.
    """
    headers = {"Content-Type": "multipart/form-data"}
    params = f"{chunkId}/{chunkIndex}/"
    results = []

    # Read the file content as bytes
    file_content = await file.read()

    for i, pod_name in enumerate(podNames):
        # Construct the upload URL for each pod
        url = f"http://{pod_name}.{SLAVE_SERVICE_URL}/api/v1/slave/chunk/upload/{params}{i}"
        print(url)
        try:
            # Send the POST request to the pod
            response = requests.post(
                url,
                files={"file": (file.filename, file_content, file.content_type)},
                headers=headers,
            )

            if response.status_code == 200:
                results.append(
                    {
                        "replica": i + 1,
                        "pod": pod_name,
                        "status": "success",
                        "message": response.text,
                    }
                )
            else:
                results.append(
                    {
                        "replica": i + 1,
                        "pod": pod_name,
                        "status": "failure",
                        "message": response.text,
                    }
                )
        except requests.RequestException as e:
            results.append(
                {
                    "replica": i + 1,
                    "pod": pod_name,
                    "status": "error",
                    "message": str(e),
                }
            )

    return JSONResponse(content={"results": results})


# Example request format
"""
POST /upload/chunk/
Form Data:
- file (UploadFile): The chunk file to upload.
- chunkId (str): The unique chunk ID.
- chunkIndex (int): The index of the chunk.
- podNames (List[str]): Comma-separated list of pod names.

Example JSON Response:
{
  "results": [
    {"replica": 1, "pod": "pod1", "status": "success", "message": "Uploaded successfully"},
    {"replica": 2, "pod": "pod2", "status": "failure", "message": "Failed to upload"},
    {"replica": 3, "pod": "pod3", "status": "error", "message": "Connection error"}
  ]
}
"""
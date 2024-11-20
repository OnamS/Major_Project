import os
import random
import string
from fastapi import FastAPI, UploadFile, Form, HTTPException, Depends
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder
import requests
from pydantic import BaseModel
from datetime import datetime
from typing import List

# Load environment variables from a .env file
from dotenv import load_dotenv
load_dotenv()

# Initialize FastAPI app
app = FastAPI()

# Constants
MASTER_NODE_URL = "https://transcode.pvehome.me/api/v1"
SLAVE_SERVICE_URL = "https://transcode.pvehome.me/api/slave/chunk"

# Database (using SQLAlchemy or other ORM, for simplicity we'll omit DB setup here)

# Define Pydantic models for request bodies
class FileInitDto(BaseModel):
    title: str
    fileSize: int
    desc: str
    thumbnailLink: str

class UploadChunkDto(BaseModel):
    chunkId: str
    chunkIndex: int
    podNames: List[str]

# Utility function for generating random alphanumeric strings
def generate_internal_file_id(length=32):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

# Simulate database operation
video_files = {}  # A simple in-memory "database"

@app.post("/api/v1/file/createFile")
async def init_file_upload(video_details: FileInitDto, user_id: str):
    # Generate internal file ID and populate VideoFile object
    internal_file_id = generate_internal_file_id()
    video_file = {
        "internalFileId": internal_file_id,
        "fileSizeBytes": video_details.fileSize,
        "uploadedOn": datetime.now(),
        "title": video_details.title,
        "ownerId": user_id,
        "desc": video_details.desc,
        "thumbnailLink": video_details.thumbnailLink
    }

    # Prepare request body for master node
    request_body = {
        "fileName": video_details.title,
        "fileId": internal_file_id,
        "fileSizeBytes": video_details.fileSize
    }
    
    headers = {"Content-Type": "application/json"}

    # Send request to the master node to initiate file creation
    try:
        response = requests.post(f"{MASTER_NODE_URL}/api/v1/file/createFile", json=request_body, headers=headers)

        if response.status_code == 200:
            # Simulate saving to the database
            video_files[internal_file_id] = video_file
            return JSONResponse(content=response.json(), status_code=200)
        else:
            return HTTPException(status_code=response.status_code, detail=response.text)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error creating file: {str(e)}")


@app.post("/api/v1/slave/chunk/upload/{chunk_id}/{chunk_index}/{replica_id}")
async def upload_chunk(chunk_id: str, chunk_index: int, replica_id: int, file: UploadFile, upload_details: UploadChunkDto):
    headers = {"Content-Type": "multipart/form-data"}
    
    # Prepare request for each replica pod (slave service)
    file_content = await file.read()

    # Assuming SLAVE_SERVICE_URL is in a format like 'slave-service-name'
    for i, pod_name in enumerate(upload_details.podNames):
        url = f"http://{pod_name}.{SLAVE_SERVICE_URL}/api/v1/slave/chunk/upload/{chunk_id}/{chunk_index}/{replica_id}"

        files = {'file': (file.filename, file_content, file.content_type)}
        
        try:
            response = requests.post(url, files=files, headers=headers)
            if response.status_code == 200:
                # Successful upload
                return {"status": "success", "message": f"Replica #{i + 1} uploaded successfully"}
            else:
                return {"status": "failed", "message": f"Replica #{i + 1} upload failed: {response.text}"}
        except requests.RequestException as e:
            return {"status": "failed", "message": f"Error uploading to replica #{i + 1}: {str(e)}"}

    raise HTTPException(status_code=500, detail="Error uploading to all replicas")


# FastAPI provides automatic documentation at `/docs` and `/redoc`

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





# import requests
# import asyncio
# import aiohttp
# import math

# # Constants
# BASE_URL = "https://your-base-url.com"  # Replace with your base URL
# URI = f"{BASE_URL}/api/v1/upload"  # Replace with the actual endpoint URI

# # Function to upload file chunks
# async def upload_file_chunks(video, video_name, video_size, description, thumbnail_url, token):
#     """
#     Uploads a file in chunks asynchronously.
    
#     Args:
#         video (bytes): The video file content as bytes.
#         video_name (str): Name of the video.
#         video_size (int): Size of the video in bytes.
#         description (str): Video description.
#         thumbnail_url (str): Thumbnail URL for the video.
#         token (str): Authentication token.
#     """
#     # Step 1: Initialize the upload
#     form_data = {
#         "title": video_name,
#         "fileSize": video_size,
#         "desc": description,
#         "thumbnailLink": thumbnail_url,
#     }

#     headers = {
#         "Authorization": f"Bearer {token}",
#         "Content-Type": "application/json",
#     }

#     try:
#         response = requests.post(URI, json=form_data, headers=headers)
#         if response.status_code == 200:
#             chunk_name = response.json()  # Response contains chunk names
#             print("Initialization response:", chunk_name)

#             chunk_size = 64 * 1000 * 1000  # 64 MB
#             chunk_count = math.ceil(video_size / chunk_size)
#             chunk_promises = []

#             # Step 2: Upload each chunk
#             async with aiohttp.ClientSession() as session:
#                 for i in range(chunk_count):
#                     start = i * chunk_size
#                     end = min(video_size, start + chunk_size)
#                     chunk = video[start:end]  # Slice the video file

#                     chunk_upload_url = (
#                         f"{BASE_URL}/api/v1/upload/uploadChunk/"
#                         f"{chunk_name[i][0]}/{i}/" + ",".join(chunk_name[i][1:])
#                     )

#                     form_data = aiohttp.FormData()
#                     form_data.add_field("file", chunk)

#                     # Prepare the upload task
#                     chunk_promises.append(
#                         session.post(
#                             chunk_upload_url,
#                             data=form_data,
#                             headers={
#                                 "Authorization": f"Bearer {token}",
#                                 "Content-Type": "multipart/form-data",
#                             },
#                         )
#                     )

#                 # Wait for all uploads to complete
#                 responses = await asyncio.gather(*chunk_promises, return_exceptions=True)
#                 for response in responses:
#                     if isinstance(response, Exception):
#                         print("Error uploading chunk:", response)
#                     elif response.status == 200:
#                         print("Chunk uploaded successfully:", await response.text())
#                     else:
#                         print("Chunk upload failed:", response.status, await response.text())

#         else:
#             print("Failed to initialize upload:", response.status_code, response.text)
#     except Exception as e:
#         print("Unexpected error:", str(e))


# # Example Usage
# if __name__ == "__main__":
#     # Mock data
#     video = b"mock video data" * 64 * 1000 * 1000 * 3  # Mock video bytes (192 MB)
#     video_name = "example_video"
#     video_size = len(video)
#     description = "This is a sample video."
#     thumbnail_url = "https://example.com/thumbnail.jpg"
#     token = "your-auth-token"

#     # Run the upload
#     asyncio.run(upload_file_chunks(video, video_name, video_size, description, thumbnail_url, token))

from fastapi import FastAPI, BackgroundTasks,status
from fastapi_utilities import repeat_every
import requests
import time
import subprocess
from datetime import datetime
import random
import string
import logging



app = FastAPI()

# Worker Pod configuration
scheduler_url = "https://transcode.pvehome.me/api/v1/worker/ping"
pod_name = "worker-pod-1"
is_assigned_task = False
assigned_task_id = ""


from fastapi import FastAPI, Request
from pydantic import BaseModel

app = FastAPI()

# Define a model for the expected payload
class SchedulerJob(BaseModel):
    AssignedTaskID: str
    VideoInternalFileID: str
    startTime: str  
    endTime: str 

@app.post("/job")
async def receive_job(job_data: SchedulerJob):
    """
    Endpoint to receive a ping from the scheduler.
    """
    # Log the received ping
    print(job_data)
    is_assigned_task = True
    startTime = datetime.strptime(job_data.startTime, '%H:%M:%S').time()
    # endTime = datetime.strptime(endTime, '%H:%M:%S').time()
    # Process the ping (add your custom logic here)
    run_command_and_store_output("output_file",startTime,job_data.endTime, job_data.VideoInternalFileID)
    return status.HTTP_200_OK




@app.on_event("startup")
@repeat_every(seconds=10)
async def send_heartbeat():

    """
    Sends a heartbeat to the scheduler every 10 seconds.
    """
    try:
        # Create the WorkerPodHeartBeat payload
        payload = {
                "podName": pod_name,
                "isAssignedTask": is_assigned_task,
                "assignedTaskId": assigned_task_id,
            }
        print(payload)

            # Send POST request to the scheduler
        
        response = requests.post(scheduler_url, json=payload)
        print(response)
            # if response.status_code == 200:
            #     print(f"Heartbeat sent successfully: {response.json()}")
            # else:
            #     print(f"Failed to send heartbeat: {response.status_code}, {response.text}")
    except Exception as e:
        print(f"Error sending heartbeat: {e}")
        

@app.on_event("startup")
def start_heartbeat_task():
    """
    Starts the heartbeat task when the FastAPI application starts.
    """
    import threading
    heartbeat_thread = threading.Thread(target=send_heartbeat, daemon=True)
    heartbeat_thread.start()

@app.get("/")
def root():
    """
    Root endpoint to verify the worker is running.
    """
    return {"message": "Worker Pod is running"}

@app.post("/assign-task")
def assign_task(task_id: str):
    """
    Assigns a task to the worker pod.
    """
    global is_assigned_task, assigned_task_id
    is_assigned_task = True
    assigned_task_id = task_id
    return {"message": f"Task {task_id} assigned to {pod_name}"}

@app.post("/clear-task")
def clear_task():
    """
    Clears the current task assignment for the worker pod.
    """
    global is_assigned_task, assigned_task_id
    is_assigned_task = False
    assigned_task_id = None
    return {"message": f"Task cleared for {pod_name}"}





def run_command_and_store_output(output_file,startTime,endTime, VideoInternalFileID):
    """
    Executes a hardcoded ffmpeg command, stores the output in a file, and returns the result.

    Args:
        output_file (str): Path to the text file to store the output.

    Returns:
        dict: A dictionary containing 'stdout', 'stderr', and 'returncode'.
    """
    # Hardcoded ffmpeg command
    command = f"""
    ffmpeg -ss {str(startTime)} -to {str(endTime)} -i {VideoInternalFileID}.mp4 \
    -map 0:v:0 -map 0:a:0 \
    -map 0:v:0 -map 0:a:0 \
    -map 0:v:0 -map 0:a:0 \
    -map 0:v:0 -map 0:a:0 \
    -map 0:v:0 -map 0:a:0 \
    -map 0:v:0 -map 0:a:0 \
    -b:v:0 500k -filter:v:0 "scale=256:144" -profile:v:0 high \
    -b:v:1 800k -filter:v:1 "scale=426:240" -profile:v:1 high \
    -b:v:2 1200k -filter:v:2 "scale=640:360" -profile:v:2 high \
    -b:v:3 2000k -filter:v:3 "scale=854:480" -profile:v:3 high \
    -b:v:4 4000k -filter:v:4 "scale=1280:720" -profile:v:4 high \
    -b:v:5 6000k -filter:v:5 "scale=1920:1080" -profile:v:5 high \
    -c:a:0 aac -b:a:0 128k -ac 2 \
    -c:a:1 aac -b:a:1 192k -ac 2 \
    -f dash -min_seg_duration 2000 -use_template 1 -use_timeline 1 \
    -init_seg_name '{VideoInternalFileID}_{startTime:%M}.mp4' \
    -media_seg_name ''{VideoInternalFileID}_{startTime:%M}_$RepresentationID$_$Number$.m4s' \
    -adaptation_sets "id=0,streams=v id=1,streams=a" \
    manifest_2_4.mpd
    """
    # print(command)
    try:
        # Run the hardcoded command
        result = subprocess.run(command, shell=True, text=True,
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        
        # Write both stdout and stderr to the specified output file
        with open(output_file, "w") as file:
            file.write("Command Output (stdout):\n")
            file.write(result.stdout.strip() + "\n\n")
            
            file.write("Error Output (stderr):\n")
            file.write(result.stderr.strip() + "\n\n") 
            
            file.write(f"Return Code: {result.returncode}\n")

        # Return the result as a dictionary
        return {
            "stdout": result.stdout.strip(),
            "stderr": result.stderr.strip(),
            "returncode": result.returncode
        }
    except Exception as e:
        # Handle unexpected exceptions
        with open(output_file, "w") as file:
            file.write(f"Unexpected Error: {str(e)}\n")
    
    upload_chunk(file,VideoInternalFileID, Index, podNames)
    return {
            "stdout": "",
            "stderr": str(e),
            "returncode": -1
        }



# Set up logging
# logging.basicConfig(level=logging.INFO)
# log = logging.getLogger(__name__)

# class Chunk:
#     """
#     A class to represent a file chunk.
#     """
#     def __init__(self, chunk_id, chunk_index, file_id):
#         self.chunk_id = chunk_id
#         self.chunk_index = chunk_index
#         self.file_id = file_id

#     def __repr__(self):
#         return f"Chunk(chunk_id='{self.chunk_id}', chunk_index={self.chunk_index}, file_id='{self.file_id}')"

# class RedisRepository:

#     def __init__(self):
#         self.storage = []

#     def save(self, obj):
#         self.storage.append(obj)

#     def count(self):
#         return len(self.storage)

# class ChunkService:
#     """
#     Service to pick slave nodes for chunks.
#     """
#     def pick_slave_node(self):
#         return [f"slave_pod_{i}" for i in range(3)]  # Example: 3 slave pods available


# class File:
#     """
#     A class to represent a file with chunks.
#     """
#     def __init__(self, file_id, size):
#         self.file_id = file_id
#         self.size = size
#         self.chunk_list = []

#     def set_chunk_list(self, chunk_list):
#         self.chunk_list = chunk_list

#     def get_size(self):
#         return self.size

#     def get_file_id(self):
#         return self.file_id


# def allocate_chunks(file, slave_pod_redis_repository, chunk_service, file_redis_repository):
#     CHUNK_SIZE = 64* 1000* 1000
#     replication_factor = 3

#     # Calculate number of chunks
#     no_of_chunks = file.get_size() // CHUNK_SIZE
#     last_chunk_size = file.get_size() % CHUNK_SIZE

#     if last_chunk_size > 0:
#         no_of_chunks += 1
        
#     # Check if sufficient slave nodes are available
#     if slave_pod_redis_repository.count() < replication_factor:
#         log.warning("File allocation deferred due to insufficient slave node count")
#         return None

#     chunk_allocations = []
#     chunk_array_list = []

#     for i in range(no_of_chunks):
#         chunk_id = ''.join(random.choices(string.ascii_letters + string.digits, k=16))
#         chunk = Chunk(chunk_id=chunk_id, chunk_index=i, file_id=file.get_file_id())

#         chosen_pods = chunk_service.pick_slave_node()
#         chosen_pods.append(chunk.chunk_id)  # Append chunk ID to the chosen pods
#         chunk_allocations.append(chosen_pods)

#         chunk_array_list.append(chunk)
#         slave_pod_redis_repository.save(chunk)

#         log.info(f"Chunk #{chunk.chunk_index} {chunk.chunk_id} assigned to slaves {chosen_pods[1:]}")
    
#     file.set_chunk_list(chunk_array_list)
#     file_redis_repository.save(file)

#     return chunk_allocations


# Example usage
if __name__ == "__main__":
    # Initialize repositories and services
    slave_pod_redis_repository = RedisRepository()
    file_redis_repository = RedisRepository()
    chunk_service = ChunkService()

    # Mock a file with 150 MB size
    file = File(file_id="file123", size=150_000_000)

    allocations = allocate_chunks(file, slave_pod_redis_repository, chunk_service, file_redis_repository)

    if allocations:
        print("Chunk Allocations:")
        for allocation in allocations:
            print(allocation)










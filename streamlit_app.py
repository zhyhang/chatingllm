import streamlit as st
import json
import time
import uuid
from datetime import datetime

# --- Backend Mock ---
# In a real-world scenario, this part would be replaced by actual calls
# to your backend service (e.g., via HTTP, gRPC, or a message queue).

def backend_one_time_query(payload: dict) -> dict:
    """Mocks a request/response call to the backend."""
    endpoint = payload.get("endpoint", "unknown")
    params = payload.get("params", {})
    st.info(f"📞 Backend received ONE_TIME_QUERY for endpoint: {endpoint} with params: {params}")
    time.sleep(1.5) # Simulate network and processing delay
    return {
        "status": "ok",
        "data": {
            "endpoint": endpoint,
            "user_id": params.get("id", "N/A"),
            "retrieved_at": datetime.now().isoformat(),
            "query_result": f"This is the result for query {endpoint}."
        }
    }

def backend_stream_subscribe(payload: dict):
    """Mocks a streaming data subscription from the backend."""
    topic = payload.get("topic", "default_topic")
    st.info(f"🌊 Backend received STREAM_SUBSCRIBE for topic: {topic}")
    
    # Simulate a stream of 10 data chunks
    for i in range(10):
        yield {
            "stream_chunk": {
                "topic": topic,
                "sequence": i,
                "value": f"Live data point #{i} for {topic}",
                "timestamp": datetime.now().isoformat()
            }
        }
        time.sleep(0.5) # Simulate data arriving every 500ms

def backend_execute_command(payload: dict):
    """Mocks executing a long-running command on the backend."""
    command = payload.get("command", "unknown_command")
    st.info(f"⚙️ Backend received EXECUTE_COMMAND: {command}")
    
    # Simulate a multi-step process
    steps = ["initializing", "processing", "validating", "finalizing"]
    for i, step in enumerate(steps):
        yield {
            "progress_update": {
                "command": command,
                "step": step,
                "progress": (i + 1) / len(steps) * 100
            }
        }
        time.sleep(1.0) # Simulate 1 second per step
    
    # Yield final result
    yield {
        "command_result": {
            "command": command,
            "status": "success",
            "message": "Command executed successfully."
        }
    }

# --- Streamlit Bridge Application ---

def process_client_request(request_str: str):
    """
    Parses the client request, calls the appropriate backend function,
    and yields the responses in the specified protocol format.
    """
    try:
        request = json.loads(request_str)
        header = request.get("header", {})
        request_id = header.get("request_id", str(uuid.uuid4()))
        request_type = request.get("type")
        payload = request.get("payload", {})

        st.success(f"Bridge received request: {request_id} (Type: {request_type})")

        response_header = {
            "request_id": request_id,
            "source_node": "streamlit-bridge-v1",
            "timestamp_utc": datetime.utcnow().isoformat()
        }

        # Route to the correct backend function based on request type
        if request_type == "ONE_TIME_QUERY":
            result_payload = backend_one_time_query(payload)
            response = {
                "header": response_header,
                "status": "COMPLETED",
                "payload": result_payload
            }
            yield json.dumps(response)

        elif request_type == "STREAM_SUBSCRIBE":
            for chunk in backend_stream_subscribe(payload):
                response = {
                    "header": response_header,
                    "status": "STREAMING_CHUNK",
                    "payload": chunk
                }
                yield json.dumps(response)
            # Signal end of stream
            yield json.dumps({
                "header": response_header,
                "status": "COMPLETED",
                "payload": {"message": "Stream finished."}
            })

        elif request_type == "EXECUTE_COMMAND":
            for progress_chunk in backend_execute_command(payload):
                response = {
                    "header": response_header,
                    "status": "STREAMING_CHUNK",
                    "payload": progress_chunk
                }
                yield json.dumps(response)
            # Signal end of command
            yield json.dumps({
                "header": response_header,
                "status": "COMPLETED",
                "payload": {"message": "Command finished."}
            })

        else:
            raise ValueError(f"Unknown request type: {request_type}")

    except Exception as e:
        st.error(f"Error processing request: {e}")
        # Yield a formatted error message back to the client
        error_response_header = {
            "request_id": locals().get("request_id", "unknown"),
            "source_node": "streamlit-bridge-v1",
            "timestamp_utc": datetime.utcnow().isoformat()
        }
        yield json.dumps({
            "header": error_response_header,
            "status": "ERROR",
            "payload": {
                "error": {
                    "code": "BRIDGE_PROCESSING_ERROR",
                    "message": str(e)
                }
            }
        })


# --- Main App UI ---

st.set_page_config(page_title="Streamlit Data Hub", page_icon="🌉")

st.title("🌉 Streamlit Data Hub")
st.caption("This application acts as a secure bridge between clients and backend services.")

# Initialize chat history
if "messages" not in st.session_state:
    st.session_state.messages = [{
        "role": "assistant", 
        "content": "Hub is active. Waiting for client connections..."
    }]

# Display chat messages from history on app rerun
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.code(message["content"], language="json")

# Accept client input
if prompt := st.chat_input("Listening for client messages..."):
    # Add user message to chat history
    st.session_state.messages.append({"role": "user", "content": prompt})
    # Display user message in chat message container
    with st.chat_message("user"):
        st.code(prompt, language="json")

    # Display assistant response in chat message container
    with st.chat_message("assistant"):
        # Use a generator to stream the response
        response_stream = process_client_request(prompt)
        # write_stream handles iterating through the generator and printing each chunk
        full_response = st.write_stream(response_stream)

    # Add the full response to session state (optional)
    st.session_state.messages.append({"role": "assistant", "content": full_response}) 
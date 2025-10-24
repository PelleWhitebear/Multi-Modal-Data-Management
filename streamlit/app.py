import streamlit as st
import time
import subprocess
from collections import deque
import logging

logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - [%(levelname)s] - %(message)s',
                    force=True,
                    filemode='w',
                    filename='streamlit.log')  # override any existing config

st.set_page_config(
    page_title="ADSDB Control Panel",
    page_icon="streamlit/upc.png",
    layout="wide"
)

st.title("ADSDB Data Warehouse Control Panel")

st.header("Pipeline Execution")

if "tail" not in st.session_state:
    st.session_state.tail = deque(maxlen=5)

selected_pipeline = st.selectbox("Select what part of the pipeline to run", ["landing zone", "formatted zone", "trusted zone", "exploitation zone", "full pipeline"], index=4)
log_placeholder = st.code("Log output will appear here...", height=100)
pipeline_button = st.button("Run")

match selected_pipeline:
    case "landing zone":
        if pipeline_button:
            info = st.info("Running landing zone...")

            proc = subprocess.Popen(
                ["/bin/bash", "/app/landing_zone/landing_zone.sh"],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1
            )
    
    case "formatted zone":
        if pipeline_button:
            info = st.info("Running formatted zone...")
            proc = subprocess.Popen(
                ["/bin/bash", "/app/formatted_zone/formatted_zone.sh"],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1
            )

    case "trusted zone":
        if pipeline_button:
            info = st.info("Running trusted zone...")
            proc = subprocess.Popen(
                ["/bin/bash", "/app/trusted_zone/trusted_zone.sh"],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1
            )

    case "exploitation zone":
        if pipeline_button:
            info = st.info("Running exploitation zone...")
            proc = subprocess.Popen(
                ["/bin/bash", "/app/exploitation_zone/exploitation_zone.sh"],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1
            )
    
    case "full pipeline":
        if pipeline_button:
            info = st.info("Running full pipeline...")
            proc = subprocess.Popen(
                ["/bin/bash", "/app/global_scripts/run_pipeline.sh"],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1
            )

if pipeline_button:
    for raw in iter(proc.stdout.readline, ""):
        if not raw:
            break
        line = raw.strip("\n")
        st.session_state.tail.append(line)

        tail_text = "\n".join(st.session_state.tail)
        log_placeholder.code(tail_text, language="python")
        if "error" in line.lower():
            time.sleep(10)

        time.sleep(0.02)

    proc.stdout.close()
    proc.wait()
    info.success("Pipeline finished!")
    time.sleep(2)
    info.empty()

st.divider()

column = st.columns(5)
with column[0]:
    task = st.radio("Select Task", ["Similarity Search", "RAG"], )

if task == "RAG":
    st.header("Retrieval-Augmented Generation (RAG)")
    user_query = st.text_area("Enter your query", "Sample text input...")

    if st.button("Run RAG"):
        st.info(f"Answering RAG query...")
        time.sleep(2)
        st.success(f"Simulated RAG response:")
        st.write("This is a simulated RAG response based on the provided input.")
else:
    st.header("Similarity Search")
    query_col1, query_col2 = st.columns(2)
    with query_col1:
        modality = st.selectbox("Select Input Modality", ["text", "image", "video"])

    output_types = query_col2.multiselect("Select Output Modality", ["text", "image", "video"])
    col1, col2 = st.columns(2)

    if modality == "text":
        user_input = st.text_area("Enter your text query", "Sample text input...")
    elif modality == "image":
        user_input = st.file_uploader("Upload your image", type=["jpg", "jpeg", "png"])
    else:
        user_input = st.file_uploader("Upload your video", type=["mp4"])
    st.write("user_input:", user_input)
    if st.button("Run Similarity Search"):
        st.info(f"Searching for similar items...")
        task_proc = subprocess.Popen(
            ["python", "-m", "similarity_search.similarity_search", 
             "--input-type", modality,
             "--input-value", user_input,
             "--output-type", *output_types,
             "--top-k", "5"],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1
        )
        log_placeholder_2 = st.code("Log output will appear here...", height=100)
        for raw in iter(task_proc.stdout.readline, ""):
            if not raw:
                break
            line = raw.strip("\n")
            st.session_state.tail.append(line)

            tail_text = "\n".join(st.session_state.tail)
            log_placeholder_2.code(tail_text, language="python")
            if "error" in line.lower():
                time.sleep(10)

            time.sleep(0.02)

        task_proc.stdout.close()
        task_proc.wait()
        with open("results.txt", "r") as f:
            results = f.read()
        st.success(f"Similarity search completed. Results:")
        st.write(results)

st.divider()

st.caption("ADSDB Data Warehouse Control Panel - Developed by the Confusion Matrix Crew")
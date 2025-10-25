import streamlit as st
import time
import boto3
import os
import subprocess
from collections import deque
import logging
import json
from collections import defaultdict
import ast
from dotenv import load_dotenv, find_dotenv
import tempfile
load_dotenv(find_dotenv())

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

def main():
    

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

        if modality == "text":
            user_input = st.text_area("Enter your text query", "Sample text input...")
            uploaded_file = None
        elif modality == "image":
            uploaded_file = st.file_uploader("Upload your image", type=["jpg", "jpeg", "png"])
            user_input = uploaded_file.getvalue() if uploaded_file else None
        else:
            uploaded_file = st.file_uploader("Upload your video", type=["mp4"])
            user_input = uploaded_file.getvalue() if uploaded_file else None

        if st.button("Run Similarity Search"):
            if (user_input is None or user_input == "Sample text input...") and uploaded_file is None:
                st.warning("Please provide an input.")
                return

            input_arg = ""
            temp_file_path = None

            if modality == "text":
                input_arg = user_input
            else:
                if uploaded_file is not None:
                    with tempfile.NamedTemporaryFile(delete=False, suffix=os.path.splitext(uploaded_file.name)[1]) as tmp:
                        tmp.write(uploaded_file.getvalue())
                        temp_file_path = tmp.name
                    input_arg = temp_file_path

            if not input_arg:
                st.warning("Input is missing.")
                return

            meanwhile = st.info(f"Searching for similar items...")
            task_proc = subprocess.Popen(
                ["python", "-m", "similarity_search.similarity_search",
                "--input-type", modality,
                "--input-value", input_arg,
                "--output-type", *output_types,
                "--top-k", os.getenv("TOP_K")],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1
            )
            res = {}
            log_placeholder_2 = st.code("Log output will appear here...", height=100)
            for raw in iter(task_proc.stdout.readline, ""):
                if not raw:
                    break
                line = raw.strip("\n")
                if "@@@" in line and "###" in line:
                    result_line = line.strip().split("@@@")[1].split("###")
                    res[result_line[0]] = {"id": ast.literal_eval(result_line[1]), "distance": ast.literal_eval(result_line[2])}
                    continue
                st.session_state.tail.append(line)
                tail_text = "\n".join(st.session_state.tail)
                if "Loaded built-in ViT-B-32 model config." in line:
                    tail_text += "\nNote: The first run may take longer due to model loading."
                log_placeholder_2.code(tail_text, language="python")

                time.sleep(0.02)

            task_proc.stdout.close()
            task_proc.wait()

            if temp_file_path:
                os.remove(temp_file_path)

            meanwhile.success("Similarity search completed!")
            try:
                s3_client = boto3.client(
                    "s3",
                    endpoint_url=os.getenv("ENDPOINT_URL"),
                    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
                    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
                )
                logging.info("Connected to MinIO.")
            except Exception:
                logging.exception("Error connecting to MinIO.")
                return

            objs = s3_client.list_objects_v2(Bucket=os.getenv("EXPLOITATION_ZONE_BUCKET"), Prefix="json/")
            if "Contents" not in objs:
                logging.error("No JSON files found in exploitation-zone.")
                return
            for obj in objs["Contents"]:
                if obj["Key"].endswith("#enhanced_games.json"):
                    game_obj = s3_client.get_object(Bucket=os.getenv("EXPLOITATION_ZONE_BUCKET"), Key=obj["Key"])
                    games = json.loads(game_obj["Body"].read().decode("utf-8"))
                    break
            image_objs = s3_client.list_objects_v2(Bucket=os.getenv("EXPLOITATION_ZONE_BUCKET"), Prefix="media/image/")
            if "Contents" not in image_objs:
                logging.error("No image files found in exploitation-zone.")
                return
            video_objs = s3_client.list_objects_v2(Bucket=os.getenv("EXPLOITATION_ZONE_BUCKET"), Prefix="media/video/")
            if "Contents" not in video_objs:
                logging.error("No video files found in exploitation-zone.")
                return
            texts = []
            images = []
            videos = []
            for type, value in res.items():
                id = value["id"]
                distance = value["distance"]
                match type:
                    case "text":
                        for text_id, text_distance in zip(value["id"], value["distance"]):
                            texts.append((games[text_id]["name"], text_distance, games[text_id]["final_description"]))
                    case "image":
                        for img_id, img_distance in zip(value["id"], value["distance"]):
                            for img_obj in image_objs["Contents"]:
                                if img_obj["Key"].endswith(f"{img_id}.jpg".replace("_", "#")):
                                    img_data = s3_client.get_object(Bucket=os.getenv("EXPLOITATION_ZONE_BUCKET"), Key=img_obj["Key"])
                                    images.append((games[img_id.split("_")[0]]["name"], img_distance, img_data["Body"].read()))
                                    break
                    case "video":
                        for video_id, video_distance in zip(value["id"], value["distance"]):
                            for vid_obj in video_objs["Contents"]:
                                if vid_obj["Key"].endswith(f"{video_id}#1.mp4"):
                                    vid_data = s3_client.get_object(Bucket=os.getenv("EXPLOITATION_ZONE_BUCKET"), Key=vid_obj["Key"])
                                    videos.append((games[video_id]["name"], video_distance, vid_data["Body"].read()))
                                    break
            # Display results
            if texts:
                st.subheader("Text Results")
                cols = st.columns(4)
                for text_name, dist, description in texts:
                    with cols[ texts.index((text_name, dist, description)) % 4 ]:
                        st.markdown(f"**Name:** {text_name}")
                        st.markdown(f"**Similarity:** {(1-dist)*100:.2f}%")
                        st.write(description)
            st.divider()
            if images:
                st.subheader("Image Results")
                cols = st.columns(4)
                for image_name, dist, img_bytes in images:
                    with cols[ images.index((image_name, dist, img_bytes)) % 4 ]:
                        st.markdown(f"**Name:** {image_name}")
                        st.markdown(f"**Similarity:** {(1-dist)*100:.2f}%")
                        st.image(img_bytes)
            st.divider()
            if videos:
                st.subheader("Video Results")
                cols = st.columns(4)
                for video_name, dist, vid_bytes in videos:
                    with cols[ videos.index((video_name, dist, vid_bytes)) % 4 ]:
                        st.markdown(f"**Name:** {video_name}")
                        st.markdown(f"**Similarity:** {(1-dist)*100:.2f}%")
                        st.video(vid_bytes)
    st.divider()

    st.caption("ADSDB Data Warehouse Control Panel - Developed by the Confusion Matrix Crew")

if __name__ == "__main__":
    main()
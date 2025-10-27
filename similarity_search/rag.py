import argparse
import os
from typing import List
from global_scripts.utils import *
from global_scripts.prompts import FilteredGame, hyde_prompt, filtering_prompt, rag_response_prompt
import dotenv

dotenv.load_dotenv(dotenv.find_dotenv())

def main(args):
    s3_client = minio_init()
    games = load_games_from_minio(s3_client, os.getenv("EXPLOITATION_ZONE_BUCKET"), "json/", "enhanced_games.json")
    chroma_client = chroma_init()
    gemini_client = gemini_init()

    # Run HyDE on the query
    hyde_query = query_gemini(gemini_client, hyde_prompt.format(query=args.query))

    results = []
    for collection in ["text", "image", "video"]:
        results.extend(query_chromadb(chroma_client, "text", hyde_query, collection, k=3))
    logging.info(f"ChromaDB results: {results}")
    results.sort(key=lambda x: x[1]) # Sort by distance ascending

    res_set = set()
    unique_results = []
    for id, distance in results:
        id_aux = id if "_" not in id else id.split("_")[0]
        if id_aux not in res_set:
            res_set.add(id_aux)
            unique_results.append((id, distance))

    top_5 = unique_results[:5]

    # Get descriptions for top 5 results
    name_desc = []
    for id, distance in top_5:
        game_id = id if "_" not in id else id.split("_")[0]
        game_info = games.get(game_id, {})
        name_desc.append({"name": game_info.get("name", "Unknown Title"), "description": game_info.get("final_description", "No description available."), "distance": distance})

    logging.info(f"Top 5 games with descriptions: {name_desc}")

    # Filter using Gemini
    config = {
        "response_mime_type": "application/json",
        "response_schema": list[FilteredGame],
    }
    filtered_results = query_gemini(gemini_client, filtering_prompt.format(query=args.query, games=name_desc), config=config)
    
    logging.info(f"Filtered results: {filtered_results}")

    try:
        filtered_results = json.loads(filtered_results)
    except json.JSONDecodeError:
        filtered_results = name_desc

    final_games = [{**n_d, "reasoning": game["reasoning"]} for n_d, game in zip(name_desc, filtered_results) if game["is_relevant"]]
    logging.info(f"Final games after filtering: {final_games}")
    # Generate final response:
    final_response = query_gemini(gemini_client, rag_response_prompt.format(query=args.query, games=final_games))
    
    logging.info(f"@@@{final_response}@@@")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run RAG similarity search.")
    parser.add_argument("--query", type=str, required=True, help="The query string for similarity search.")
    args = parser.parse_args()
    main(args)
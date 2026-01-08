"""
Visualization script to create 3 t-SNE 2D plots (1 per game with both models).

For each of the top 3 games with largest differences, creates:
- 1 plot showing baseline and fp16 models together (5 images + 1 description each)
- Different colors for each model (baseline = blue, fp16 = orange)
Total: 3 plots
"""

import json
import logging
import os

import matplotlib.pyplot as plt
import numpy as np
from sklearn.manifold import TSNE

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - [%(levelname)s] - %(message)s",
    force=True,
)


def load_analysis_results():
    """Load embeddings and metadata from test.py results."""
    results_dir = os.path.join(os.path.dirname(__file__), "analysis_results")

    logging.info(f"Loading results from {results_dir}...")

    baseline_embeddings = np.load(
        os.path.join(results_dir, "embeddings_baseline.npy"), allow_pickle=True
    ).item()

    fp16_embeddings = np.load(os.path.join(results_dir, "embeddings_fp16.npy"), allow_pickle=True).item()

    with open(os.path.join(results_dir, "game_metadata.json"), "r") as f:
        metadata = json.load(f)

    logging.info(f"Loaded embeddings for {len(baseline_embeddings)} games.")

    return baseline_embeddings, fp16_embeddings, metadata


def plot_game_both_models_tsne(baseline_embeddings, fp16_embeddings, game_id, output_dir):
    """
    Create a 2D t-SNE plot for a single game showing both baseline and fp16 models.

    Args:
        baseline_embeddings: dict with 'image_embeddings' [5, dim] and 'text_embedding' [1, dim]
        fp16_embeddings: dict with 'image_embeddings' [5, dim] and 'text_embedding' [1, dim]
        game_id: str, the game identifier
        output_dir: str, directory to save the plot
    """
    # Stack all embeddings from both models
    baseline_images = baseline_embeddings["image_embeddings"]  # [5, dim]
    baseline_text = baseline_embeddings["text_embedding"]  # [1, dim]
    fp16_images = fp16_embeddings["image_embeddings"]  # [5, dim]
    fp16_text = fp16_embeddings["text_embedding"]  # [1, dim]

    all_embeds = np.vstack([baseline_images, baseline_text, fp16_images, fp16_text])  # [12, dim]

    # Apply t-SNE to reduce to 2D
    logging.info(f"  Running t-SNE for game {game_id}...")
    tsne = TSNE(n_components=2, random_state=42, perplexity=5, max_iter=1000)
    embeds_2d = tsne.fit_transform(all_embeds)

    # Split back into baseline and fp16
    baseline_2d = embeds_2d[:6]  # 5 images + 1 text
    fp16_2d = embeds_2d[6:]  # 5 images + 1 text

    # Create plot
    fig, ax = plt.subplots(figsize=(12, 10))

    # Plot baseline model (blue)
    # Plot the 5 images
    ax.scatter(
        baseline_2d[:5, 0],
        baseline_2d[:5, 1],
        c="#3498db",
        marker="o",
        s=250,
        alpha=0.7,
        label="Baseline",
        edgecolors="black",
        linewidths=2,
    )
    # Add image labels
    for i in range(5):
        ax.annotate(
            f"Img{i + 1}",
            (baseline_2d[i, 0], baseline_2d[i, 1]),
            textcoords="offset points",
            xytext=(0, 10),
            ha="center",
            fontsize=9,
            fontweight="bold",
            color="#3498db",
        )
    # Plot the description
    ax.scatter(
        baseline_2d[5, 0],
        baseline_2d[5, 1],
        c="#3498db",
        marker="o",
        s=250,
        alpha=0.7,
        edgecolors="black",
        linewidths=2,
    )
    ax.annotate(
        "Desc",
        (baseline_2d[5, 0], baseline_2d[5, 1]),
        textcoords="offset points",
        xytext=(0, 10),
        ha="center",
        fontsize=9,
        fontweight="bold",
        color="#3498db",
    )

    # Plot fp16 model (orange)
    # Plot the 5 images
    ax.scatter(
        fp16_2d[:5, 0],
        fp16_2d[:5, 1],
        c="#ff7f0e",
        marker="o",
        s=250,
        alpha=0.7,
        label="FP16",
        edgecolors="black",
        linewidths=2,
    )
    # Add image labels
    for i in range(5):
        ax.annotate(
            f"Img{i + 1}",
            (fp16_2d[i, 0], fp16_2d[i, 1]),
            textcoords="offset points",
            xytext=(0, 10),
            ha="center",
            fontsize=9,
            fontweight="bold",
            color="#ff7f0e",
        )
    # Plot the description
    ax.scatter(
        fp16_2d[5, 0],
        fp16_2d[5, 1],
        c="#ff7f0e",
        marker="o",
        s=250,
        alpha=0.7,
        edgecolors="black",
        linewidths=2,
    )
    ax.annotate(
        "Desc",
        (fp16_2d[5, 0], fp16_2d[5, 1]),
        textcoords="offset points",
        xytext=(0, 10),
        ha="center",
        fontsize=9,
        fontweight="bold",
        color="#ff7f0e",
    )

    # Formatting
    ax.set_xlabel("t-SNE Dimension 1", fontsize=12)
    ax.set_ylabel("t-SNE Dimension 2", fontsize=12)
    ax.set_title(
        f"Game {game_id} - Baseline vs FP16",
        fontsize=14,
        fontweight="bold",
    )
    ax.legend(loc="best", fontsize=12, framealpha=0.9)
    ax.grid(True, alpha=0.3)

    # Set equal aspect ratio for better visualization
    ax.set_aspect("equal", adjustable="box")

    plt.tight_layout()

    # Save
    output_path = os.path.join(output_dir, f"tsne_game_{game_id}_both_models.png")
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    logging.info(f"Saved: tsne_game_{game_id}_both_models.png")
    plt.close()


def main():
    # Load results
    logging.info("=" * 60)
    logging.info("LOADING ANALYSIS RESULTS")
    logging.info("=" * 60)

    baseline_embeddings, fp16_embeddings, metadata = load_analysis_results()

    # Create output directory
    output_dir = os.path.join(os.path.dirname(__file__), "visualization_outputs")
    os.makedirs(output_dir, exist_ok=True)
    logging.info(f"\nSaving visualizations to {output_dir}/")

    # Get top 3 games with largest differences
    top_games = [item["game_id"] for item in metadata["analysis_a_top3_largest_differences"]]

    # Generate 3 plots (1 per game with both models)
    logging.info("\n" + "=" * 60)
    logging.info("GENERATING 3 t-SNE PLOTS (BOTH MODELS)")
    logging.info("=" * 60)
    logging.info(f"Games: {', '.join(top_games)}\n")

    for game_id in top_games:
        logging.info(f"Processing Game {game_id}...")
        plot_game_both_models_tsne(
            baseline_embeddings[game_id], fp16_embeddings[game_id], game_id, output_dir
        )

    logging.info("\n" + "=" * 60)
    logging.info("VISUALIZATION COMPLETE!")
    logging.info("=" * 60)
    logging.info(f"\nAll 3 plots saved to: {output_dir}/")
    logging.info("\nGenerated files:")
    for game_id in top_games:
        logging.info(f"  - tsne_game_{game_id}_both_models.png")


if __name__ == "__main__":
    main()

# Metrics functions for evaluating multi-modal retrieval models

import numpy as np
import torch


def recall_at_k(sorted_indices, correct_indices, k):
    """
    Calculate Recall@K for image-text retrieval.

    Args:
        sorted_indices: Tensor of shape (n_queries, n_items) with sorted item indices (descending by similarity)
        correct_indices: List/array of correct item indices for each query
        k: Top-K items to consider

    Returns:
        float: Recall@K score
    """
    n_queries = sorted_indices.shape[0]
    recall_count = 0

    for i in range(n_queries):
        # Get top-k indices for this query (already sorted)
        top_k_indices = sorted_indices[i, :k]

        # Check if correct index is in top-k
        if correct_indices[i] in top_k_indices:
            recall_count += 1

    return recall_count / n_queries


def mean_average_precision_at_k(sorted_indices, correct_indices, k):
    """
    Calculate Mean Average Precision@K (mAP@K).

    Args:
        sorted_indices: Tensor of shape (n_queries, n_items) with sorted item indices (descending by similarity)
        correct_indices: List/array of correct item indices for each query
        k: Top-K items to consider

    Returns:
        float: mAP@K score
    """
    n_queries = sorted_indices.shape[0]
    average_precisions = []

    for i in range(n_queries):
        # Get top-k indices for this query (already sorted)
        top_k_indices = sorted_indices[i, :k]

        # Find position of correct item in top-k (1-indexed)
        correct_idx = correct_indices[i]
        if correct_idx in top_k_indices:
            position = (top_k_indices == correct_idx).nonzero(as_tuple=True)[0].item() + 1
            # Average Precision for single relevant item = 1/position
            average_precisions.append(1.0 / position)
        else:
            average_precisions.append(0.0)

    return np.mean(average_precisions)


def mean_reciprocal_rank(sorted_indices, correct_indices):
    """
    Calculate Mean Reciprocal Rank (MRR).

    Args:
        sorted_indices: Tensor of shape (n_queries, n_items) with sorted item indices (descending by similarity)
        correct_indices: List/array of correct item indices for each query

    Returns:
        float: MRR score
    """
    n_queries = sorted_indices.shape[0]
    reciprocal_ranks = []

    for i in range(n_queries):
        # Find position of correct item (1-indexed) - already sorted
        correct_idx = correct_indices[i]
        position = (sorted_indices[i] == correct_idx).nonzero(as_tuple=True)[0].item() + 1
        reciprocal_ranks.append(1.0 / position)

    return np.mean(reciprocal_ranks)


def compute_all_metrics(similarities, correct_indices, k_values=[1, 5, 10]):
    """
    Compute all retrieval metrics.

    Args:
        similarities: Tensor of shape (n_queries, n_items) with similarity scores
        correct_indices: List/array of correct item indices for each query
        k_values: List of K values for Recall@K and mAP@K

    Returns:
        dict: Dictionary containing all computed metrics
    """
    # Sort once: get indices sorted by similarity (descending)
    sorted_indices = torch.argsort(similarities, dim=1, descending=True)

    metrics = {}

    # Compute Recall@K for each K (using pre-sorted indices)
    for k in k_values:
        metrics[f"recall@{k}"] = recall_at_k(sorted_indices, correct_indices, k)

    # Compute mAP@K for each K (using pre-sorted indices)
    for k in k_values:
        metrics[f"map@{k}"] = mean_average_precision_at_k(sorted_indices, correct_indices, k)

    # Compute MRR (using pre-sorted indices)
    metrics["mrr"] = mean_reciprocal_rank(sorted_indices, correct_indices)

    return metrics

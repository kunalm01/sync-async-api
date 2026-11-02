"""Shared business logic used by both /sync and /async. Pure and deterministic."""
import hashlib


def do_work(text: str, rounds: int) -> dict:
    """Iterated SHA-256 (CPU-bound, deterministic) plus simple text stats."""
    digest = text.encode()
    for _ in range(rounds):
        digest = hashlib.sha256(digest).digest()
    words = text.split()
    return {
        "digest": digest.hex(),
        "rounds": rounds,
        "word_count": len(words),
        "char_count": len(text),
        "top_word": max(set(words), key=lambda w: (words.count(w), w)) if words else None,
    }

import re
import numpy as np
import chromadb
from rank_bm25 import BM25Okapi
from sentence_transformers import SentenceTransformer, CrossEncoder


def chunk_articles(articles, sentences_per_chunk=2):
    chunks = []
    for article in articles:
        sentences = re.split(r"(?<=[.!?])\s+", article["body"].strip())
        # A one-sentence stride creates overlap without breaking sentence boundaries.
        for index in range(len(sentences)):
            text = " ".join(sentences[index:index + sentences_per_chunk]).strip()
            if text:
                chunks.append({"id": f'{article["article_id"]}-{index}', "text": text,
                               "article_id": article["article_id"], "title": article["title"]})
    return chunks


class HybridRAG:
    def __init__(self, chunks, embedding_model="all-MiniLM-L6-v2", persist_path="artifacts/chroma"):
        self.chunks = chunks
        self.embedder = SentenceTransformer(embedding_model)
        self.embeddings = self.embedder.encode([c["text"] for c in chunks], normalize_embeddings=True)
        self.client = chromadb.PersistentClient(path=persist_path)
        self.collection = self.client.get_or_create_collection(
            name="support_knowledge_v1", metadata={"hnsw:space": "cosine"})
        # Upsert makes repeated notebook runs idempotent and supports incremental refresh.
        self.collection.upsert(
            ids=[c["id"] for c in chunks],
            documents=[c["text"] for c in chunks],
            embeddings=self.embeddings.tolist(),
            metadatas=[{"article_id": c["article_id"], "title": c["title"]} for c in chunks],
        )
        self.bm25 = BM25Okapi([c["text"].lower().split() for c in chunks])
        self.reranker = CrossEncoder("cross-encoder/ms-marco-MiniLM-L-6-v2")

    def search(self, query, candidate_k=8, final_k=3):
        q = self.embedder.encode(query, normalize_embeddings=True)
        vector = self.collection.query(query_embeddings=[q.tolist()], n_results=min(candidate_k, len(self.chunks)))
        id_to_index = {chunk["id"]: index for index, chunk in enumerate(self.chunks)}
        dense_rank = [id_to_index[chunk_id] for chunk_id in vector["ids"][0]]
        sparse_rank = np.argsort(self.bm25.get_scores(query.lower().split()))[::-1][:candidate_k]
        rrf = {}
        for ranking in (dense_rank, sparse_rank):
            for rank, idx in enumerate(ranking, 1):
                rrf[int(idx)] = rrf.get(int(idx), 0.0) + 1 / (60 + rank)
        candidates = [self.chunks[i] for i in sorted(rrf, key=rrf.get, reverse=True)]
        scores = self.reranker.predict([(query, c["text"]) for c in candidates])
        ranked = sorted(zip(scores, candidates), key=lambda x: float(x[0]), reverse=True)

        # Return one best chunk per article to prevent repeated citations/context.
        results, seen_articles = [], set()
        for score, chunk in ranked:
            if chunk["article_id"] in seen_articles:
                continue
            seen_articles.add(chunk["article_id"])
            results.append({**chunk, "rerank_score": round(float(score), 4)})
            if len(results) == final_k:
                break
        return results

    @staticmethod
    def grounded_answer(query, contexts):
        """Deterministic extractive answer for zero-secret Colab runs."""
        if not contexts:
            return {"answer": "I do not have enough verified information.", "citations": []}
        answer = " ".join(c["text"] for c in contexts[:2])
        citations = sorted({c["article_id"] for c in contexts[:2]})
        return {"question": query, "answer": answer, "citations": citations}

    def evaluate_retrieval(self, examples, k=3):
        """Compute deterministic citation hit-rate@k on a labelled evaluation set."""
        rows = []
        for example in examples:
            hits = self.search(example["question"], final_k=k)
            retrieved = [hit["article_id"] for hit in hits]
            rows.append({**example, "retrieved": retrieved,
                         "hit": example["expected_article_id"] in retrieved})
        return {"hit_rate_at_k": sum(row["hit"] for row in rows) / len(rows),
                "k": k, "examples": rows}

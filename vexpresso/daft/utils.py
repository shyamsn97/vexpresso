from types import MethodType
from typing import Any, Iterable, List, Optional

import daft
from daft import col

from vexpresso.retrievers import Retriever
from vexpresso.utils import DataType, ResourceRequest, get_batch_size


@daft.udf(return_dtype=DataType.int64())
def indices(columnn):
    return list(range(len(columnn.to_pylist())))


@daft.udf(return_dtype=DataType.python())
def add_column(columnn, values):
    return values


@daft.udf(return_dtype=DataType.python())
def retrieve_udf(embedding_col, query_embeddings, retriever, k):
    embeddings = embedding_col.to_pylist()
    retrieval_outputs = retriever.retrieve(
        query_embeddings=query_embeddings, embeddings=embeddings, k=k
    )
    out = []
    for i in range(len(embeddings)):
        r = []
        for retrieval_output in retrieval_outputs:
            indices = retrieval_output.indices
            scores = retrieval_output.scores
            if i >= len(scores):
                score = -(10**10)
            else:
                score = scores[i]
            results = {"retrieve_index": None, "retrieve_score": score}
            if i in indices:
                results["retrieve_index"] = i

            r.append(results)
        out.append(r)
    return out


def retrieve(
    batch_size: int,
    df: daft.DataFrame,
    embedding_column_name: str,
    query_embeddings: Iterable[Any],
    retriever: Retriever,
    k: int = None,
    sort: bool = True,
    return_scores: bool = False,
    score_column_name: Optional[str] = None,
    resource_request: ResourceRequest = ResourceRequest(),
) -> List[daft.DataFrame]:
    if score_column_name is None:
        score_column_name = f"{embedding_column_name}_score"

    df = df.with_column(
        "retrieve_output",
        retrieve_udf(
            col(embedding_column_name),
            query_embeddings=query_embeddings,
            k=k,
            retriever=retriever,
        ),
        resource_request=resource_request,
    )

    dfs = []
    batch_size = get_batch_size(query_embeddings)
    for i in range(batch_size):
        _df = (
            df.with_column(
                "retrieve_index",
                col("retrieve_output").apply(
                    lambda x: x[i]["retrieve_index"], return_dtype=DataType.int64()
                ),
            )
            .with_column(
                score_column_name,
                col("retrieve_output").apply(
                    lambda x: x[i]["retrieve_score"], return_dtype=DataType.float64()
                ),
            )
            .exclude("retrieve_output")
            .where(col("retrieve_index") != -1)
            .exclude("retrieve_index")
        )
        if sort:
            _df = _df.sort(col(score_column_name), desc=True)

        if not return_scores:
            _df = _df.exclude(score_column_name)

        dfs.append(_df)
    return dfs


class Wrapper:
    def __init__(self, collection):
        self.collection = collection

    def __repr__(self) -> str:
        return self.daft_df.__repr__()

    def __getattr__(self, name):
        if hasattr(self.collection.daft_df, name):
            func = getattr(self.collection.daft_df, name)
            return lambda *args, **kwargs: self._wrap(func, args, kwargs)
        raise AttributeError(name)

    def _wrap(self, func, args, kwargs):
        if type(func) == MethodType:  # noqa
            daft_df = func(*args, **kwargs)
        else:
            daft_df = func(self.collection.daft_df, *args, **kwargs)
        if daft_df is None:
            return self.collection
        return self.collection.from_daft_df(daft_df)

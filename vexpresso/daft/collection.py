from __future__ import annotations

import json
import os
from typing import Any, Dict, Iterable, List, Optional, Union

import daft
import numpy as np
import pandas as pd
import pyarrow.parquet as pq
import ray
from daft import col

from vexpresso.collection import Collection
from vexpresso.filter import FilterHelper
from vexpresso.retriever import NumpyRetriever, Retriever
from vexpresso.utils import Transformation, lazy, transformation


@daft.udf(return_dtype=daft.DataType.int64())
def indices(columnn):
    return list(range(len(columnn)))


def embed(content_list, embedding_fn):
    # dumb langchain check, might need something more specific here
    if getattr(embedding_fn, "embed_documents", None) is not None:
        return np.array(embedding_fn.embed_documents(content_list))
    return np.array(embedding_fn(content_list))


@daft.udf(return_dtype=daft.DataType.python())
def _retrieve(embedding_col, query_embeddings, retriever, k):
    embeddings = embedding_col.to_pylist()
    retrieval_output = retriever.retrieve(
        query_embeddings=query_embeddings, embeddings=embeddings, k=k
    )[0]
    indices = retrieval_output.indices
    scores = retrieval_output.scores

    results = [
        {"retrieve_index": None, "retrieve_score": scores[i]}
        for i in range(len(embeddings))
    ]

    for idx in indices:
        results[idx]["retrieve_index"] = idx

    return results


class DaftCollection(Collection):
    def __init__(
        self,
        data: Optional[Union[str, pd.DataFrame]] = None,
        retriever: Retriever = NumpyRetriever(),
        embedding_functions: Dict[str, Any] = {},
        daft_df: Optional[daft.DataFrame] = None,
    ):
        self.df = daft_df
        self.retriever = retriever
        self.embedding_functions = embedding_functions

        _metadata_dict = {}

        if data is not None:
            if isinstance(data, str):
                if data.endswith(".json"):
                    with open(data, "r") as f:
                        data = pd.DataFrame(json.load(f))
            elif isinstance(data, pd.DataFrame):
                _metadata_dict = data.to_dict("list")
            else:
                _metadata_dict = data

        if daft_df is None and len(_metadata_dict) > 0:
            self.df = daft.from_pydict({**_metadata_dict})
            self.df = self.df.with_column(
                "vexpresso_index", indices(col(self.column_names[0]))
            )

    def __len__(self) -> int:
        return self.df.count_rows()

    def __getitem__(self, column: str) -> Collection:
        return self.select(column)

    def __setitem__(self, column: str, value: List[Any]) -> None:
        self.df = self.add_column(column=value, name=column).df

    def add_rows(self, data: List[Dict[str, Any]]) -> DaftCollection:
        dic = self.to_dict()
        for k in dic:
            for d in data:
                value = d.get(k, None)
                dic[k].append(value)
        return self.from_data(dic)

    def set_embedding_function(self, column: str, embedding_function: Transformation):
        self.embedding_functions[column] = embedding_function

    @property
    def column_names(self) -> List[str]:
        return self.df.column_names

    def from_df(self, df: daft.DataFrame) -> DaftCollection:
        return DaftCollection(
            retriever=self.retriever,
            embedding_functions=self.embedding_functions,
            daft_df=df,
        )

    def from_data(self, data: Any) -> DaftCollection:
        return DaftCollection(
            data=data,
            retriever=self.retriever,
            embedding_functions=self.embedding_functions,
        )

    def add_column(self, column: List[Any], name: str = None) -> DaftCollection:
        if name is None:
            num_columns = len(self.df.column_names)
            name = f"column_{num_columns}"

        new_df = daft.from_pydict({name: column})
        df = self.df.with_column(name, new_df[name])
        return self.from_df(df)

    def collect(self, in_place: bool = False):
        if in_place:
            self.df = self.df.collect()
            return self
        return self.from_df(self.df.collect())

    def execute(self) -> DaftCollection:
        return self.collect()

    @classmethod
    def from_collection(cls, collection: DaftCollection, **kwargs) -> DaftCollection:
        kwargs = {
            "daft_df": collection.df,
            "retriever": collection.retriever,
            **kwargs,
        }
        return DaftCollection(**kwargs)

    def clone(self, **kwargs) -> DaftCollection:
        kwargs = {
            "df": self.df,
            "embeddings_fn": self.embeddings_fn,
            "retriever": self.retriever,
            **kwargs,
        }
        return DaftCollection(**kwargs)

    def to_pandas(self) -> pd.DataFrame:
        collection = self.execute()
        return collection.df.to_pandas()

    def to_dict(self) -> Dict[str, List[Any]]:
        collection = self.execute()
        return collection.df.to_pydict()

    def to_list(self) -> List[Any]:
        collection = self.execute()
        return list(collection.df.to_pydict().values())

    def show(self, num_rows: int):
        return self.df.show(num_rows)

    def _retrieve(
        self,
        df,
        column_name: str,
        query: Union[str, List[Any]],
        query_embeddings=None,
        k: int = None,
        sort=True,
        embedding_fn: Optional[Transformation] = None,
        score_column_name: Optional[str] = None,
        *args,
        **kwargs,
    ) -> daft.DataFrame:
        if embedding_fn is None:
            embedding_fn = self.embedding_functions[column_name]
        else:
            if column_name in self.embedding_functions:
                if embedding_fn != self.embedding_functions[column_name]:
                    print("embedding_fn may not be the same as whats in map!")
            else:
                self.embedding_functions[column_name] = embedding_fn

        if query_embeddings is None:
            query_embeddings = self.embedding_functions[column_name].func(
                query, *args, **kwargs
            )

        embedding_column_name = column_name
        if embedding_column_name not in df.column_names:
            raise ValueError(
                f"{embedding_column_name} not found in daft df. Make sure to call `embed` on column {column_name}..."
            )

        if score_column_name is None:
            score_column_name = f"{column_name}_score"

        df = df.with_column(
            "retrieve_output",
            _retrieve(
                col(embedding_column_name),
                query_embeddings=query_embeddings,
                k=k,
                retriever=self.retriever,
            ),
        )
        df = (
            df.with_column(
                "retrieve_index",
                col("retrieve_output").apply(
                    lambda x: x["retrieve_index"], return_dtype=daft.DataType.int64()
                ),
            )
            .with_column(
                score_column_name,
                col("retrieve_output").apply(
                    lambda x: x["retrieve_score"], return_dtype=daft.DataType.float64()
                ),
            )
            .exclude("retrieve_output")
            .where(col("retrieve_index") != -1)
            .exclude("retrieve_index")
        )
        if sort:
            df = df.sort(col(score_column_name), desc=True)

        return df

    @lazy(default=True)
    def sort(self, column, desc=True) -> DaftCollection:
        return self.from_df(self.df.sort(col(column), desc=desc))

    @lazy(default=True)
    def query(
        self,
        column: str,
        query: List[Any] = None,
        query_embeddings: Any = None,
        filter_conditions: Optional[Dict[str, Dict[str, str]]] = None,
        k=None,
        sort=True,
        embedding_fn: Optional[Transformation] = None,
        score_column_name: Optional[str] = None,
        *args,
        **kwargs,
    ) -> DaftCollection:
        df = self.df
        if k is None:
            k = len(self)

        df = self._retrieve(
            df=df,
            column_name=column,
            query=[query],
            query_embeddings=query_embeddings,
            k=k,
            sort=sort,
            embedding_fn=embedding_fn,
            score_column_name=score_column_name,
            *args,
            **kwargs,
        )

        if filter_conditions is not None:
            df = FilterHelper.filter(df, filter_conditions)

        return self.from_df(df)

    @lazy(default=True)
    def select(
        self,
        *args,
    ) -> DaftCollection:
        return self.from_df(FilterHelper.select(self.df, *args))

    @lazy(default=True)
    def exclude(
        self,
        *args,
    ) -> DaftCollection:
        return self.from_df(self.df.exclude(*args))

    @lazy(default=True)
    def filter(
        self, filter_conditions: Dict[str, Dict[str, str]], *args, **kwargs
    ) -> DaftCollection:
        return self.from_df(
            FilterHelper.filter(self.df, filter_conditions, *args, **kwargs)
        )

    @lazy(default=True)
    def apply(
        self, transform_fn: Transformation, *args, to: Optional[str] = None, **kwargs
    ) -> DaftCollection:
        if getattr(transform_fn, "__vexpresso_transform", None) is None:
            transform_fn = transformation(transform_fn)

        if not isinstance(args[0], DaftCollection):
            raise TypeError(
                "first args in apply must be a DaftCollection! use `collection['column_name']`"
            )

        _args = []
        for _arg in args:
            if isinstance(_arg, DaftCollection):
                column = _arg.df.columns[0]
                _args.append(column)
            else:
                _args.append(_arg)

        _kwargs = {}
        for k in kwargs:
            _kwargs[k] = kwargs[k]
            if isinstance(_kwargs[k], DaftCollection):
                # only support first column
                column = _kwargs[k].df.columns[0]
                _kwargs[k] = column

        if to is None:
            to = f"tranformed_{_args[0].name()}"

        df = self.df.with_column(to, transform_fn(*_args, **_kwargs))
        return self.from_df(df)

    @lazy(default=True)
    def embed(
        self,
        column_name: str,
        content: Optional[List[Any]] = None,
        embedding_fn: Optional[Transformation] = None,
        update_embedding_fn: bool = True,
        to: Optional[str] = None,
        *args,
        **kwargs,
    ) -> DaftCollection:
        collection = self

        if to is None:
            to = f"embeddings_{column_name}"

        if content is None and column_name is None:
            raise ValueError("column_name or content must be specified!")

        if content is not None:
            collection = self.add_column(content, column_name)

        if embedding_fn is None:
            embedding_fn = self.embedding_functions[to]
        else:
            if to in self.embedding_functions:
                if embedding_fn != self.embedding_functions[to]:
                    print("embedding_fn may not be the same as whats in map!")
                if update_embedding_fn:
                    self.embedding_functions[to] = embedding_fn
            else:
                self.embedding_functions[to] = embedding_fn

        if getattr(self.embedding_functions[to], "__vexpresso_transform", None) is None:
            self.embedding_functions[to] = transformation(self.embedding_functions[to])

        args = [self[column_name], *args]

        return collection.apply(
            self.embedding_functions[to],
            *args,
            to=to,
            **kwargs,
        )

    def save_local(self, directory: str) -> str:
        os.makedirs(directory, exist_ok=True)
        table = self.df.to_arrow()
        pq.write_table(table, os.path.join(directory, "content.parquet"))

    @classmethod
    def from_local_dir(cls, local_dir: str, *args, **kwargs) -> DaftCollection:
        df = daft.read_parquet(os.path.join(local_dir, "content.parquet"))
        return DaftCollection(daft_df=df, *args, **kwargs)

    @classmethod
    def connect(
        cls, address: str = None, cluster_kwargs: Dict[str, Any] = {}, *args, **kwargs
    ) -> DaftCollection:
        if address is None:
            addy = ray.init(**cluster_kwargs)
        else:
            addy = ray.init(address=address, **cluster_kwargs)
        daft.context.set_runner_ray(address=addy.address_info["address"])
        return DaftCollection(*args, **kwargs)

    def to_langchain(self, document_column: str, embeddings_column: str):
        from langchain.docstore.document import Document
        from langchain.vectorstores import VectorStore

        class VexpressoVectorStore(VectorStore):
            def __init__(self, collection: DaftCollection):
                self.collection = collection
                self.document_column = document_column
                self.embeddings_column = embeddings_column

            def add_texts(
                self,
                texts: Iterable[str],
                metadatas: Optional[List[dict]] = None,
                **kwargs: Any,
            ) -> List[str]:
                if metadatas is None:
                    metadatas = [{} for _ in range(len(texts))]

                combined = [
                    {self.document_column: t, **m} for t, m in zip(texts, metadatas)
                ]

                self.collection = self.collection.add_rows(combined)

            def similarity_search(
                self, query: str, k: int = 4, **kwargs: Any
            ) -> List[Document]:
                dictionary = self.collection.query(
                    self.embeddings_column, query=query, k=k, lazy=False, **kwargs
                ).to_dict()
                documents = dictionary[self.column]
                metadatas = {k: dictionary[k] for k in dictionary if k != self.column}

                out = []
                for i in range(len(documents)):
                    doc = documents[i]
                    d = {k: metadatas[k][i] for k in metadatas}
                    out.append(Document(doc, d))
                return out

        return VexpressoVectorStore(self)

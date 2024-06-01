import daft
import pathlib
from typing import Any, Dict, Optional

from vexpresso.collection import Collection
from vexpresso.daft import DaftCollection

COLLECTION_TYPES = {
    "daft": DaftCollection,
}

DEFAULT_COLLECTION_TYPE = "daft"
DAFT_READ_FROM_FILE_FN_MAP = {
    ".parquet": daft.read_parquet,
    ".csv": daft.read_csv,
    ".json": daft.read_json,
}

def _should_load(
    directory_or_repo_id: Optional[str] = None,
    hf_username: Optional[str] = None,
    repo_name: Optional[str] = None,
) -> bool:
    if directory_or_repo_id is None and hf_username is None and repo_name is None:
        return False
    return True


def create(
    *args,
    collection_type: Optional[str] = None,
    directory_or_repo_id: Optional[str] = None,
    token: Optional[str] = None,
    local_dir: Optional[str] = None,
    to_tmpdir: bool = False,
    hf_username: Optional[str] = None,
    repo_name: Optional[str] = None,
    hub_download_kwargs: Optional[Dict[str, Any]] = {},
    backend: str = "python",
    cluster_address: Optional[str] = None,
    cluster_kwargs: Dict[str, Any] = {},
    **kwargs,
) -> Collection:
    BACKEND_SET = {"python", "ray"}

    collection_class = COLLECTION_TYPES.get(
        collection_type, COLLECTION_TYPES[DEFAULT_COLLECTION_TYPE]
    )
    if backend not in BACKEND_SET:
        backend = "python"

    if backend == "ray":
        _ = collection_class.connect(cluster_address, cluster_kwargs)

    if _should_load(directory_or_repo_id, hf_username, repo_name):
        return collection_class.load(
            directory_or_repo_id=directory_or_repo_id,
            token=token,
            local_dir=local_dir,
            to_tmpdir=to_tmpdir,
            hf_username=hf_username,
            repo_name=repo_name,
            hub_download_kwargs=hub_download_kwargs,
            *args,
            **kwargs,
        )
    collection = collection_class(*args, **kwargs)
    return collection

def load_from_hf(
        repo_name: str,
        api_token: Optional[str] = None,
        hf_hub_download_kwargs: Optional[Dict[str, Any]] = {}
) -> Collection:
    pass

def load_from_file(file_path: str, *args, **kwargs) -> Collection:

    # Check file type and select daft loader
    extension: str = pathlib.Path(file_path).suffix
    if extension not in DAFT_READ_FROM_FILE_FN_MAP:
        raise ValueError(f"File type {extension} not supported")
    daft_loader = DAFT_READ_FROM_FILE_FN_MAP.get(extension)
    if "backend" in kwargs and kwargs["backend"] == "ray":
        kwargs.pop("backend")
        daft.context.set_runner_ray()
    df: daft.DataFrame = daft_loader(file_path)
    return DaftCollection(daft_df=df, *args, **kwargs)



def connect(
    collection_type: str = "daft",
    cluster_address: Optional[str] = None,
    cluster_kwargs: Dict[str, Any] = {},
    *args,
    **kwargs,
) -> Collection:
    return create(
        collection_type=collection_type,
        cluster_address=cluster_address,
        cluster_kwargs=cluster_kwargs,
        *args,
        **kwargs,
    )


create_collection = create

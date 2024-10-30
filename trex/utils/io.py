"""
I/O utilities for loading and saving data.
"""

from pathlib import Path
from typing import Union, Any, List, Optional
import yaml
import pandas as pd
import uproot
from tqdm import tqdm
import awkward as ak
from numpy.typing import ArrayLike
import re
import pickle
from loguru import logger


def oneshot_load_root(
    path: str,
    key: Optional[str] = None,
    tree: str = "DecayTree",
    branches: Optional[List[str]] = None,
    max_events: Optional[int] = None,
    library: str = "ak",
    cut: Optional[str] = None,
    **kwargs: Any,
) -> Any:
    """Load data from a ROOT file into a pandas DataFrame or awkward Array in one shot.

    Parameters
    ----------
    path : str
        Path to the ROOT file.
    key : str, optional
        Key to target directory containing the tree.
    tree : str, optional
        Name of the tree to load.
    branches : list of str, optional
        List of branches to load. If None, load all branches.
    max_events : int, optional
        Maximum number of events to load.
    library : str, optional
        Library to use for loading the data ('ak' for awkward arrays or 'pd' for pandas).
    cut : str, optional
        Cut to apply to the data.

    Returns
    -------
    Any
        Loaded data as pandas DataFrame or awkward array.
    """
    try:
        file = uproot.open(path)
    except Exception as e:
        raise RuntimeError(f"Failed to open ROOT file: {path}. Error: {e}")

    events = file[f"{key}/{tree}"] if key else file[tree]

    logger.info(f"Loading data from {path}, tree: {tree}")
    return events.arrays(
        library=library, expressions=branches, entry_stop=max_events, cut=cut, **kwargs
    )


def batched_load_root(
    path: str,
    key: Optional[str] = None,
    tree: str = "DecayTree",
    branches: Optional[List[str]] = None,
    cut: Optional[Union[str, List[str]]] = None,
    name: Optional[str] = None,
    max_events: Optional[int] = None,
    batch_size: str = "2 MB",
    library: str = "ak",
    **kwargs: Any,
) -> Any:
    """Load ROOT data in batches into pandas DataFrame or awkward Array iterating through batches."""
    try:
        file = uproot.open(path)
    except Exception as e:
        raise RuntimeError(f"Failed to open ROOT file: {path}. Error: {e}")

    events = file[f"{key}/{tree}"] if key else file[tree]
    bevs = events.num_entries_for(batch_size, branches, entry_stop=max_events, **kwargs)
    tevs = events.num_entries
    nits = round(tevs / bevs + 0.5)

    aggr = []

    for batch in tqdm(
        events.iterate(
            expressions=branches,
            cut=cut,
            library=library,
            entry_stop=max_events,
            step_size=batch_size,
            **kwargs,
        ),
        total=nits,
        ascii=True,
        desc="Batches loaded",
    ):
        aggr.append(batch)

    if library == "pd":
        df = pd.concat(aggr)
    elif library == "ak":
        df = ak.concatenate(aggr, axis=0)
    else:
        raise ValueError(
            "Unsupported library. Use 'pd' for pandas or 'ak' for awkward arrays."
        )

    if name:
        df.name = name

    logger.info(f"\nSUCCESS: Loaded {len(df)} entries")
    return df


def open_count(
    file_path: str,
    key: Optional[str] = None,
    tree_name: Optional[str] = None,
) -> int:
    """Open ROOT file and simply count the number of entries."""
    try:
        events = uproot.open(
            f"{file_path}:{key}/{tree_name}" if key else f"{file_path}:{tree_name}"
        )
        return events.num_entries
    except Exception as e:
        logger.error(f"Failed to open or count entries in {file_path}. Error: {e}")
        raise


def book_output_file(
    path: str,
    compression: uproot.compression.Compression = uproot.ZLIB(4),
) -> uproot.WritableDirectory:
    """Book the output file with default compression ZLIB(4)."""
    try:
        outpath = Path(path)
        outpath.parent.mkdir(parents=True, exist_ok=True)
        outfile = uproot.recreate(outpath.absolute(), compression=compression)
    except Exception as e:
        logger.error(f"Failed to create or open output file at {path}. Error: {e}")
        raise

    return outfile


def update_write_df(
    outfile: uproot.WritableDirectory,
    data: Union[pd.DataFrame, ak.Array],
    key: Optional[str] = None,
    treename: str = "DecayTree",
) -> None:
    """Update a ROOT file to include the new data."""
    try:
        if key:
            outfile[f"{key}/{treename}"] = data
            logger.info(
                f"Entries written to file {key}/{treename}: {outfile[f'{key}/{treename}'].num_entries}"
            )
        else:
            outfile[treename] = data
            logger.info(
                f"Entries written to file DecayTree: {outfile[treename].num_entries}"
            )
    except Exception as e:
        logger.error(f"Failed to write data to {outfile}. Error: {e}")
        raise


def write_df(
    data: Union[pd.DataFrame, ak.Array],
    path: str,
    key: Optional[str] = None,
    treename: str = "DecayTree",
) -> None:
    """Write anew (recreate) the DataFrame or Awkward array to a ROOT file."""
    outfile = book_output_file(path)
    update_write_df(outfile, data, key, treename)


def extract_sel_dict_branches(selection_dict: dict) -> List[str]:
    """Extract unique branches from selection strings in the provided dictionary."""
    branch_pattern = re.compile(r"\b[A-Za-z_]+\b(?=\s*[!<>=])")
    all_branches = set()

    for selection in selection_dict.values():
        branches = branch_pattern.findall(selection)
        all_branches.update(branches)

    return list(all_branches)


def load_hist(f: str):
    """Load boost_histogram/Hist from pickle file."""
    try:
        with open(f, "rb") as f_in:
            return pickle.load(f_in)
    except Exception as e:
        logger.error(f"Failed to load histogram from {f}. Error: {e}")
        raise

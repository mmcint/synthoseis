from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

import numpy as np


def _safe_var_name(name: str) -> str:
    name = name.strip()
    name = name.replace(".npy", "")
    name = re.sub(r"[^0-9A-Za-z_]+", "_", name)
    name = re.sub(r"_+", "_", name).strip("_")
    if not name:
        return "var"
    if name[0].isdigit():
        return f"v_{name}"
    return name


@dataclass(frozen=True)
class GridSpec:
    first_track: int = 2000
    first_bin: int = 1000
    delta_track_num: int = 1
    delta_bin_num: int = 1


class XRWriter:
    """
    Incremental writer that appends variables into a single Zarr store.

    Coordinates are shared across all volumes:
      - track, bin are integer survey indices
      - z_sample is integer sample index
      - twt_ms is derived from digi (ms/sample) if TIME-mode (current repo default)
    """

    def __init__(self, cfg: Any, store_path: str, grid: GridSpec | None = None) -> None:
        self.cfg = cfg
        self.store_path = store_path
        self.grid = grid or GridSpec()

        nx, ny, nz0 = int(cfg.cube_shape[0]), int(cfg.cube_shape[1]), int(cfg.cube_shape[2])
        nz = nz0 + int(getattr(cfg, "pad_samples", 0))
        self._coords: dict[str, np.ndarray] = {
            "iline": self.grid.first_track
            + np.arange(nx, dtype="int32") * self.grid.delta_track_num,
            "xline": self.grid.first_bin
            + np.arange(ny, dtype="int32") * self.grid.delta_bin_num,
            "z": np.arange(nz, dtype="int32"),
        }

        digi = float(getattr(cfg, "digi", 1.0))
        self._coords["twt_ms"] = self._coords["z"].astype("float32") * digi

        self._dataset_attrs = {
            "synthoseis_store_version": "1",
            "date_stamp": str(getattr(cfg, "date_stamp", "")),
            "cube_shape": tuple(getattr(cfg, "cube_shape", ())),
            "pad_samples": int(getattr(cfg, "pad_samples", 0)),
            "digi": digi,
            "first_track": self.grid.first_track,
            "first_bin": self.grid.first_bin,
            "delta_track_num": self.grid.delta_track_num,
            "delta_bin_num": self.grid.delta_bin_num,
        }

    def _default_dims_for(self, data: np.ndarray) -> tuple[str, ...]:
        if data.ndim == 2:
            return ("iline", "xline")
        if data.ndim == 3:
            return ("iline", "xline", "z")
        if data.ndim == 4:
            return ("angle", "iline", "xline", "z")
        raise ValueError(f"Unsupported data.ndim={data.ndim} for XRWriter")

    def _default_chunks_for(self, data: np.ndarray) -> tuple[int, ...]:
        if data.ndim == 2:
            nx, ny = data.shape
            return (min(128, nx), min(128, ny))
        if data.ndim == 3:
            nx, ny, nz = data.shape
            return (min(64, nx), min(64, ny), min(256, nz))
        if data.ndim == 4:
            na, nx, ny, nz = data.shape
            return (min(1, na), min(64, nx), min(64, ny), min(256, nz))
        raise ValueError(f"Unsupported data.ndim={data.ndim} for XRWriter")

    def write_var(
        self,
        name: str,
        data: np.ndarray,
        *,
        dims: tuple[str, ...] | None = None,
        coords: dict[str, np.ndarray] | None = None,
        attrs: dict[str, Any] | None = None,
        chunks: tuple[int, ...] | None = None,
    ) -> str:
        import xarray as xr
        import zarr

        var_name = _safe_var_name(name)
        dims = tuple(dims) if dims is not None else self._default_dims_for(data)

        # Pad along shared z dimension when callers drop pad_samples.
        if "z" in dims:
            z_axis = dims.index("z")
            z_target = int(self._coords["z"].shape[0])
            z_current = int(data.shape[z_axis])
            if z_current != z_target:
                if z_current > z_target:
                    data = np.take(data, indices=np.arange(z_target), axis=z_axis)
                else:
                    pad_width = [(0, 0)] * data.ndim
                    pad_width[z_axis] = (0, z_target - z_current)
                    if np.issubdtype(data.dtype, np.floating):
                        fill = np.nan
                    else:
                        fill = 0
                    data = np.pad(data, pad_width, mode="constant", constant_values=fill)

        merged_coords: dict[str, Any] = dict(self._coords)
        if coords:
            merged_coords.update(coords)

        da = xr.DataArray(
            data,
            dims=dims,
            coords={k: merged_coords[k] for k in dims if k in merged_coords},
        )
        # allow auxiliary coordinate variables not in dims (e.g. twt_ms for z_sample)
        if "z" in dims and "twt_ms" in merged_coords:
            da = da.assign_coords(twt_ms=("z", merged_coords["twt_ms"]))

        if attrs:
            da.attrs.update(attrs)

        if chunks is None:
            chunks = self._default_chunks_for(data)

        # Ensure store exists with dataset-level attrs, and delete any existing variable.
        root = zarr.open_group(self.store_path, mode="a")
        root.attrs.update(self._dataset_attrs)
        if var_name in root.array_keys() or var_name in root.group_keys():
            del root[var_name]

        ds = da.to_dataset(name=var_name)
        encoding = {var_name: {"chunks": chunks}}
        ds.to_zarr(self.store_path, mode="a", encoding=encoding)
        return var_name


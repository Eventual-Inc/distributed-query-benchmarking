from __future__ import annotations

import dataclasses
import contextlib
from datetime import datetime

DEFAULT_DAFT_VERSION = "0.2.6"


@dataclasses.dataclass(frozen=True)
class Config:
    framework: str
    s3_parquet_url: str
    ray_address: str
    num_attempts: int
    questions: list[int]
    dask_address: str
    timeout_s: int
    daft_pypi_version: str | None
    daft_wheel_uri: str | None

    @classmethod
    def from_args(cls, args):
        daft_version = args.daft_pypi_version
        if daft_version is None:
            daft_version = DEFAULT_DAFT_VERSION
        return cls(
            framework=args.framework,
            s3_parquet_url=args.s3_parquet_url,
            ray_address=args.ray_address,
            num_attempts=args.num_attempts,
            questions=args.questions,
            dask_address=args.dask_address,
            timeout_s=args.timeout_s,
            daft_pypi_version=daft_version,
            daft_wheel_uri=args.daft_wheel_uri,
        )


@dataclasses.dataclass(frozen=False)
class Metrics:
    start_dt: int
    walltime_s: float | None

    def _finished(self):
        self.walltime_s = (datetime.now() - self.start_dt).total_seconds()


@contextlib.contextmanager
def metrics():
    start = datetime.now()

    m = Metrics(start_dt=start, walltime_s=None)
    yield m
    m._finished()

import dataclasses
import contextlib
from datetime import datetime


@dataclasses.dataclass(frozen=True)
class Config:
    framework: str
    s3_parquet_url: str
    ray_address: str
    num_attempts: int
    questions: list[int]
    dask_address: str
    timeout_s: int

    @classmethod
    def from_args(cls, args):
        return cls(
            framework=args.framework,
            s3_parquet_url=args.s3_parquet_url,
            ray_address=args.ray_address,
            num_attempts=args.num_attempts,
            questions=args.questions,
            dask_address=args.dask_address,
            timeout_s=args.timeout_s,
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

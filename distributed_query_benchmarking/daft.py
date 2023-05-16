import subprocess
import os
import pathlib

current_dir = pathlib.Path(os.path.dirname(__file__))
PATH_TO_RAY_YAML = current_dir / "assets" / "ray_cluster_config.yaml"

def setup():
    print("Setting up Ray cluster...")
    subprocess.run(["ray", "up", str(PATH_TO_RAY_YAML)])

def teardown():
    print("Tearing down Ray cluster...")
    subprocess.run(["ray", "down", str(PATH_TO_RAY_YAML)])

def run():
    print("Submitting Daft benchmarking job to Ray cluster...")
    subprocess.run(["ray", "submit", str(PATH_TO_RAY_YAML), __file__])

###
# Use this file as a Ray Job entrypoint
###

if __name__ == "__main__":
    print("Starting Daft run...")

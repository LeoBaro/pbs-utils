import pytest
import argparse
from pathlib import Path
from subprocess import call
from as_pbs_job import (
    optional_args_map,
    required_args,
    _prepare_sh_script_for_pbs,
    _add_pbs_optional_args,
    _check_required_args
)

def test_required_pbs_params():
    pbs_args = {
        "job_name": "test_job",
        "ngpus": 4,
        "ncpus": 24
    }
    with pytest.raises(ValueError):
        _check_required_args(pbs_args)

def test_optional_pbs_params():
    pbs_args = {
        "job_name": "test_job",
        "queue": "gpu",
        "ngpus": 4,
        "ncpus": 24
    }
    _add_pbs_optional_args(pbs_args)
    assert set(pbs_args.keys()) == set(optional_args_map.keys()) | set(required_args)

def test_prepare_sh_script_for_pbs():
    pbs_args = {
        "job_name": "test_job",
        "merge_stdout_stderr": True,
        "queue": "gpu",
        "ngpus": 4,
        "ncpus": 24,
        "walltime_h": 72,
        "modules_to_load": ["proxy", "anaconda3"],
        "virtual_env_path": None,
        "anaconda_env_name": "test-env",
        "pbs_script_path": "./here",
        "submit": False
    }

    cli_args = argparse.Namespace(arg=42)
    def test_function(cli_args):
        print("arg:",cli_args.arg)
    cli_args = vars(cli_args)

    function_script_path = Path(__file__).absolute()

    sh_script_str = _prepare_sh_script_for_pbs(function_script_path, pbs_args, cli_args)

    ground_truth = f"""
#!/bin/bash
#PBS -N test_job
#PBS -j eo
#PBS -q gpu -l select=1:ngpus=4:ncpus=24,walltime=72:00:00
module load proxy
module load anaconda3
source activate test-env
python {function_script_path} --arg 42"""

    assert sh_script_str.split("\n") == ground_truth.split("\n")

def test_submission():
    py_script = """
import argparse
from pbs import pbs_job
def cli():
    parser = argparse.ArgumentParser()
    parser.add_argument("-a", "--arg", type=int, required=True, help="")
    return parser.parse_args()
@pbs_job({
    "job_name": "test_job",
    "merge_stdout_stderr": True,
    "queue": "gpu",
    "ngpus": 4,
    "ncpus": 4,
    "walltime_h": 72,
    "modules_to_load": ["anaconda3", "proxy"],
    "virtual_env_path": None,
    "anaconda_env_name": None,
    "pbs_script_path": None,
    "submit": False
})
def main(args):
    print("main", args.arg)
if __name__=='__main__':
    main(cli())
"""
    script_path = Path("/tmp") / "py_script.py"
    with open(script_path, "w") as ps:
        ps.write(py_script)
    call(f"python {script_path}", shell=True)
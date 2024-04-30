import time
import inspect
import argparse
from pathlib import Path
from subprocess import call

required_args = ["job_name", "queue", "ngpus", "ncpus"]
optional_args_map = dict(
    merge_stdout_stderr="oe",
    walltime_h=72,
    modules_to_load=[],
    virtual_env_path=None,
    anaconda_env_name=None,
    pbs_script_path=None,
    submit=True
)

def as_pbs_job(pbs_args: dict):
    # The return value of the decorator function must be a function used to wrap the function to be decorated.
    def wrap(func):
        def wrapped_func(cli_args):
            _cliargs_check(cli_args)
            _pbsargs_check(pbs_args)
            script_path = _get_script_path(func)
            cli_args      = _convert_argparse_namespace_to_dict(cli_args)
            sh_script_str = _prepare_sh_script_for_pbs(script_path, pbs_args, cli_args)
            file          = _save_sh_script_for_pbs(sh_script_str, pbs_args)
            if pbs_args["submit"]:
                _submit_to_pbs(file)
        return wrapped_func
    return wrap

def _cliargs_check(cli_args):
    try:
        assert isinstance(cli_args, argparse.Namespace)
    except:
        raise TypeError("This library works only with argparse.Namespace arguments!")

def _pbsargs_check(pbsargs):
    _check_required_args(pbsargs)
    _add_pbs_optional_args(pbsargs)
    _check_pbs_args_values(pbsargs)

def _check_required_args(pbsargs):
    missing_args = set(required_args) - set(pbsargs.keys())
    if len(missing_args) > 0:
        raise ValueError(f"The @pbs decorator is missing the following keys: {missing_args}")

def _add_pbs_optional_args(pbsargs):
    for key, def_val in optional_args_map.items():
        if key not in pbsargs:
            pbsargs[key] = def_val

def _check_pbs_args_values(pbsargs):
    pass

def _get_script_path(func) -> Path:
    return Path(inspect.getfile(func)).absolute()

def _convert_argparse_namespace_to_dict(namespace: argparse.Namespace) -> dict:
    return vars(namespace)

def _prepare_sh_script_for_pbs(function_path: Path, pbs_args: dict, cli_args: dict) -> Path:
    sh_script_str = _add_shebang()
    sh_script_str = _add_pbs_job_name(sh_script_str, pbs_args)
    sh_script_str = _add_stdout_stderr(sh_script_str, pbs_args)
    sh_script_str = _add_pbs_resources_selector(sh_script_str, pbs_args)
    sh_script_str = _add_modules_activation(sh_script_str, pbs_args)
    sh_script_str = _add_virtualenv_activation(sh_script_str, pbs_args)
    sh_script_str = _add_py_script_call(function_path, sh_script_str, cli_args)
    return sh_script_str

def _add_shebang():
    sh_script_str = "\n#!/bin/bash"
    return sh_script_str

def _add_pbs_job_name(sh_script_str: str, pbs_args: dict):
    sh_script_str += f"\n#PBS -N {pbs_args['job_name']}"
    return sh_script_str

def _add_stdout_stderr(sh_script_str: str, pbs_args: dict):
    if pbs_args['merge_stdout_stderr']:
        sh_script_str += f"\n#PBS -j eo"
    return sh_script_str
def _add_pbs_resources_selector(sh_script_str: str, pbs_args: dict):
    sh_script_str += f"\n#PBS -q {pbs_args['queue']} -l select=1:ngpus={pbs_args['ngpus']}:ncpus={pbs_args['ncpus']},walltime={pbs_args['walltime_h']}:00:00"
    return sh_script_str

def _add_modules_activation(sh_script_str: str, pbs_args: dict):
    for mod in pbs_args['modules_to_load']:
        sh_script_str += f"\nmodule load {mod}"
    return sh_script_str

def _add_virtualenv_activation(sh_script_str: str, pbs_args: dict):
    if pbs_args['virtual_env_path']:
        sh_script_str += f"\nsource {pbs_args['virtual_env_path']}/bin/activate"
    if pbs_args['anaconda_env_name']:
        sh_script_str += f"\nCONDA_BASE_DIR=$(dirname $(dirname \"$CONDA_EXE\"))"
        sh_script_str += f"\nsource \"$CONDA_BASE_DIR/etc/profile.d/conda.sh\""
        sh_script_str += f"\nsource activate {pbs_args['anaconda_env_name']}"
    return sh_script_str

def _add_py_script_call(function_path: Path, sh_script_str: str, cli_args: dict):
    sh_script_str += f"\npython {function_path} "
    for key,val in cli_args.items():
        sh_script_str += f"--{key} {val} ".strip()
    return sh_script_str

def _save_sh_script_for_pbs(sh_script_str: str, pbs_args: dict) -> Path:
    output_path = _generate_output_path(pbs_args)
    with open(str(output_path), "w") as pbs_s:
        pbs_s.write(sh_script_str)
    return output_path

def _generate_output_path(pbs_args: dict) -> Path:
    time_str = time.strftime("%Y%m%d-%H%M%S")
    filename = f"pbs_script_{pbs_args['job_name']}_{time_str}.sh"
    if pbs_args["pbs_script_path"] is not None:
        Path(pbs_args["pbs_script_path"]).mkdir(parents=True, exist_ok=True)
        return Path(pbs_args["pbs_script_path"])/ filename
    else:
        return Path("/tmp") / filename

def _submit_to_pbs(sh_script: Path):
    call(f"qsub {sh_script}", shell=True)

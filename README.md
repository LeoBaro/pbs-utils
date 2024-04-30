# pbs_utils

This library provides a Python decorator to easily submit a script to pbs.
The `main(args)` function must accept an object of type argparse.Namespace.
```
import argparse
from subprocess import check_output
from pbs_utils import as_pbs_job

def cli():
    parser = argparse.ArgumentParser()
    parser.add_argument("-a", "--arg", type=int, required=True)
    return parser.parse_args()

@as_pbs_job({
    "job_name": "test_job",
    "queue": "gpu",
    "ngpus": 1,
    "ncpus": 1,
    "anaconda_env_name": "my-gpu-env",
})
def main(args: argparse.Namespace):
    print("Arg:", args.arg)
    print(
        check_output(["nvidia-smi"])
    )
    print(
        check_output(["which", "python"])
    )

if __name__=='__main__':
    main(cli())
```
## Supported parameters
Required parameters:
* job_name
* queue
* ngpus
* ncpus

Optional parameters description: [here](pbs_utils/src/as_pbs_job.py#L8).
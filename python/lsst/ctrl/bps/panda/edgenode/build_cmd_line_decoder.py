#!/usr/bin/python
"""
The module is needed to decode the command line string sent from the BPS
plugin -> PanDA -> Edge node cluster management
-> Edge node -> Container. This file is not a part
of the BPS but a part of the payload wrapper.
It decodes the hexified command line.
"""
# import base64
import datetime
import logging
import os
import sys
import tarfile

from lsst.ctrl.bps.bps_utils import _create_execution_butler
from lsst.ctrl.bps.constants import DEFAULT_MEM_FMT, DEFAULT_MEM_UNIT
from lsst.ctrl.bps.drivers import prepare_driver
from lsst.ctrl.bps.panda.constants import PANDA_DEFAULT_MAX_COPY_WORKERS
from lsst.ctrl.bps.panda.utils import copy_files_for_distribution, get_idds_client
from lsst.resources import ResourcePath
from lsst.utils.timer import time_this

logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format="%(asctime)s\t%(threadName)s\t%(name)s\t%(levelname)s\t%(message)s",
)

_LOG = logging.getLogger(__name__)


def download_extract_archive(filename):
    """Download and extract the tarball from pandacache.

    Parameters
    ----------
    filename : `str`
        The filename to download.
    """
    archive_basename = os.path.basename(filename)
    target_dir = os.getcwd()
    full_output_filename = os.path.join(target_dir, archive_basename)

    if filename.startswith("https:"):
        panda_cache_url = os.path.dirname(os.path.dirname(filename))
        os.environ["PANDACACHE_URL"] = panda_cache_url
    elif "PANDACACHE_URL" not in os.environ and "PANDA_URL_SSL" in os.environ:
        os.environ["PANDACACHE_URL"] = os.environ["PANDA_URL_SSL"]
    panda_cache_url = os.environ.get("PANDACACHE_URL", None)
    print(f"PANDACACHE_URL: {panda_cache_url}")

    from pandaclient import Client

    attempt = 0
    max_attempts = 3
    done = False
    while attempt < max_attempts and not done:
        status, output = Client.getFile(archive_basename, output_path=full_output_filename)
        if status == 0:
            done = True
    print(f"Download archive file from pandacache status: {status}, output: {output}")
    if status != 0:
        raise RuntimeError("Failed to download archive file from pandacache")
    with tarfile.open(full_output_filename, "r:gz") as f:
        f.extractall(target_dir)
    print(f"Extract {full_output_filename} to {target_dir}")
    os.remove(full_output_filename)
    print(f"Remove {full_output_filename}")


def create_idds_workflow(config_file, compute_site):
    """Create pipeline workflow at remote site.

    Parameters
    ----------
    config_file : `str`
        Name of the configuration file.
    compute_site : `str`
        Name of the compute site.
    """
    _LOG.info("Starting building process")
    kwargs = {}
    if compute_site:
        kwargs["compute_site"] = compute_site
    with time_this(
        log=_LOG,
        level=logging.INFO,
        prefix=None,
        msg="Completed entire submission process",
        mem_usage=True,
        mem_unit=DEFAULT_MEM_UNIT,
        mem_fmt=DEFAULT_MEM_FMT,
    ):
        wms_workflow_config, wms_workflow = prepare_driver(config_file, **kwargs)
        _, when_create = wms_workflow_config.search(".executionButler.whenCreate")
        if when_create.upper() == "SUBMIT":
            _, execution_butler_dir = wms_workflow_config.search(".bps_defined.executionButlerDir")
            _LOG.info("Creating execution butler in '%s'", execution_butler_dir)
            with time_this(
                log=_LOG, level=logging.INFO, prefix=None, msg="Completed creating execution butler"
            ):
                _create_execution_butler(
                    wms_workflow_config,
                    wms_workflow_config["runQgraphFile"],
                    execution_butler_dir,
                    wms_workflow_config["submitPath"],
                )
    return wms_workflow_config, wms_workflow


# download the submission tarball
remote_filename = sys.argv[1]
download_extract_archive(remote_filename)

# request_id and signature are added by iDDS for build task
request_id = os.environ.get("IDDS_BUILD_REQUEST_ID", None)
signature = os.environ.get("IDDS_BUIL_SIGNATURE", None)
config_file = sys.argv[2]
sys_argv_length = len(sys.argv)
compute_site = sys.argv[3] if sys_argv_length > 3 else None

if request_id is None:
    print("IDDS_BUILD_REQUEST_ID is not defined.")
    sys.exit(-1)
if signature is None:
    print("IDDS_BUIL_SIGNATURE is not defined")
    sys.exit(-1)

print(f"INFO: start {datetime.datetime.utcnow()}")
print(f"INFO: config file: {config_file}")
print(f"INFO: compute site: {compute_site}")

current_dir = os.getcwd()

print(f"INFO: current dir: {current_dir}")

config, bps_workflow = create_idds_workflow(config_file, compute_site)
idds_workflow = bps_workflow.idds_client_workflow

_, max_copy_workers = config.search("maxCopyWorkers", opt={"default": PANDA_DEFAULT_MAX_COPY_WORKERS})
file_distribution_uri = ResourcePath(config["fileDistributionEndPoint"], forceDirectory=True)
copy_files_for_distribution(bps_workflow.files_to_pre_stage, file_distribution_uri, max_copy_workers)

idds_client = get_idds_client(config)
ret = idds_client.update_build_request(request_id, signature, idds_workflow)
print(f"update_build_request returns: {ret}")
sys.exit(ret[0])

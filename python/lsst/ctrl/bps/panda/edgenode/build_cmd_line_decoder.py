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

from lsst.ctrl.bps.constants import DEFAULT_MEM_FMT, DEFAULT_MEM_UNIT
from lsst.ctrl.bps.drivers import prepare_driver
from lsst.ctrl.bps.panda.constants import PANDA_DEFAULT_MAX_COPY_WORKERS
from lsst.ctrl.bps.panda.utils import copy_files_for_distribution, get_idds_client
from lsst.resources import ResourcePath
from lsst.utils.timer import time_this
from lsst.ctrl.bps.panda.utils import download_extract_archive

logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format="%(asctime)s\t%(threadName)s\t%(name)s\t%(levelname)s\t%(message)s",
)

_LOG = logging.getLogger(__name__)


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

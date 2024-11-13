# This file is part of ctrl_bps_panda.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This software is dual licensed under the GNU General Public License and also
# under a 3-clause BSD license. Recipients may choose which of these licenses
# to use; please see the files gpl-3.0.txt and/or bsd_license.txt,
# respectively.  If you choose the GPL option then the following text applies
# (but note that there is still no warranty even if you opt for BSD instead):
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

"""Utilities for bps PanDA plugin."""

__all__ = [
    "copy_files_for_distribution",
    "get_idds_client",
    "get_idds_result",
    "convert_exec_string_to_hex",
    "add_decoder_prefix",
]

import binascii
import concurrent.futures
import logging
import os
import tarfile
import uuid

import idds.common.utils as idds_utils
import pandaclient.idds_api
from idds.doma.workflowv2.domapandawork import DomaPanDAWork
from idds.doma.workflowv2.domatree import DomaTree
from idds.workflowv2.workflow import AndCondition
from idds.workflowv2.workflow import Workflow as IDDS_client_workflow
from lsst.ctrl.bps import BpsConfig, GenericWorkflow, GenericWorkflowJob
from lsst.ctrl.bps.panda.cmd_line_embedder import CommandLineEmbedder
from lsst.ctrl.bps.panda.constants import (
    PANDA_DEFAULT_CLOUD,
    PANDA_DEFAULT_CORE_COUNT,
    PANDA_DEFAULT_MAX_ATTEMPTS,
    PANDA_DEFAULT_MAX_JOBS_PER_TASK,
    PANDA_DEFAULT_MAX_PAYLOADS_PER_PANDA_JOB,
    PANDA_DEFAULT_MAX_WALLTIME,
    PANDA_DEFAULT_ORDER_ID_MAP_FILE,
    PANDA_DEFAULT_PRIORITY,
    PANDA_DEFAULT_PROCESSING_TYPE,
    PANDA_DEFAULT_PROD_SOURCE_LABEL,
    PANDA_DEFAULT_RSS,
    PANDA_DEFAULT_RSS_MAX,
    PANDA_DEFAULT_TASK_TYPE,
    PANDA_DEFAULT_VO,
)
from lsst.resources import ResourcePath

_LOG = logging.getLogger(__name__)


def copy_files_for_distribution(files_to_stage, file_distribution_uri, max_copy_workers):
    """Brings locally generated files into Cloud for further
    utilization them on the edge nodes.

    Parameters
    ----------
    files_to_stage : `dict` [`str`, `str`]
        Files which need to be copied to a workflow staging area.
    file_distribution_uri : `ResourcePath`
        Path on the edge node accessed storage,
        including access protocol, bucket name to place files.
    max_copy_workers : `int`
        Maximum number of workers for copying files.

    Raises
    ------
    RuntimeError
        Raised when error copying files to the distribution point.
    """
    files_to_copy = {}

    # In case there are folders we iterate over its content
    for local_pfn in files_to_stage.values():
        folder_name = os.path.basename(os.path.normpath(local_pfn))
        if os.path.isdir(local_pfn):
            folder_uri = file_distribution_uri.join(folder_name, forceDirectory=True)
            files_in_folder = ResourcePath.findFileResources([local_pfn])
            for file in files_in_folder:
                file_name = file.basename()
                files_to_copy[file] = folder_uri.join(file_name, forceDirectory=False)
        else:
            folder_uri = file_distribution_uri.join(folder_name, forceDirectory=False)
            files_to_copy[ResourcePath(local_pfn, forceDirectory=False)] = folder_uri

    copy_executor = concurrent.futures.ThreadPoolExecutor(max_workers=max_copy_workers)
    future_file_copy = []
    for src, trgt in files_to_copy.items():
        _LOG.debug("Staging %s to %s", src, trgt)
        # S3 clients explicitly instantiate here to overpass this
        # https://stackoverflow.com/questions/52820971/is-boto3-client-thread-safe
        trgt.exists()
        future_file_copy.append(copy_executor.submit(trgt.transfer_from, src, transfer="copy"))

    for future in concurrent.futures.as_completed(future_file_copy):
        if future.result() is not None:
            raise RuntimeError("Error of placing files to the distribution point")


def get_idds_client(config):
    """Get the idds client.

    Parameters
    ----------
    config : `lsst.ctrl.bps.BpsConfig`
        BPS configuration.

    Returns
    -------
    idds_client: `idds.client.clientmanager.ClientManager`
        The iDDS ClientManager object.
    """
    idds_server = None
    if isinstance(config, BpsConfig):
        _, idds_server = config.search("iddsServer", opt={"default": None})
    elif isinstance(config, dict) and "iddsServer" in config:
        idds_server = config["iddsServer"]
    # if idds_server is None, a default value on the panda relay service
    # will be used
    idds_client = pandaclient.idds_api.get_api(
        idds_utils.json_dumps, idds_host=idds_server, compress=True, manager=True
    )
    return idds_client


def get_idds_result(ret):
    """Parse the results returned from iDDS.

    Parameters
    ----------
    ret : `tuple` [`int`, `tuple` [`bool`, payload ]]
        The first part ``ret[0]`` is the status of PanDA relay service.
        The part of ``ret[1][0]`` is the status of iDDS service.
        The part of ``ret[1][1]`` is the returned payload.
        If ``ret[1][0]`` is `False`, ``ret[1][1]`` can be error messages.

    Returns
    -------
    status: `bool`
        The status of iDDS calls.
    result: `int` or `list` or `dict` or `None`
        The result returned from iDDS. `None` if error state.
    error: `str` or `None`
        Error messages. `None` if no error state.
    """
    # https://panda-wms.readthedocs.io/en/latest/client/rest_idds.html
    if not isinstance(ret, list | tuple) or ret[0] != 0:
        # Something wrong with the PanDA relay service.
        # The call may not be delivered to iDDS.
        status = False
        result = None
        error = f"PanDA relay service returns errors: {ret}"
    else:
        if ret[1][0]:
            status = True
            result = ret[1][1]
            error = None
            if isinstance(result, str) and "Authentication no permission" in result:
                status = False
                result = None
                error = result
        else:
            # iDDS returns errors
            status = False
            result = None
            error = f"iDDS returns errors: {ret[1][1]}"
    return status, result, error


def _make_pseudo_filename(config, gwjob):
    """Make the job pseudo filename.

    Parameters
    ----------
    config : `lsst.ctrl.bps.BpsConfig`
        BPS configuration.
    gwjob : `lsst.ctrl.bps.GenericWorkflowJob`
        Job for which to create the pseudo filename.

    Returns
    -------
    pseudo_filename : `str`
        The pseudo filename for the given job.
    """
    cmd_line_embedder = CommandLineEmbedder(config)
    _, pseudo_filename = cmd_line_embedder.substitute_command_line(
        gwjob.executable.src_uri + " " + gwjob.arguments, gwjob.cmdvals, gwjob.name, []
    )
    return pseudo_filename


def _make_doma_work(
    config,
    generic_workflow,
    gwjob,
    task_count,
    task_chunk,
    enable_event_service=False,
    es_files={},
    es_label=None,
    max_payloads_per_panda_job=PANDA_DEFAULT_MAX_PAYLOADS_PER_PANDA_JOB,
    max_wms_job_wall_time=None,
    remote_filename=None,
):
    """Make the DOMA Work object for a PanDA task.

    Parameters
    ----------
    config : `lsst.ctrl.bps.BpsConfig`
        BPS configuration.
    gwjob : `lsst.ctrl.bps.GenericWorkflowJob`
        Job representing the jobs for the PanDA task.
    task_count : `int`
        Count of PanDA tasks used when making unique names.
    task_chunk : `int`
        Count of chunk of a PanDA tasks used when making unique names.

    Returns
    -------
    work : `idds.doma.workflowv2.domapandawork.DomaPanDAWork`
        The client representation of a PanDA task.
    local_pfns : `dict` [`str`, `str`]
        Files which need to be copied to a workflow staging area.
    """
    _LOG.debug("Using gwjob %s to create new PanDA task (gwjob=%s)", gwjob.name, gwjob)
    cvals = {"curr_cluster": gwjob.label}
    _, site = config.search("computeSite", opt={"curvals": cvals, "required": True})
    cvals["curr_site"] = site
    _, processing_type = config.search(
        "processing_type", opt={"curvals": cvals, "default": PANDA_DEFAULT_PROCESSING_TYPE}
    )
    _, task_type = config.search("taskType", opt={"curvals": cvals, "default": PANDA_DEFAULT_TASK_TYPE})
    _, prod_source_label = config.search(
        "prodSourceLabel", opt={"curvals": cvals, "default": PANDA_DEFAULT_PROD_SOURCE_LABEL}
    )
    _, vo = config.search("vo", opt={"curvals": cvals, "default": PANDA_DEFAULT_VO})

    _, file_distribution_end_point = config.search(
        "fileDistributionEndPoint", opt={"curvals": cvals, "default": None}
    )

    _, file_distribution_end_point_default = config.search(
        "fileDistributionEndPointDefault", opt={"curvals": cvals, "default": None}
    )

    task_rss = gwjob.request_memory if gwjob.request_memory else PANDA_DEFAULT_RSS
    task_rss_retry_step = task_rss * gwjob.memory_multiplier if gwjob.memory_multiplier else 0
    task_rss_retry_offset = 0 if task_rss_retry_step else task_rss

    # Assume input files are same across task
    local_pfns = {}
    direct_io_files = set()

    if gwjob.executable.transfer_executable:
        local_pfns["job_executable"] = gwjob.executable.src_uri
        job_executable = f"./{os.path.basename(gwjob.executable.src_uri)}"
    else:
        job_executable = gwjob.executable.src_uri
    cmd_line_embedder = CommandLineEmbedder(config)
    _LOG.debug(
        "job %s inputs = %s, outputs = %s",
        gwjob.name,
        generic_workflow.get_job_inputs(gwjob.name),
        generic_workflow.get_job_outputs(gwjob.name),
    )

    cmd_line, _ = cmd_line_embedder.substitute_command_line(
        job_executable + " " + gwjob.arguments,
        gwjob.cmdvals,
        gwjob.name,
        generic_workflow.get_job_inputs(gwjob.name) + generic_workflow.get_job_outputs(gwjob.name),
    )

    my_log = f"enable_event_service {enable_event_service} for {gwjob.label}"
    _LOG.info(my_log)
    if enable_event_service:
        if gwjob.request_walltime and max_wms_job_wall_time:
            my_log = (
                f"requestWalltime({gwjob.request_walltime}) "
                f"and maxWmsJobWalltime({max_wms_job_wall_time}) are set, "
                "max_payloads_per_panda_job is int(max_wms_job_wall_time / gwjob.request_walltime), "
                "ignore maxPayloadsPerPandaJob."
            )
            _LOG.info(my_log)
            max_payloads_per_panda_job = int(max_wms_job_wall_time / gwjob.request_walltime)
            if max_payloads_per_panda_job < 2:
                my_log = (
                    f"max_payloads_per_panda_job ({max_payloads_per_panda_job}) is too small, "
                    "disable EventService"
                )
                _LOG.info(my_log)
                enable_event_service = False

    maxwalltime = gwjob.request_walltime if gwjob.request_walltime else PANDA_DEFAULT_MAX_WALLTIME
    if enable_event_service:
        for es_name in es_files:
            local_pfns[es_name] = es_files[es_name]
        if gwjob.request_walltime and max_payloads_per_panda_job:
            maxwalltime = gwjob.request_walltime * max_payloads_per_panda_job
        elif max_wms_job_wall_time:
            maxwalltime = max_wms_job_wall_time

    for gwfile in generic_workflow.get_job_inputs(gwjob.name, transfer_only=True):
        local_pfns[gwfile.name] = gwfile.src_uri
        if os.path.isdir(gwfile.src_uri):
            # this is needed to make isdir function working
            # properly in ButlerURL instance on the edge node
            local_pfns[gwfile.name] += "/"

        if gwfile.job_access_remote:
            direct_io_files.add(gwfile.name)

    _, submit_cmd = config.search("submitCmd", opt={"default": False})

    if not direct_io_files:
        if submit_cmd:
            direct_io_files.add(remote_filename)
        else:
            direct_io_files.add("cmdlineplaceholder")

    lsst_temp = "LSST_RUN_TEMP_SPACE"
    if lsst_temp in file_distribution_end_point and lsst_temp not in os.environ:
        file_distribution_end_point = file_distribution_end_point_default
    if submit_cmd and not file_distribution_end_point:
        file_distribution_end_point = "FileDistribution"

    executable = add_decoder_prefix(
        config, cmd_line, file_distribution_end_point, (local_pfns, direct_io_files)
    )
    work = DomaPanDAWork(
        executable=executable,
        primary_input_collection={
            "scope": "pseudo_dataset",
            "name": f"pseudo_input_collection#{task_count}",
        },
        output_collections=[{"scope": "pseudo_dataset", "name": f"pseudo_output_collection#{task_count}"}],
        log_collections=[],
        dependency_map=[],
        task_name=f"{generic_workflow.name}_{task_count:02d}_{gwjob.label}_{task_chunk:02d}",
        task_queue=gwjob.queue,
        task_log={
            "destination": "local",
            "value": "log.tgz",
            "dataset": "PandaJob_#{pandaid}/",
            "token": "local",
            "param_type": "log",
            "type": "template",
        },
        encode_command_line=True,
        task_rss=task_rss,
        task_rss_retry_offset=task_rss_retry_offset,
        task_rss_retry_step=task_rss_retry_step,
        task_rss_max=gwjob.request_memory_max if gwjob.request_memory_max else PANDA_DEFAULT_RSS_MAX,
        task_cloud=gwjob.compute_cloud if gwjob.compute_cloud else PANDA_DEFAULT_CLOUD,
        task_site=site,
        task_priority=int(gwjob.priority) if gwjob.priority else PANDA_DEFAULT_PRIORITY,
        core_count=gwjob.request_cpus if gwjob.request_cpus else PANDA_DEFAULT_CORE_COUNT,
        working_group=gwjob.accounting_group,
        processing_type=processing_type,
        task_type=task_type,
        prodSourceLabel=prod_source_label,
        vo=vo,
        es=enable_event_service,
        es_label=es_label,
        max_events_per_job=max_payloads_per_panda_job,
        maxattempt=gwjob.number_of_retries if gwjob.number_of_retries else PANDA_DEFAULT_MAX_ATTEMPTS,
        maxwalltime=maxwalltime,
    )
    return work, local_pfns


def add_final_idds_work(
    config, generic_workflow, idds_client_workflow, dag_sink_work, task_count, task_chunk
):
    """Add the special final PanDA task to the client workflow.

    Parameters
    ----------
    config : `lsst.ctrl.bps.BpsConfig`
        BPS configuration.
    generic_workflow : `lsst.ctrl.bps.GenericWorkflow`
        Generic workflow in which to find the final job.
    idds_client_workflow : `idds.workflowv2.workflow.Workflow`
        The iDDS client representation of the workflow to which the final task
        is added.
    dag_sink_work : `list` [`idds.doma.workflowv2.domapandawork.DomaPanDAWork`]
        The work nodes in the client workflow which have no successors.
    task_count : `int`
        Count of PanDA tasks used when making unique names.
    task_chunk : `int`
        Count of chunk of a PanDA tasks used when making unique names.

    Returns
    -------
    files : `dict` [`str`, `str`]
        Files which need to be copied to a workflow staging area.

    Raises
    ------
    NotImplementedError
        Raised if final job in GenericWorkflow is itself a workflow.
    TypeError
        Raised if final job in GenericWorkflow is invalid type.
    """
    files = {}

    # If final job exists in generic workflow, create DAG final job
    final = generic_workflow.get_final()
    if final:
        if isinstance(final, GenericWorkflow):
            raise NotImplementedError("PanDA plugin does not support a workflow as the final job")

        if not isinstance(final, GenericWorkflowJob):
            raise TypeError(f"Invalid type for GenericWorkflow.get_final() results ({type(final)})")

        dag_final_work, files = _make_doma_work(
            config,
            generic_workflow,
            final,
            task_count,
            task_chunk,
        )
        pseudo_filename = "pure_pseudoinput+qgraphNodeId:+qgraphId:"
        dag_final_work.dependency_map.append(
            {"name": pseudo_filename, "submitted": False, "dependencies": []}
        )
        idds_client_workflow.add_work(dag_final_work)
        conditions = []
        for work in dag_sink_work:
            conditions.append(work.is_terminated)
        and_cond = AndCondition(conditions=conditions, true_works=[dag_final_work])
        idds_client_workflow.add_condition(and_cond)
    else:
        _LOG.debug("No final job in GenericWorkflow")
    return files


def convert_exec_string_to_hex(cmdline):
    """Convert the command line into hex representation.

    This step is currently involved because large blocks of command lines
    including special symbols passed to the pilot/container. To make sure
    the 1 to 1 matching and pass by the special symbol stripping
    performed by the Pilot we applied the hexing.

    Parameters
    ----------
    cmdline : `str`
        UTF-8 command line string.

    Returns
    -------
    hex : `str`
        Hex representation of string.
    """
    return binascii.hexlify(cmdline.encode()).decode("utf-8")


def add_decoder_prefix(config, cmd_line, distribution_path, files):
    """Compose the command line sent to the pilot from the functional part
    (the actual SW running) and the middleware part (containers invocation).

    Parameters
    ----------
    config : `lsst.ctrl.bps.BpsConfig`
        Configuration information.
    cmd_line : `str`
        UTF-8 based functional part of the command line.
    distribution_path : `str`
        URI of path where all files are located for distribution.
    files : `tuple` [`dict` [`str`, `str`], `list` [`str`]]
        File names needed for a task (copied local, direct access).

    Returns
    -------
    decoder_prefix : `str`
        Full command line to be executed on the edge node.
    """
    # Manipulate file paths for placement on cmdline
    files_plc_hldr = {}
    for key, pfn in files[0].items():
        if pfn.endswith("/"):
            files_plc_hldr[key] = os.path.basename(pfn[:-1])
            isdir = True
        else:
            files_plc_hldr[key] = os.path.basename(pfn)
            _, extension = os.path.splitext(pfn)
            isdir = os.path.isdir(pfn) or (key == "butlerConfig" and extension != "yaml")
        if isdir:
            # this is needed to make isdir function working
            # properly in ButlerURL instance on the egde node
            files_plc_hldr[key] += "/"
        _LOG.debug("files_plc_hldr[%s] = %s", key, files_plc_hldr[key])

    cmdline_hex = convert_exec_string_to_hex(cmd_line)
    _, runner_command = config.search("runnerCommand", opt={"replaceEnvVars": False, "expandEnvVars": False})
    order_id_map_filename = files[0].get("orderIdMapFilename", None)
    if order_id_map_filename:
        order_id_map_filename = os.path.basename(order_id_map_filename)
        order_id_map_filename = os.path.join(distribution_path, order_id_map_filename)
        runner_command = runner_command.replace("orderIdMapFilename", order_id_map_filename)
    runner_command = runner_command.replace("\n", " ")
    decoder_prefix = runner_command.replace(
        "_cmd_line_",
        str(cmdline_hex)
        + " ${IN/L} "
        + distribution_path
        + "  "
        + "+".join(f"{k}:{v}" for k, v in files_plc_hldr.items())
        + " "
        + "+".join(files[1]),
    )
    return decoder_prefix


def add_idds_work(config, generic_workflow, idds_workflow):
    """Convert GenericWorkflowJobs to iDDS work and add them to the iDDS
        workflow.

    Parameters
    ----------
    config : `lsst.ctrl.bps.BpsConfig`
        BPS configuration.
    generic_workflow : `lsst.ctrl.bps.GenericWorkflow`
        Generic workflow containing jobs to convert.
    idds_workflow : `idds.workflowv2.workflow.Workflow`
        The iDDS workflow to which the converted jobs should be added.

    Returns
    -------
    files_to_pre_stage : `dict` [`str`, `str`]
        Files that need to be copied to the staging area before submission.
    dag_sink_work : `list` [`idds.doma.workflowv2.domapandawork.DomaPanDAWork`]
        The work nodes in the client workflow which have no successors.
    task_count : `int`
        Number of tasks in iDDS workflow used for unique task names.

    Raises
    ------
    RuntimeError
        If cannot recover from dependency issues after pass through workflow.
    """
    # custom job
    _, submit_cmd = config.search("submitCmd", opt={"default": False})

    # event service
    _, enable_event_service = config.search("enableEventService", opt={"default": None})
    _, max_payloads_per_panda_job = config.search(
        "maxPayloadsPerPandaJob", opt={"default": PANDA_DEFAULT_MAX_PAYLOADS_PER_PANDA_JOB}
    )
    _, max_wms_job_wall_time = config.search("maxWmsJobWalltime", opt={"default": None})
    my_log = (
        f"enableEventService: {enable_event_service}, "
        f"maxPayloadsPerPandaJob: {max_payloads_per_panda_job}"
    )
    _LOG.info(my_log)

    # Limit number of jobs in single PanDA task
    _, max_jobs_per_task = config.search("maxJobsPerTask", opt={"default": PANDA_DEFAULT_MAX_JOBS_PER_TASK})

    files_to_pre_stage = {}
    dag_sink_work = []  # Workflow sink nodes that need to be connected to final task
    job_to_task = {}
    job_to_pseudo_filename = {}
    task_count = 0  # Task number/ID in idds workflow used for unique name
    remote_filename = None

    if submit_cmd:
        files = []
        _, script = config["customJob"].search("executable", opt={"default": ""})
        files.append(script)
        submit_path = config["submitPath"]
        archive_filename = f"jobO.{uuid.uuid4()}.tar.gz"
        archive_filename = create_archive_file(submit_path, archive_filename, files)
        remote_filename = copy_files_to_pandacache(archive_filename)

    es_files = {}
    name_works = {}
    order_id_map = {}
    doma_tree = None
    order_id_map_file = None
    if enable_event_service:
        enable_event_service = enable_event_service.split(",")
        enable_event_service = [i.strip() for i in enable_event_service]
        doma_tree = DomaTree(name=generic_workflow.name)
        submit_path = config[".bps_defined.submitPath"]
        _, order_id_map_filename = config.search(
            "orderIdMapFilename", opt={"default": PANDA_DEFAULT_ORDER_ID_MAP_FILE}
        )
        order_id_map_file = os.path.join(submit_path, order_id_map_filename)
        order_id_map = doma_tree.order_jobs_from_generic_workflow(generic_workflow, order_id_map_file)
        es_files = {"orderIdMapFilename": order_id_map_file}
        files_to_pre_stage.update(es_files)

    # To avoid dying due to optimizing number of times through workflow,
    # catch dependency issues to loop through again later.
    jobs_with_dependency_issues = {}

    # Assume jobs with same label share config values
    for job_label in generic_workflow.labels:
        _LOG.debug("job_label = %s", job_label)
        # Add each job with a particular label to a corresponding PanDA task
        # A PanDA task has a limit on number of jobs, so break into multiple
        # PanDA tasks if needed.
        job_count = 0  # Number of jobs in idds task used for task chunking
        task_chunk = 1  # Task chunk number within job label used for unique name
        work = None

        # Instead of changing code to make chunks up front and round-robin
        # assign jobs to chunks, for now keeping chunk creation in loop
        # but using knowledge of how many chunks there will be to set better
        # maximum number of jobs in a chunk for more even distribution.
        jobs_by_label = generic_workflow.get_jobs_by_label(job_label)
        num_chunks = -(-len(jobs_by_label) // max_jobs_per_task)  # ceil
        max_jobs_per_task_this_label = -(-len(jobs_by_label) // num_chunks)
        _LOG.debug(
            "For job_label = %s, num jobs = %s, num_chunks = %s, max_jobs = %s",
            job_label,
            len(jobs_by_label),
            num_chunks,
            max_jobs_per_task_this_label,
        )
        for gwjob in jobs_by_label:
            job_count += 1
            if job_count > max_jobs_per_task_this_label:
                job_count = 1
                task_chunk += 1

            order_id = job_count
            job_order_id = gwjob.attrs.get("order_id", None)
            if job_order_id is not None:
                order_id = job_order_id

            if job_count == 1:
                # Create new PanDA task object
                task_count += 1
                work_enable_event_service = False
                if enable_event_service and job_label in enable_event_service:
                    work_enable_event_service = True
                work, files = _make_doma_work(
                    config,
                    generic_workflow,
                    gwjob,
                    task_count,
                    task_chunk,
                    enable_event_service=work_enable_event_service,
                    es_files=es_files,
                    es_label=job_label,
                    max_payloads_per_panda_job=max_payloads_per_panda_job,
                    max_wms_job_wall_time=max_wms_job_wall_time,
                    remote_filename=remote_filename,
                )
                name_works[work.task_name] = work
                files_to_pre_stage.update(files)
                idds_workflow.add_work(work)
                if generic_workflow.out_degree(gwjob.name) == 0:
                    dag_sink_work.append(work)

            pseudo_filename = _make_pseudo_filename(config, gwjob)
            job_to_pseudo_filename[gwjob.name] = pseudo_filename
            job_to_task[gwjob.name] = work.get_work_name()
            deps = []
            missing_deps = False
            for parent_job_name in generic_workflow.predecessors(gwjob.name):
                if parent_job_name not in job_to_task:
                    _LOG.debug("job_to_task.keys() = %s", job_to_task.keys())
                    missing_deps = True
                    break
                else:
                    deps.append(
                        {
                            "task": job_to_task[parent_job_name],
                            "inputname": job_to_pseudo_filename[parent_job_name],
                            "available": False,
                        }
                    )
            if not missing_deps:
                work.dependency_map.append(
                    {"name": pseudo_filename, "order_id": order_id, "dependencies": deps}
                )
            else:
                jobs_with_dependency_issues[gwjob.name] = {"work": work, "order_id": order_id}

    # If there were any issues figuring out dependencies through earlier loop
    if jobs_with_dependency_issues:
        _LOG.warning("Could not prepare workflow in single pass.  Please notify developers.")
        _LOG.info("Trying to recover...")
        for job_name, work_item in jobs_with_dependency_issues.items():
            deps = []
            work = work_item["work"]
            order_id = work_item["order_id"]

            for parent_job_name in generic_workflow.predecessors(job_name):
                if parent_job_name not in job_to_task:
                    _LOG.debug("job_to_task.keys() = %s", job_to_task.keys())
                    raise RuntimeError(
                        "Could not recover from dependency issues ({job_name} missing {parent_job_name})."
                    )
                deps.append(
                    {
                        "task": job_to_task[parent_job_name],
                        "inputname": job_to_pseudo_filename[parent_job_name],
                        "available": False,
                    }
                )
            pseudo_filename = job_to_pseudo_filename[job_name]
            work.dependency_map.append({"name": pseudo_filename, "order_id": order_id, "dependencies": deps})
        _LOG.info("Successfully recovered.")

    for task_name in name_works:
        work = name_works[task_name]
        # trigger the setter function which will validate the dependency_map:
        # 1) check the name length to avoid the the name too long,
        # 2) check to avoid duplicated items.
        work.dependency_map = work.dependency_map

    if enable_event_service:
        for label_name in order_id_map:
            for order_id in order_id_map[label_name]:
                job_name = order_id_map[label_name][order_id]
                if job_name in job_to_pseudo_filename:
                    order_id_map[label_name][order_id] = job_to_pseudo_filename[job_name]
        doma_tree.save_order_id_map(order_id_map, order_id_map_file)

    return files_to_pre_stage, dag_sink_work, task_count


def create_archive_file(submit_path, archive_filename, files):
    if not archive_filename.startswith("/"):
        archive_filename = os.path.join(submit_path, archive_filename)

    with tarfile.open(archive_filename, "w:gz", dereference=True) as tar:
        for local_file in files:
            base_name = os.path.basename(local_file)
            tar.add(local_file, arcname=os.path.basename(base_name))
    return archive_filename


def copy_files_to_pandacache(filename):
    from pandaclient import Client

    attempt = 0
    max_attempts = 3
    done = False
    while attempt < max_attempts and not done:
        status, out = Client.putFile(filename, True)
        if status == 0:
            done = True
    print(f"copy_files_to_pandacache: status: {status}, out: {out}")
    if out.startswith("NewFileName:"):
        # found the same input sandbox to reuse
        filename = out.split(":")[-1]
    elif out != "True":
        print(out)
        return None

    filename = os.path.basename(filename)
    cache_path = os.path.join(os.environ["PANDACACHE_URL"], "cache")
    filename = os.path.join(cache_path, filename)
    return filename

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

def get_task_parameter(config, remote_build, key):
    search_opt = {"replaceVars": True, "expandEnvVars": False, "replaceEnvVars": False, "required": False}
    _, value = remote_build.search(key, search_opt)
    if not value:
        _, value = config.search(key, search_opt)
    return value


def create_idds_build_workflow(**kwargs):
    config = kwargs["config"] if "config" in kwargs else None
    remote_build = kwargs["remote_build"] if "remote_build" in kwargs else None
    config_file = kwargs["config_file"] if "config_file" in kwargs else None
    config_file_base = os.path.basename(config_file) if config_file else None
    compute_site = kwargs["compute_site"] if "compute_site" in kwargs else None
    _, files = remote_build.search("files", opt={"default": []})
    submit_path = config["submitPath"]
    files.append(config_file)
    archive_filename = f"jobO.{uuid.uuid4()}.tar.gz"
    archive_filename = create_archive_file(submit_path, archive_filename, files)
    _LOG.info(f"archive file name: {archive_filename}")
    remote_filename = copy_files_to_pandacache(archive_filename)
    _LOG.info(f"pandacache file: {remote_filename}")

    _LOG.info(type(remote_build))
    search_opt = {"replaceVars": True, "expandEnvVars": False, "replaceEnvVars": False, "required": False}
    cvals = {"LSST_VERSION": get_task_parameter(config, remote_build, "LSST_VERSION")}
    cvals["custom_lsst_setup"] = get_task_parameter(config, remote_build, "custom_lsst_setup")
    search_opt["curvals"] = cvals
    _, executable = remote_build.search("runnerCommand", opt=search_opt)
    executable = executable.replace("_download_cmd_line_", remote_filename)
    executable = executable.replace("_build_cmd_line_", config_file_base)
    executable = executable.replace("_compute_site_", compute_site or "")

    task_cloud = get_task_parameter(config, remote_build, "computeCloud")
    task_site = get_task_parameter(config, remote_build, "computeSite")
    task_queue = get_task_parameter(config, remote_build, "queue")
    task_rss = get_task_parameter(config, remote_build, "requestMemory")
    nretries = get_task_parameter(config, remote_build, "numberOfRetries")
    _LOG.info("requestMemory: %s", task_rss)
    _LOG.info("Site: %s", task_site)
    # _LOG.info("executable: %s", executable)
    # TODO: fill other parameters based on config
    build_work = DomaPanDAWork(
        executable=executable,
        task_type="lsst_build",
        primary_input_collection={"scope": "pseudo_dataset", "name": "pseudo_input_collection#1"},
        output_collections=[{"scope": "pseudo_dataset", "name": "pseudo_output_collection#1"}],
        log_collections=[],
        dependency_map=None,
        task_name="build_task",
        task_queue=task_queue,
        encode_command_line=True,
        prodSourceLabel="managed",
        task_log={
            "dataset": "PandaJob_#{pandaid}/",
            "destination": "local",
            "param_type": "log",
            "token": "local",
            "type": "template",
            "value": "log.tgz",
        },
        task_rss=task_rss if task_rss else PANDA_DEFAULT_RSS,
        task_cloud=task_cloud,
        task_site=task_site,
        maxattempt=nretries if nretries > 0 else PANDA_DEFAULT_MAX_ATTEMPTS,
    )

    workflow = IDDS_client_workflow()

    workflow.add_work(build_work)
    workflow.name = config["bps_defined"]["uniqProcName"]
    return workflow

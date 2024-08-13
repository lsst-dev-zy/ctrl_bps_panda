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
"""Interface between generic workflow to PanDA/iDDS workflow system.
"""


__all__ = ["PanDAService", "PandaBpsWmsWorkflow"]


import json
import logging
import os
import pickle
import re

from idds.workflowv2.workflow import Workflow as IDDS_client_workflow
from lsst.ctrl.bps import BaseWmsService, BaseWmsWorkflow, WmsRunReport, WmsStates
from lsst.ctrl.bps.panda.constants import PANDA_DEFAULT_MAX_COPY_WORKERS
from lsst.ctrl.bps.panda.utils import (
    add_final_idds_work,
    add_idds_work,
    copy_files_for_distribution,
    create_idds_build_workflow,
    get_idds_client,
    get_idds_result,
)
from lsst.resources import ResourcePath

_LOG = logging.getLogger(__name__)


class PanDAService(BaseWmsService):
    """PanDA version of WMS service."""

    def prepare(self, config, generic_workflow, out_prefix=None):
        # Docstring inherited from BaseWmsService.prepare.
        _LOG.debug("out_prefix = '%s'", out_prefix)
        workflow = PandaBpsWmsWorkflow.from_generic_workflow(
            config, generic_workflow, out_prefix, f"{self.__class__.__module__}.{self.__class__.__name__}"
        )
        workflow.write(out_prefix)
        return workflow

    def submit(self, workflow, **kwargs):
        config = kwargs["config"] if "config" in kwargs else None
        remote_build = kwargs["remote_build"] if "remote_build" in kwargs else None

        if config and remote_build:
            _LOG.info("remote build")

            idds_build_workflow = create_idds_build_workflow(**kwargs)
            idds_client = get_idds_client(self.config)
            ret = idds_client.submit_build(idds_build_workflow, username=None, use_dataset_name=False)
            _LOG.debug("iDDS client manager submit returned = %s", ret)

            # Check submission success
            status, result, error = get_idds_result(ret)
            if status:
                request_id = int(result)
            else:
                raise RuntimeError(f"Error submitting to PanDA service: {error}")

            _LOG.info("Submitted into iDDs with request id=%s", request_id)
            idds_build_workflow.run_id = request_id
            return idds_build_workflow
        else:
            _, max_copy_workers = self.config.search(
                "maxCopyWorkers", opt={"default": PANDA_DEFAULT_MAX_COPY_WORKERS}
            )
            # Docstring inherited from BaseWmsService.submit.
            file_distribution_uri = self.config["fileDistributionEndPoint"]
            lsst_temp = "LSST_RUN_TEMP_SPACE"
            if lsst_temp in file_distribution_uri and lsst_temp not in os.environ:
                file_distribution_uri = self.config["fileDistributionEndPointDefault"]
            copy_files_for_distribution(
                workflow.files_to_pre_stage,
                ResourcePath(file_distribution_uri, forceDirectory=True),
                max_copy_workers,
            )

            idds_client = get_idds_client(self.config)
            ret = idds_client.submit(workflow.idds_client_workflow, username=None, use_dataset_name=False)
            _LOG.debug("iDDS client manager submit returned = %s", ret)

            # Check submission success
            status, result, error = get_idds_result(ret)
            if status:
                request_id = int(result)
            else:
                raise RuntimeError(f"Error submitting to PanDA service: {error}")

            _LOG.info("Submitted into iDDs with request id=%s", request_id)
            workflow.run_id = request_id

    def restart(self, wms_workflow_id):
        # Docstring inherited from BaseWmsService.restart.
        idds_client = get_idds_client(self.config)
        ret = idds_client.retry(request_id=wms_workflow_id)
        _LOG.debug("Restart PanDA workflow returned = %s", ret)

        status, result, error = get_idds_result(ret)
        if status:
            _LOG.info("Restarting PanDA workflow %s", result)
            return wms_workflow_id, None, json.dumps(result)

        return None, None, f"Error retry PanDA workflow: {error}"

    def report(
        self,
        wms_workflow_id=None,
        user=None,
        hist=0,
        pass_thru=None,
        is_global=False,
        return_exit_codes=False,
    ):
        # Docstring inherited from BaseWmsService.report.
        message = ""
        run_reports = []

        if not wms_workflow_id:
            message = "Run summary not implemented yet, use 'bps report --id <workflow_id>' instead"
            return run_reports, message

        idds_client = get_idds_client(self.config)
        ret = idds_client.get_requests(request_id=wms_workflow_id, with_detail=True)
        _LOG.debug("PanDA get workflow status returned = %s", str(ret))

        request_status = ret[0]
        if request_status != 0:
            raise RuntimeError(f"Error to get workflow status: {ret} for id: {wms_workflow_id}")

        tasks = ret[1][1]
        if not tasks:
            message = f"No records found for workflow id '{wms_workflow_id}'. Hint: double check the id"
        else:
            head = tasks[0]
            wms_report = WmsRunReport(
                wms_id=str(head["request_id"]),
                operator=head["username"],
                project="",
                campaign="",
                payload="",
                run=head["name"],
                state=WmsStates.UNKNOWN,
                total_number_jobs=0,
                job_state_counts={state: 0 for state in WmsStates},
                job_summary={},
                run_summary="",
                exit_code_summary=[],
            )

            # The status of a task is taken from the first item of state_map.
            # The workflow is in status WmsStates.FAILED when:
            #      All tasks have failed.
            # SubFinished tasks has jobs in
            #      output_processed_files: Finished
            #      output_failed_files: Failed
            #      output_missing_files: Missing
            state_map = {
                "Finished": [WmsStates.SUCCEEDED],
                "SubFinished": [
                    WmsStates.SUCCEEDED,
                    WmsStates.FAILED,
                    WmsStates.PRUNED,
                ],
                "Transforming": [
                    WmsStates.RUNNING,
                    WmsStates.SUCCEEDED,
                    WmsStates.FAILED,
                    WmsStates.UNREADY,
                    WmsStates.PRUNED,
                ],
                "Failed": [WmsStates.FAILED, WmsStates.PRUNED],
            }

            file_map = {
                WmsStates.SUCCEEDED: "output_processed_files",
                WmsStates.RUNNING: "output_processing_files",
                WmsStates.FAILED: "output_failed_files",
                WmsStates.UNREADY: "input_new_files",
                WmsStates.PRUNED: "output_missing_files",
            }

            workflow_status = head["status"]["attributes"]["_name_"]
            if workflow_status in ["Finished", "SubFinished"]:
                wms_report.state = WmsStates.SUCCEEDED
            elif workflow_status in ["Failed", "Expired"]:
                wms_report.state = WmsStates.FAILED
            elif workflow_status in ["Cancelled"]:
                wms_report.state = WmsStates.DELETED
            elif workflow_status in ["Suspended"]:
                wms_report.state = WmsStates.HELD
            else:
                wms_report.state = WmsStates.RUNNING

            try:
                tasks.sort(key=lambda x: x["transform_workload_id"])
            except Exception:
                tasks.sort(key=lambda x: x["transform_id"])

            exit_codes_all = {}
            # Loop over all tasks data returned by idds_client
            for task in tasks:
                if task["transform_id"] is None:
                    # Not created task (It happens because of an outer join
                    # between requests table and transforms table).
                    continue

                exit_codes = []
                totaljobs = task["output_total_files"]
                wms_report.total_number_jobs += totaljobs
                tasklabel = task["transform_name"]
                tasklabel = re.sub(wms_report.run + "_", "", tasklabel)
                status = task["transform_status"]["attributes"]["_name_"]
                taskstatus = {}
                # if the state is failed, gather exit code information
                if status in ["SubFinished", "Failed"]:
                    transform_workload_id = task["transform_workload_id"]
                    if not (task["transform_name"] and task["transform_name"].startswith("build_task")):
                        new_ret = idds_client.get_contents_output_ext(
                            request_id=wms_workflow_id, workload_id=transform_workload_id
                        )
                        _LOG.debug(
                            "PanDA get task %s detail returned = %s", transform_workload_id, str(new_ret)
                        )

                        request_status = new_ret[0]
                        if request_status != 0:
                            raise RuntimeError(
                                f"Error to get workflow status: {new_ret} for id: {wms_workflow_id}"
                            )
                        # task_info is a dictionary of len 1 that contains
                        # a list of dicts containing panda job info
                        task_info = new_ret[1][1]

                        if len(task_info) == 1:
                            wmskey = list(task_info.keys())[0]
                            wmsjobs = task_info[wmskey]
                        else:
                            err_msg = "Unexpected job return from PanDA: "
                            err_msg += f"{task_info} for id: {transform_workload_id}"
                            raise RuntimeError(err_msg)
                        exit_codes = [
                            wmsjob["trans_exit_code"]
                            for wmsjob in wmsjobs
                            if wmsjob["trans_exit_code"] is not None and int(wmsjob["trans_exit_code"]) != 0
                        ]
                        exit_codes_all[tasklabel] = exit_codes
                # Fill number of jobs in all WmsStates
                for state in WmsStates:
                    njobs = 0
                    # Each WmsState have many iDDS status mapped to it.
                    if status in state_map:
                        for mappedstate in state_map[status]:
                            if state in file_map and mappedstate == state:
                                if task[file_map[mappedstate]] is not None:
                                    njobs = task[file_map[mappedstate]]
                                if state == WmsStates.RUNNING:
                                    njobs += task["output_new_files"] - task["input_new_files"]
                                break
                    wms_report.job_state_counts[state] += njobs
                    taskstatus[state] = njobs
                wms_report.job_summary[tasklabel] = taskstatus

                # To fill the EXPECTED column
                if wms_report.run_summary:
                    wms_report.run_summary += ";"
                wms_report.run_summary += f"{tasklabel}:{totaljobs}"

            wms_report.exit_code_summary = exit_codes_all
            run_reports.append(wms_report)

        return run_reports, message

    def list_submitted_jobs(self, wms_id=None, user=None, require_bps=True, pass_thru=None, is_global=False):
        # Docstring inherited from BaseWmsService.list_submitted_jobs.
        if wms_id is None and user is not None:
            raise RuntimeError(
                "Error to get workflow status report: wms_id is required"
                " and filtering workflows with 'user' is not supported."
            )

        idds_client = get_idds_client(self.config)
        ret = idds_client.get_requests(request_id=wms_id)
        _LOG.debug("PanDA get workflows returned = %s", ret)

        status, result, error = get_idds_result(ret)
        if status:
            req_ids = [req["request_id"] for req in result]
            return req_ids

        raise RuntimeError(f"Error list PanDA workflow requests: {error}")

    def cancel(self, wms_id, pass_thru=None):
        # Docstring inherited from BaseWmsService.cancel.
        idds_client = get_idds_client(self.config)
        ret = idds_client.abort(request_id=wms_id)
        _LOG.debug("Abort PanDA workflow returned = %s", ret)

        status, result, error = get_idds_result(ret)
        if status:
            _LOG.info("Aborting PanDA workflow %s", result)
            return True, json.dumps(result)

        return False, f"Error abort PanDA workflow: {error}"

    def ping(self, pass_thru=None):
        # Docstring inherited from BaseWmsService.ping.
        idds_client = get_idds_client(self.config)
        ret = idds_client.ping()
        _LOG.debug("Ping PanDA service returned = %s", ret)

        status, result, error = get_idds_result(ret)
        if status:
            if "Status" in result and result["Status"] == "OK":
                return 0, None

            return -1, f"Error ping PanDA service: {result}"

        return -1, f"Error ping PanDA service: {error}"

    def run_submission_checks(self):
        # Docstring inherited from BaseWmsService.run_submission_checks.
        for key in ["PANDA_URL"]:
            if key not in os.environ:
                raise OSError(f"Missing environment variable {key}")

        status, message = self.ping()
        if status != 0:
            raise RuntimeError(message)


class PandaBpsWmsWorkflow(BaseWmsWorkflow):
    """A single Panda based workflow.

    Parameters
    ----------
    name : `str`
        Unique name for Workflow.
    config : `lsst.ctrl.bps.BpsConfig`
        BPS configuration that includes necessary submit/runtime information.
    """

    def __init__(self, name, config=None):
        super().__init__(name, config)
        self.files_to_pre_stage = {}  # src, dest
        self.idds_client_workflow = IDDS_client_workflow(name=name)

    @classmethod
    def from_generic_workflow(cls, config, generic_workflow, out_prefix, service_class):
        # Docstring inherited from BaseWmsWorkflow.from_generic_workflow.
        wms_workflow = cls(generic_workflow.name, config)

        files, dag_sink_work, task_count = add_idds_work(
            config, generic_workflow, wms_workflow.idds_client_workflow
        )
        wms_workflow.files_to_pre_stage.update(files)

        files = add_final_idds_work(
            config, generic_workflow, wms_workflow.idds_client_workflow, dag_sink_work, task_count + 1, 1
        )
        wms_workflow.files_to_pre_stage.update(files)

        return wms_workflow

    def write(self, out_prefix):
        # Docstring inherited from BaseWmsWorkflow.write.
        with open(os.path.join(out_prefix, "panda_workflow.pickle"), "wb") as fh:
            pickle.dump(self, fh)

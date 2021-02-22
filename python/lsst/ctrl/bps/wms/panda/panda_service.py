# This file is part of ctrl_bps.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
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

import os
import logging
import binascii

from lsst.ctrl.bps.wms_service import BaseWmsWorkflow, BaseWmsService
from lsst.ctrl.bps.wms.panda.idds_tasks import IDDSWorkflowGenerator
from lsst.daf.butler import ButlerURI
from idds.workflow.workflow import Workflow as IDDS_client_workflow
from idds.doma.workflow.domalsstwork import DomaLSSTWork
import idds.common.constants as idds_constants
import idds.common.utils as idds_utils
import pandatools.idds_api

_LOG = logging.getLogger(__name__)


class PanDAService(BaseWmsService):
    """PanDA version of WMS service
    """

    def prepare(self, config, generic_workflow, out_prefix=None):
        """Convert generic workflow to an PanDA iDDS ready for submission

        Parameters
        ----------
        config : `~lsst.ctrl.bps.BPSConfig`
            BPS configuration that includes necessary submit/runtime information
        generic_workflow :  `~lsst.ctrl.bps.generic_workflow.GenericWorkflow`
            The generic workflow (e.g., has executable name and arguments)
        out_prefix : `str`
            The root directory into which all WMS-specific files are written

        Returns
        ----------
        workflow : `~lsst.ctrl.bps.wms.panda.panda_service.PandaBpsWmsWorkflow`
            PanDA workflow ready to be run.
        """
        _LOG.debug("out_prefix = '%s'", out_prefix)
        workflow = PandaBpsWmsWorkflow.from_generic_workflow(config, generic_workflow, out_prefix,
                                                             f"{self.__class__.__module__}."
                                                             f"{self.__class__.__name__}")
        workflow.write(out_prefix)
        return workflow

    def convert_exec_string_to_hex(self, cmdline):
        """
        Converts the command line into hex representation. This step is currently involved because large
        blocks of command lines including special symbols passed to the pilot/container. To make sure the 1
        to 1 matching and pass by the special symbol stripping performed by the Pilot we applied the
        hexing.

        Parameters
        ----------
        cmdline: `str`
            UTF-8 command line string

        Returns
        -------
            Hex representation of string
        """
        return binascii.hexlify(cmdline.encode()).decode("utf-8")

    def add_decoder_prefix(self, cmdline):
        """
        Compose the command line sent to the pilot from the functional part (the actual SW running) and the
        middleware part (containers invocation)

        Parameters
        ----------
        cmdline: `str`
            UTF-8 based functional part of the command line

        Returns
        -------
            Full command line to be executed on the edge node
        """

        cmdline_hex = self.convert_exec_string_to_hex(cmdline)
        decoder_prefix = self.config.get('runner_command')
        decoder_prefix = decoder_prefix.replace("_cmd_line_", str(cmdline_hex))
        return decoder_prefix

    def submit(self, workflow):
        """Submit a single PanDA iDDS workflow

        Parameters
        ----------
        workflow : `~lsst.ctrl.bps.wms_service.BaseWorkflow`
            A single PanDA iDDS workflow to submit
        """
        idds_client_workflow = IDDS_client_workflow()

        for idx, task in enumerate(workflow.generated_tasks):
            work = DomaLSSTWork(
                executable=self.add_decoder_prefix(task.executable),
                primary_input_collection={'scope': 'pseudo_dataset',
                                          'name': 'pseudo_input_collection#' + str(idx)},
                output_collections=[{'scope': 'pseudo_dataset',
                                     'name': 'pseudo_output_collection#' + str(idx)}],
                log_collections=[], dependency_map=task.dependencies,
                task_name=task.name,
                task_queue=task.queue)
            idds_client_workflow.add_work(work)
        idds_request = {
            'scope': 'workflow',
            'name': idds_client_workflow.get_name(),
            'requester': 'panda',
            'request_type': idds_constants.RequestType.Workflow,
            'transform_tag': 'workflow',
            'status': idds_constants.RequestStatus.New,
            'priority': 0,
            'lifetime': 30,
            'workload_id': idds_client_workflow.get_workload_id(),
            'request_metadata': {'workload_id': idds_client_workflow.get_workload_id(),
                                 'workflow': idds_client_workflow}
        }
        primary_init_work = idds_client_workflow.get_primary_initial_collection()
        if primary_init_work:
            idds_request['scope'] = primary_init_work['scope']
            idds_request['name'] = primary_init_work['name']
        c = pandatools.idds_api.get_api(idds_utils.json_dumps)
        request_id = c.add_request(**idds_request)
        _LOG.info("Submitted into iDDs with request id=%i", request_id)
        workflow.run_id = request_id

    def report(self, wms_workflow_id=None, user=None, hist=0, pass_thru=None):
        """
        Stub for future implementation of the report method
        Expected to return run information based upon given constraints.

        Parameters
        ----------
        wms_workflow_id : `int` or `str`
            Limit to specific run based on id.
        user : `str`
            Limit results to runs for this user.
        hist : `float`
            Limit history search to this many days.
        pass_thru : `str`
            Constraints to pass through to HTCondor.

        Returns
        -------
        runs : `dict` of `~lsst.ctrl.bps.wms_service.WmsRunReport`
            Information about runs from given job information.
        message : `str`
            Extra message for report command to print.  This could be pointers to documentation or
            to WMS specific commands.
        """
        message = ""
        run_reports = None
        return run_reports, message


class PandaBpsWmsWorkflow(BaseWmsWorkflow):
    """
    A single Panda based workflow
    Parameters
    ----------
    name : `str`
        Unique name for Workflow
    config : `~lsst.ctrl.bps.BPSConfig`
        BPS configuration that includes necessary submit/runtime information
    """

    def __init__(self, name, config=None):
        super().__init__(name, config)
        self.generated_tasks = None

    @classmethod
    def from_generic_workflow(cls, config, generic_workflow, out_prefix, service_class):
        # Docstring inherited from parent class
        idds_workflow = cls(generic_workflow.name, config)
        workflow_generator = IDDSWorkflowGenerator(generic_workflow, config)
        idds_workflow.generated_tasks = workflow_generator.define_tasks()
        cloud_prefix = config['bucket'] + '/' + \
            config['payload_folder'] + '/' + config['workflowName'] + '/'
        for task in idds_workflow.generated_tasks:
            cls.copy_pickles_into_cloud(task.local_pfns, cloud_prefix)
        _LOG.debug("panda dag attribs %s", generic_workflow.run_attrs)
        return idds_workflow

    @staticmethod
    def copy_pickles_into_cloud(local_pfns, cloud_prefix):
        """
        Brings locally generated pickle files into Cloud for further utilization them on the egde nodes.
        Parameters
        ----------
        local_pfns: `str`
            Local path to the file to be copied

        cloud_prefix: `str`
            Path on the cloud, including access protocol, bucket name to place files
        """
        for src_path in local_pfns:
            src = ButlerURI(src_path)
            target_base_uri = ButlerURI(cloud_prefix)
            target = target_base_uri.join(os.path.basename(src_path))
            target.transfer_from(src)

    def write(self, out_prefix):
        """
        Not yet implemented
        """

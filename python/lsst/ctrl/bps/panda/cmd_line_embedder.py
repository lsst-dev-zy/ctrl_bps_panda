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

import logging
import os
import re

from lsst.ctrl.bps.panda.constants import PANDA_MAX_LEN_INPUT_FILE

_LOG = logging.getLogger(__name__)


class CommandLineEmbedder:
    """Class embeds static (constant across a task) values
    into the pipeline execution command line
    and resolves submission side environment variables

    Parameters
    ----------
    config : `lsst.ctrl.bps.BpsConfig`
        BPS configuration that includes the list of dynamic
        (uniques per job) and submission side resolved variables
    """

    def __init__(self, config):
        self.leave_placeholder_params = config.get("placeholderParams", ["qgraphNodeId", "qgraphId"])
        self.submit_side_resolved = config.get("submitSideResolvedParams", ["USER"])

    def replace_static_parameters(self, cmd_line, lazy_vars):
        """Substitutes the lazy parameters in the command line which
        are static, the same for every job in the workflow and could be
        defined once.

        This function offloads the edge node processing
        and number of parameters transferred together with job

        Parameters
        ----------
        cmd_line:  `str`
            Command line to be processed.
        lazy_vars : `dict`
            Lazy variables and its values.

        Returns
        -------
        cmd : `str`
            Processed command line.
        """
        for param_name, param_val in lazy_vars.items():
            if param_name not in self.leave_placeholder_params:
                cmd_line = cmd_line.replace("{" + param_name + "}", param_val)
        return cmd_line

    def replace_static_files(self, cmd_line, files):
        """Substitute the FILE keys with values in the command line
        which are static, the same for every job in the workflow and
        could be defined once.

        Parameters
        ----------
        cmd_line: `str`
            command line to be processed
        files: `list` [`lsst.ctrl.bps.GenericWorkflowFile`]
            input and output files for the job.

        Returns
        -------
        cmd : `str`
            Processed command line.
        """
        # make copy of given command line for error message.
        orig_cmd_line = cmd_line

        # make gwfile lookup by name
        files_by_name = {}
        for gwfile in files:
            files_by_name[gwfile.name] = gwfile

        for file_key in re.findall(r"<FILE:([^>]+)>", cmd_line):
            try:
                gwfile = files_by_name[file_key]
            except KeyError as e:
                raise RuntimeError(
                    "%s in command line, but corresponding file not given to function (%s)",
                    file_key,
                    orig_cmd_line,
                ) from e

            if not gwfile.wms_transfer and gwfile.job_access_remote:
                cmd_line = cmd_line.replace(f"<FILE:{gwfile.name}>", gwfile.src_uri)
        return cmd_line

    def resolve_submission_side_env_vars(self, cmd_line):
        """Substitute the lazy parameters in the command line
        which are defined and resolved on the submission side.

        Parameters
        ----------
        cmd_line : `str`
            Command line to be processed.

        Returns
        -------
        cmd : `str`
            Processed command line.
        """
        for param in self.submit_side_resolved:
            if os.getenv(param):
                cmd_line = cmd_line.replace("<ENV:" + param + ">", os.getenv(param))
            else:
                _LOG.info("Expected parameter %s is not found in the environment variables", param)
        return cmd_line

    def attach_pseudo_file_params(self, lazy_vars):
        """Add the parameters needed to finalize creation of a pseudo file.

        Parameters
        ----------
        lazy_vars : `dict`
            Values to be substituted.

        Returns
        -------
        suffix : `str`
            Pseudo input file name suffix.
        """
        file_suffix = ""
        for item in self.leave_placeholder_params:
            file_suffix += "+" + item + ":" + lazy_vars.get(item, "")
        return file_suffix

    def substitute_command_line(self, cmd_line, lazy_vars, job_name, gwfiles):
        """Preprocess the command line leaving for the edge node evaluation
        only parameters which are job / environment dependent

        Parameters
        ----------
        cmd_line: `str`
            Command line containing all lazy placeholders.
        lazy_vars: `dict` [ `str`, `str` ]
            Lazy parameter name/values.
        job_name: `str`
            Job name proposed by BPS.
        gwfiles: `list` [`lsst.ctrl.bps.GenericWorkflowFile`]
            Job files.

        Returns
        -------
        cmd_line: `str`
            processed command line
        file_name: `str`
            job pseudo input file name

        Raises
        ------
        RuntimeError
            Raised if pseudo input filename is too long.
        """
        cmd_vals = {m.group(1) for m in re.finditer(r"[^$]{([^}]+)}", cmd_line)}
        actual_lazy_vars = {}
        for key in cmd_vals:
            actual_lazy_vars[key] = lazy_vars[key]

        cmd_line = self.replace_static_parameters(cmd_line, actual_lazy_vars)
        cmd_line = self.resolve_submission_side_env_vars(cmd_line)
        if gwfiles:
            cmd_line = self.replace_static_files(cmd_line, gwfiles)
        file_name = job_name + self.attach_pseudo_file_params(actual_lazy_vars)

        max_char = "IDDS_MAX_NAME_LENGTH"
        panda_url = "PANDA_URL"
        is_usdf = False
        PANDA_MAX_DB_VCHAR = PANDA_MAX_LEN_INPUT_FILE
        if panda_url in os.environ:
            PANDA_instance = os.environ[panda_url]
            if "doma" not in PANDA_instance:
                is_usdf = True
        if max_char in os.environ and is_usdf:
            PANDA_MAX_DB_VCHAR = int(os.environ[max_char])
        if len(file_name) > PANDA_MAX_DB_VCHAR:
            _LOG.error(f"Too long pseudo input filename: {file_name}")
            raise RuntimeError(
                f"job pseudo input file name contains more than {PANDA_MAX_LEN_INPUT_FILE} symbols. Aborting."
            )

        return cmd_line, file_name

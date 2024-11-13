#!/usr/bin/python

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

"""
Decode the command line string sent from the BPS
plugin -> PanDA -> Edge node cluster management
-> Edge node -> Container. This file is not a part
of the BPS but a part of the payload wrapper.
It decodes the hexified command line.
"""
import binascii
import json
import os
import re
import sys

from lsst.resources import ResourcePath
from lsst.ctrl.bps.panda.utils import download_extract_archive

def replace_placeholders(cmd_line: str, tag: str, replacements: dict[str, str]) -> str:
    """Replace the placeholders.

    Parameters
    ----------
    cmd_line : `str`
        Command line.
    tag : `str`
        Tag to use for finding placeholders.
    replacements : `dict` [`str`, `str`]
        Replacements indexed by place holder string.

    Returns
    -------
    modified : `str`
        Processed command line.
    """
    occurrences_to_replace = re.findall(f"<{tag}:(.*?)>", cmd_line)
    for placeholder in occurrences_to_replace:
        if placeholder in replacements:
            cmd_line = cmd_line.replace(f"<{tag}:{placeholder}>", replacements[placeholder])
        else:
            raise ValueError(
                "ValueError exception thrown, because "
                f"{placeholder} is not found in the "
                "replacement values and could "
                "not be passed to the command line"
            )
    return cmd_line


def replace_environment_vars(cmd_line):
    """Replace placeholders to the actual environment variables.

    Parameters
    ----------
    cmd_line : `str`
        Command line.

    Returns
    -------
    cmdline: `str`
        Processed command line.
    """
    environment_vars = os.environ
    cmd_line = replace_placeholders(cmd_line, "ENV", environment_vars)
    return cmd_line


def replace_files_placeholders(cmd_line, files):
    """Replace placeholders for files.

    Parameters
    ----------
    cmd_line : `str`
        Command line.
    files : `str`
        String with key:value pairs separated by the '+' sign.
        Keys contain the file type (runQgraphFile,
        butlerConfig, ...).
        Values contains file names.

    Returns
    -------
    cmd_line: `str`
        Processed command line.
    """
    files_key_vals = files.split("+")
    files = {}
    for file in files_key_vals:
        file_name_placeholder, file_name = file.split(":")
        files[file_name_placeholder] = file_name
    cmd_line = replace_placeholders(cmd_line, "FILE", files)
    return cmd_line


def deliver_input_files(src_path, files, skip_copy):
    """Deliver input files needed for a job.

    Parameters
    ----------
    src_path : `str`
        URI for folder where the input files placed.
    files : `str`
        String with file names separated by the '+' sign.
    skip_copy : `str`
        String with file names separated by the '+' sign indicating which
        files in ``files`` should not be copied.

    Returns
    -------
    cmdline: `str`
        Processed command line.
    """
    files = files.split("+")
    src_uri = ResourcePath(src_path, forceDirectory=True)

    if 'jobO' in skip_copy:
        download_extract_archive(skip_copy)
        for script in files:
            file_name_placeholder, file_pfn = script.split(":")
            os.chmod(file_pfn, 0o755)
        return

    for file in files:
        file_name_placeholder, file_pfn = file.split(":")
        if file_name_placeholder not in skip_copy.split("+"):
            src = src_uri.join(file_pfn)
            base_dir = None
            if src.isdir():
                files_to_copy = ResourcePath.findFileResources([src])
                base_dir = file_pfn
            else:
                files_to_copy = [src]
            dest_base = ResourcePath("", forceAbsolute=True, forceDirectory=True)
            if base_dir:
                dest_base = dest_base.join(base_dir)
            for file_to_copy in files_to_copy:
                dest = dest_base.join(file_to_copy.basename())
                if file_name_placeholder == "orderIdMapFilename":
                    if not dest.exists():
                        dest.transfer_from(file_to_copy, transfer="copy")
                else:
                    dest.transfer_from(file_to_copy, transfer="copy")
                print(f"copied {file_to_copy.path} to {dest.path}", file=sys.stderr)
            if file_name_placeholder == "job_executable":
                os.chmod(dest.path, 0o777)

def replace_event_file(params, files):
    """Replace events with node id.

    Parameters
    ----------
    params : `str`
        String with parameters separated by the '+' sign.
        Example params:
            isr:eventservice_90^10+somethingelse. This part
            'isr:eventservice_90^10' is the EventService parameter.
        The format for the EventService parameter for LSST is
        'label:eventservice_<baseid>^<localid>'. The '<localid>' should
        start from 1, which means the first event of the file
        'label:eventservice_<baseid>'. In EventService, all pseudo files
        for a label is recorded in the 'orderIdMapFilename' file, with
        a dict {'label0':{"0":"pseudo_file0", "1":..},'label1':..}.
        For example, for a workflow with 100 pseudo files for the 'isr' label,
        the dict will be {'isr': {"0": "pseudo0", "1": "pseudo_file1",
        "99": "pseudo_file99"}}. If we split the 100 pseudo files into 5 PanDA
        jobs with 20 files per PanDA job, the 5 eventservice group name will be
        'isr:event_service_0' for events ["0"~"19"], 'isr:event_service_20' for
        events ["20"~"39"], ..., and 'isr:event_service_80' for events
        ["80"~"99"]. The EventService param 'isr:event_service_80^5' means the
        5th event in the group 'isr:event_service_80', which is '80 + 5 -1=84'
        and will be mapped to file 'pseudo_file84'.
    files : `str`
        String with file names separated by the '+' sign.
        Example:
            orderIdMapFilename:panda_order_id_map.json+runQgraphFile:a.qgraph

    Returns
    -------
    ret_status: `bool`
        Status of this function. If eventservice is enabled but this function
        cannot handle it, it should return False. Otherwise it should
        return True.
    with_events: `bool`
        Whether there are event parameters.
    params_map: `dict` [`str`, `dict`]
        Parameter map for event information.
    """
    ret_status = True
    with_events = False
    files = files.split("+")
    file_map = {}
    for file in files:
        file_name_placeholder, file_pfn = file.split(":")
        file_map[file_name_placeholder] = file_pfn
    order_id_map_file = file_map.get("orderIdMapFilename", None)
    order_id_map = {}
    try:
        # The orderIdMapFilename should exist locally or copied to current
        # directory by deliver_input_files
        if order_id_map_file and os.path.exists(order_id_map_file):
            with open(order_id_map_file) as f:
                order_id_map = json.load(f)
    except Exception as ex:
        print(f"failed to load orderIdMapFilename: {ex}")

    params_map = {}
    params_list = params.split("+")
    for param in params_list:
        if "eventservice_" in param:
            with_events = True
            label, event = param.split(":")
            event_id = event.split("_")[1]
            event_base_id = event_id.split("^")[0]
            # The original format for EventService parameter is
            # 'label:eventservice_<baseid>^<localid>^<numberOfEvents>',
            # which can have multiple events per EventService job.
            # However, for LSST, the '<numberOfEvents>' is always 1.
            # When <numberOfEvents> is 1, it will not show. So for LSST,
            # we will see 'label:eventservice_<baseid>^<localid>'.
            # However, to leave posibilities for future updates,
            # the line below has two splits based on '^', which is from
            # the original EventService parameter format.
            event_order = event_id.split("^")[1].split("^")[0]
            event_index = str(int(event_base_id) + int(event_order) - 1)
            if not order_id_map:
                print("EventSerice is enabled but order_id_map file doesn't exist.")
                ret_status = False
                break

            if label not in order_id_map:
                print(
                    f"EventSerice is enabled but label {label} doesn't in the keys"
                    f" of order_id_map {order_id_map.keys()}"
                )
                ret_status = False
                break
            if event_index not in order_id_map[label]:
                print(
                    f"EventSerice is enabled but event_index {event_index} is not"
                    f" in order_id_map[{label}] {order_id_map[label].keys()}"
                )
                ret_status = False
                break

            params_map[param] = {"event_index": event_index, "order_id_map": order_id_map[label]}
    return ret_status, with_events, params_map

deliver_input_files(sys.argv[3], sys.argv[4], sys.argv[5])
cmd_line = str(binascii.unhexlify(sys.argv[1]).decode())
data_params = sys.argv[2]
cmd_line = replace_environment_vars(cmd_line)

# If EventService is enabled, data_params will only contain event information.
# So we need to convert the event information to LSST pseudo file names.
# If EventService is not enabled, this part will not change data_params.
ret_event_status, with_events, event_params_map = replace_event_file(data_params, sys.argv[4])
print(f"ret_event_status: {ret_event_status}, with_events: {with_events}")
if not ret_event_status:
    print("failed to map EventService parameters to original LSST pseudo file names")
    exit_code = 1
    sys.exit(exit_code)

for event_param in event_params_map:
    event_index = event_params_map[event_param]["event_index"]
    pseudo_file_name = event_params_map[event_param]["order_id_map"][event_index]
    print(f"replacing event {event_param} with event_index {event_index} to: {pseudo_file_name}")
    cmd_line = cmd_line.replace(event_param, pseudo_file_name)
    data_params = data_params.replace(event_param, pseudo_file_name)

data_params = data_params.split("+")

"""Replace the pipetask command line placeholders
 with actual data provided in the script call
 in form placeholder1:file1+placeholder2:file2:...
"""
cmd_line = replace_files_placeholders(cmd_line, sys.argv[4])

for key_value_pair in data_params[1:]:
    (key, value) = key_value_pair.split(":")
    cmd_line = cmd_line.replace("{" + key + "}", value)

print("executable command line:")
print(cmd_line)

exit_status = os.system(cmd_line)
exit_code = 1
if os.WIFSIGNALED(exit_status):
    exit_code = os.WTERMSIG(exit_status) + 128
elif os.WIFEXITED(exit_status):
    exit_code = os.WEXITSTATUS(exit_status)
sys.exit(exit_code)

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
import click

from lsst.daf.butler.cli.butler import LoaderCLI
from lsst.daf.butler.cli.opt import (
    log_file_option,
    log_label_option,
    log_level_option,
    log_tty_option,
    long_log_option,
)


class PandaAuthCli(LoaderCLI):
    """Command-line interface loader."""

    localCmdPkg = "lsst.ctrl.bps.panda.cli.cmd"


@click.command(cls=PandaAuthCli, context_settings={"help_option_names": ["-h", "--help"]}, epilog=None)
@log_level_option(default=["INFO"])
@long_log_option()
@log_file_option()
@log_tty_option()
@log_label_option()
def cli(log_level, long_log, log_file, log_tty, log_label):  # numpydoc ignore=PR01
    """Run click command-line."""
    pass


def main():
    """Run main command-line interface."""
    return cli()

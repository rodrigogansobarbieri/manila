# Copyright 2015, Hitachi Data Systems.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

import json
import os
import time
import subprocess

import shlex
import six

from oslo_log import log
from oslo_utils import importutils
from manila.i18n import _LI
from manila import utils

#eventlet = importutils.try_import('eventlet')
#if eventlet and eventlet.patcher.is_monkey_patched(time):
#    from eventlet.green import subprocess
#else:
#    import subprocess

LOG = log.getLogger(__name__)


class CopyUtils(object):

    def __init__(self, src, dest, ignore_list):
        self.src = src
        self.dest = dest
        self.total_size = 0
        self.cancelled = False
        self.current_size = 0
        self.current_copy = None
        self.ignore_list = ignore_list
        self.process = None

    def get_progress(self):

        if self.current_copy is not None:

            try:
                size = os.stat(self.current_copy['file_path']).st_size

            except OSError:
                size = 0

            total_progress = 0
            if self.total_size > 0:
                total_progress = self.current_size * 100 / self.total_size
            current_file_progress = 0
            if self.current_copy['size'] > 0:
                current_file_progress = size * 100 / self.current_copy['size']
            current_file_path = six.text_type(self.current_copy['file_path'])

            progress = {
                'total_progress': total_progress,
                'current_file_path': current_file_path,
                'current_file_progress': current_file_progress
            }

            return progress
        else:
            return {'total_progress': 100}

    def cancel(self):

        self.cancelled = True
        if self.process is not None:
            self.process.kill()

    def run(self):

        self._get_total_size()
        self._copy()

    def _copy(self):

        args_json = {
            'src': self.src,
            'dest': self.dest,
            'ignore_list': self.ignore_list,
        }
        args = json.dumps(args_json)
        root_helper = utils._get_root_helper()
        cmd = ("python", "/opt/stack/manila/manila/data/copy.py", args)
        cmd = shlex.split(root_helper) + list(cmd)
        _PIPE = subprocess.PIPE  # pylint: disable=E1101

        LOG.debug("Starting copy process: %s", six.text_type(cmd))

        self.process = subprocess.Popen(
            cmd, stdin=_PIPE, stdout=_PIPE, stderr=_PIPE, shell=True)

        while True:
            line = self.process.stdout.readline()
            LOG.debug("Received output from copy process: %s", line)
            if line is not None and line != '' and len(line) > 3:
                progress = json.loads(line)
                self.current_copy = progress['current_copy']
                self.current_size = progress['current_size']
                LOG.info(_LI(six.text_type(self.get_progress())))
            else:
                break

        LOG.debug("Copy process return code: %s", self.process.returncode)
        LOG.debug("Copy process stdout: %s", self.process.stdout.readall())
        LOG.debug("Copy process stderr: %s", self.process.stderr.readall())

        if self.process.returncode != 0:
            raise Exception("Copy failed.")

    def _get_total_size(self):

        for dirpath, dirnames, filenames in os.walk(self.src):

            if self.cancelled:
                return

            for filename in filenames:
                if self.cancelled:
                    return
                if filename not in self.ignore_list:
                    src_file = os.path.join(dirpath, filename)

                    LOG.debug("Checking size of file %s", src_file)

                    size = os.stat(src_file).st_size
                    self.total_size += size

        LOG.debug("Total size of %(src)s is %(size)s.",
                  {'src': self.src, 'size': self.total_size})

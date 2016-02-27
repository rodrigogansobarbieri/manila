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
import subprocess
import sys


class Copy(object):

    def __init__(self, src, dest, ignore_list):
        self.src = src
        self.dest = dest
        self.current_size = 0
        self.current_copy = None
        self.ignore_list = ignore_list

    def run(self):

        self._copy()

    def _copy(self):

        for dirpath, dirnames, filenames in os.walk(self.src):

            for dirname in dirnames:
                if dirname not in self.ignore_list:
                    src_dir = os.path.join(dirpath, dirname)
                    dest_dir = src_dir.replace(self.src, self.dest)
                    os.mkdir(dest_dir)
                    stat = os.stat(src_dir)
                    os.chown(dest_dir, stat.st_uid, stat.st_gid)
                    print("e5")

            for filename in filenames:
                if filename not in self.ignore_list:
                    src_file = os.path.join(dirpath, filename)
                    size = os.stat(src_file).st_size
                    dest_file = src_file.replace(self.src, self.dest)
                    self.current_copy = {'file_path': dest_file,
                                         'size': size}
                    print("f6")
                    return_code = subprocess.call(
                        ["cp", "-d", "--preserve=all", src_file, dest_file])
                    print("g7")
                    if return_code != 0:
                        print("h8")
                        sys.exit("Could not copy file %s." % src_file)
                    print("i9")
                    self.current_size += size
                    output = json.dumps({'current_copy': self.current_copy,
                                         'current_size': self.current_size})
                    print(output)

print("a1")
try:
    args_json = sys.argv[1]
    args = json.loads(args_json)
    print("b2")
    copy = Copy(args['src'], args['dest'], args['ignore_list'])
    copy.run()
except Exception as e:
    print("c3")
    sys.exit(e)
print("d4")
sys.exit(0)
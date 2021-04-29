#
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

module Shell
  module Commands
    # Switch compaction offload for a region server
    class CompactionOffloadSwitch < Command
      def help
        <<~EOF
          Turn the compaction offload on or off on regionservers. The setting will be lost on
          restart of the server. Compaction offload can also be enabled/disabled by modifying
          configuration hbase.compaction.offload.enabled in hbase-site.xml.
          Examples:
            To enable compactions offload on all region servers
            hbase> compaction_offload_switch true
            To disable compactions offload on all region servers
            hbase> compaction_offload_switch false
            To enable compactions offload on specific region servers
            hbase> compaction_offload_switch true, 'server2','server1'
            To disable compactions offload on specific region servers
            hbase> compaction_offload_switch false, 'server2','server1'
          NOTE: A server name is its host, port plus startcode. For example:
          host187.example.com,60020,1289493121758
        EOF
      end

      def command(enable_disable, *server)
        formatter.header(%w(['SERVER' 'PREV_STATE']))
        prev_state = admin.compaction_offload_switch(enable_disable, server)
        prev_state.each { |k, v| formatter.row([k.getServerName, java.lang.String.valueOf(v)]) }
        formatter.footer(prev_state.size)
      end
    end
  end
end

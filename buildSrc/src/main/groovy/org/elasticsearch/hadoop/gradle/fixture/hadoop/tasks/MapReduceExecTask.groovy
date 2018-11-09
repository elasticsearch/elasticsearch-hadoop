/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.hadoop.gradle.fixture.hadoop.tasks

import org.elasticsearch.hadoop.gradle.fixture.hadoop.conf.HadoopClusterConfiguration
import org.elasticsearch.hadoop.gradle.fixture.hadoop.conf.InstanceConfiguration
import org.elasticsearch.hadoop.gradle.fixture.hadoop.services.HadoopServiceDescriptor
import org.gradle.api.DefaultTask
import org.gradle.api.GradleException
import org.gradle.api.tasks.TaskAction
import org.gradle.process.ExecSpec

class MapReduceExecTask extends DefaultTask {

    HadoopClusterConfiguration clusterConfiguration
    String jobClass
    File jobJar
    List<File> libJars = []
    List<String> args = []

    MapReduceExecTask() {
        super()
        this.clusterConfiguration = project.extensions.findByName('hadoopFixture') as HadoopClusterConfiguration
    }

    @TaskAction
    void submitJob() {
        // Verification
        if (clusterConfiguration == null) {
            // FIXHERE: Remove once we have a plugin and extension
            throw new GradleException("No cluster configuration found")
        }

        // Gateway conf
        InstanceConfiguration hadoopGateway = clusterConfiguration
                .service(HadoopClusterConfiguration.HADOOP)
                .role(HadoopServiceDescriptor.GATEWAY)
                .instance(0)

        File baseDir = hadoopGateway.getBaseDir()
        File homeDir = new File(baseDir, hadoopGateway.getServiceDescriptor().homeDirName(hadoopGateway))
        File binDir = new File(homeDir, hadoopGateway.serviceDescriptor.scriptDir(hadoopGateway))
        String commandName = 'yarn' // TODO: or yarn.cmd for Windows
        File command = new File(binDir, commandName)
        // bin/yarn jar job.jar full.class.name.Here <genericArgs> <args>
        List<String> commandLine = [command.toString(), 'jar', jobJar.toString(), jobClass]
        if (!libJars.isEmpty()) {
            commandLine.addAll(['-libjars', libJars.join(',')])
        }
        if (!args.isEmpty()) {
            commandLine.addAll(args)
        }

        // Do command
        project.exec { ExecSpec spec ->
            spec.commandLine(commandLine)
//            spec.environment(finalEnv)
        }
    }
}

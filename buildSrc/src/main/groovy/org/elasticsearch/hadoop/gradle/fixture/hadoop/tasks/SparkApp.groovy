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
import org.elasticsearch.hadoop.gradle.fixture.hadoop.services.SparkYarnServiceDescriptor
import org.gradle.api.GradleException
import org.gradle.api.tasks.Input
import org.gradle.api.tasks.InputFile
import org.gradle.api.tasks.InputFiles
import org.gradle.api.tasks.Internal
import org.gradle.api.tasks.Optional
import org.gradle.api.tasks.TaskAction
import org.gradle.process.ExecOperations
import org.gradle.process.ExecSpec

import javax.inject.Inject

import static org.elasticsearch.hadoop.gradle.util.ObjectUtil.unapplyString

abstract class SparkApp extends AbstractClusterTask {

    enum Master {
        LOCAL, YARN, STANDALONE
    }

    enum DeployMode {
        CLIENT, CLUSTER
    }

    @Input
    String jobClass
    @InputFile
    File jobJar
    @Internal
    Master master = Master.YARN
    @Internal
    DeployMode deployMode = DeployMode.CLIENT
    @Input
    Map<String, Object> jobSettings = [:]
    @Input @Optional
    String principal
    @Input @Optional
    String keytab
    @InputFiles
    List<File> libJars = []
    @Input
    List<String> args = []

    @Internal
    ExecOperations execOperations

    @Inject
    SparkApp(ExecOperations execOperations) {
        this.execOperations = execOperations
    }

    void deployMode(DeployMode mode) {
        deployMode = mode
    }

    void deployModeClient() {
        deployMode = DeployMode.CLIENT
    }

    void deployModeCluster() {
        deployMode = DeployMode.CLUSTER
    }

    void jobSetting(String key, Object value) {
        jobSettings.put(key, value)
    }

    void jobSettings(Map<String, Object> configs) {
        jobSettings.putAll(configs)
    }

    void libJars(File... files) {
        libJars.addAll(files)
    }

    @Override
    InstanceConfiguration defaultInstance(HadoopClusterConfiguration clusterConfiguration) {
        return clusterConfiguration
                .service(HadoopClusterConfiguration.SPARK)
                .role(SparkYarnServiceDescriptor.GATEWAY)
                .instance(0)
    }

    @Override
    Map<String, String> taskEnvironmentVariables() {
        return [:]
    }

    @TaskAction
    void runSparkSubmit() {
        //Verification
        if (clusterConfiguration == null) {
            throw new GradleException("no cluster configuration found")
        }
        if (jobClass == null) {
            throw new GradleException("No job class given")
        }
        if (jobJar == null) {
            throw new GradleException("No job jar given")
        }

        // Gateway conf
        InstanceConfiguration sparkGateway = getInstance()

        File baseDir = sparkGateway.getBaseDir()
        File homeDir = new File(baseDir, sparkGateway.getServiceDescriptor().homeDirName(sparkGateway))
        File binDir = new File(homeDir, sparkGateway.getServiceDescriptor().scriptDir(sparkGateway))
        String commandName = 'spark-submit' // TODO: Windows?
        File command = new File(binDir, commandName)

        String argMaster = getMasterURL(sparkGateway)

        String argDeployMode = getDeployModeValue()

        // bin/spark-submit \
        //          --class <class> \
        //          --master yarn \
        //          --deploy-mode client \
        //          [--conf k=v] \
        //          [--jars <jar,jar>] \
        //          [--principal <principal> --keytab <keytab>] \
        //          path/to/jar.jar
        List<String> commandLine = [command.toString(),
                                    '--class', jobClass,
                                    '--master', argMaster,
                                    '--executor-memory=600m',
                                    '--deploy-mode', argDeployMode]

        if (!libJars.isEmpty()) {
            commandLine.addAll(['--jars', libJars.join(',')])
        }

        jobSettings.collect { k, v -> /$k=${unapplyString(v)}/ }.forEach { conf -> commandLine.add('--conf'); commandLine.add(conf) }

        if (DeployMode.CLUSTER.equals(deployMode) && (principal != null || keytab != null)) {
            if (principal == null || keytab == null) {
                throw new GradleException("Must specify both principal and keytab! Principal:[$principal] Keytab:[$keytab]")
            }
            commandLine.addAll(['--principal', principal, '--keytab', keytab])
        }

        commandLine.add(jobJar.toString())
        commandLine.addAll(args)

        // HADOOP_CONF_DIR=..../etc/hadoop
        Map<String, String> finalEnv = collectEnvVars()

        // Do command
        logger.info("Command Env: " + finalEnv)
        execOperations.exec { ExecSpec spec ->
            spec.commandLine(commandLine)
            spec.environment(finalEnv)
        }
    }

    private String getMasterURL(InstanceConfiguration sparkGateway) {
        if (master == Master.YARN) {
            return 'yarn'
        } else {
            // TODO: Eventually support standalone or local
            throw new GradleException("Unsupported Master mode $master")
        }
    }

    private String getDeployModeValue() {
        return deployMode.name().toLowerCase()
    }
}

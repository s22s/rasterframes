/*
 * This software is licensed under the Apache 2 license, quoted below.
 *
 * Copyright 2019 Astraea, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *     [http://www.apache.org/licenses/LICENSE-2.0]
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 *
 */

import sbt.KeyRanks.ASetting
import sbt.Keys.{`package`, _}
import sbt._
import complete.DefaultParsers._
import scala.sys.process.Process
import sbtassembly.AssemblyPlugin.autoImport.assembly

object PIPBuildPlugin extends AutoPlugin {
  override def trigger: PluginTrigger = allRequirements
  override def requires = RFAssemblyPlugin

  object autoImport {
    val Python = config("python")
    val pythonSource = settingKey[File]("Default Python source directory.").withRank(ASetting)
    val pythonCommand = settingKey[String]("Python command. Defaults to 'python'")
    val pySetup = inputKey[Int]("Run 'python setup.py <args>'. Returns exit code.")
  }
  import autoImport._

  val copyPySources = Def.task {
    val s = streams.value
    val src = (Compile / pythonSource).value
    val dest = (Python / target).value
    if (!dest.exists()) dest.mkdirs()
    s.log(s"Copying '$src' to '$dest'")
    IO.copyDirectory(src, dest)
    dest
  }

  val buildPyDist= Def.task {
    val buildDir = (Python / target).value
    val pyDist = (packageBin / artifactPath).value
    val retcode = pySetup.toTask(" build sdist --formats=zip").value
    if(retcode != 0) throw new RuntimeException(s"'python setup.py' returned $retcode")
    val art = (Python / packageBin / artifact).value
    val ver = version.value
    IO.move(buildDir / "dist" / s"${art.name}-$ver.zip", pyDist)
    pyDist
  }

  override def projectConfigurations: Seq[Configuration] = Seq(Python)

  override def projectSettings = Seq(
    assembly / test := {},
    pythonCommand := "python",
    pySetup := {
      val s = streams.value
      val wd = copyPySources.value
      val args = spaceDelimited("<args>").parsed
      val cmd = Seq(pythonCommand.value, "setup.py") ++ args
      val ver = version.value
      s.log.info(s"Running '${cmd.mkString(" ")}' in $wd")
      Process(cmd, wd, "RASTERFRAMES_VERSION" -> ver).!
    },
    Compile / pythonSource := (Compile / sourceDirectory).value / "python",
    Test / pythonSource := (Test / sourceDirectory).value / "python",
    Compile / `package` := (Compile / `package`).dependsOn(Python / packageBin).value
  ) ++
    inConfig(Python)(Seq(
      target := target.value / "python",
      packageBin := Def.sequential(
        Compile / packageBin,
        buildPyDist
      ).value,
      packageBin / artifact := {
        val java = (Compile / packageBin / artifact).value
        java.withType("zip").withClassifier(Some("python")).withExtension("zip")
      },
      packageBin / artifactPath := {
        val dest = (Compile / packageBin / artifactPath).value.getParentFile
        val art = (Python / packageBin / artifact).value
        val ver = version.value
        dest / s"${art.name}-python-$ver.zip"
      },
      test := {
        pySetup.toTask(" test").value
      }
    )) ++
    addArtifact(Python / packageBin / artifact, Python / packageBin)
}

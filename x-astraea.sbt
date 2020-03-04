import com.typesafe.sbt.git.DefaultReadableGit

// Internal Astraea-specific overides
val sha = new DefaultReadableGit(file(".")).withGit(_.headCommitSha)
ThisBuild / version := "0.9.0-astraea-" + sha.map(_.take(7)).getOrElse("0000000")
ThisBuild / credentials += Credentials(Path.userHome / ".sbt" / ".credentials")
publishTo := {
  val base = "https://s22s.mycloudrepo.io/repositories"
  if (isSnapshot.value)
    Some("Astraea Internal Snapshots" at s"$base/snapshots/")
  else
    Some("Astraea Internal Releases" at s"$base/releases/")
}

// Couldn't figure out why we have to call all these out explicitly.
// The above should have been sufficient based on my understanding of
// the precidence rules in sbt.
LocalProject("core") / publishTo := publishTo.value
LocalProject("datasource") / publishTo := publishTo.value
LocalProject("pyrasterframes") / publishTo := publishTo.value
LocalProject("experimental") / publishTo := publishTo.value



version := "0.1"

ThisBuild / scalaVersion := "2.12.0"
val sparkVersion = "3.0.0"

lazy val intuit = (project in file("."))
  .settings(
    name := "intuit",
    dependencies
  )

lazy val dependencies = libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "3.0.5",
  "org.apache.spark" %%  "spark-core" % sparkVersion,
  "org.apache.spark" %%  "spark-sql" % sparkVersion
)

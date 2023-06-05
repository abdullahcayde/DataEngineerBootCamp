
name := "Extract"

version := "1.0"

scalaVersion := "2.12.17"

//javacOptions ++= Seq("-source", "10", "-target", "10", "-bootclasspath", "/Library/Java/JavaVirtualMachines/jdk-10.0.2.jdk/Contents/Home/lib/rt.jar")

libraryDependencies ++= Seq(
  "com.lihaoyi" %% "requests" % "0.8.0",
  "org.apache.spark" %% "spark-sql" % "3.3.1"
)

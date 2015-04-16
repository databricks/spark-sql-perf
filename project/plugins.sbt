// You may use this file to add plugin dependencies for sbt.

resolvers += "Spark Packages repo" at "https://dl.bintray.com/spark-packages/maven/"

resolvers += "sonatype-releases" at "https://oss.sonatype.org/content/repositories/releases/"

addSbtPlugin("org.spark-packages" %% "sbt-spark-package" % "0.1.1")

addSbtPlugin("com.github.mpeltonen" % "sbt-idea" % "1.6.0")

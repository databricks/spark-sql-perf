package com.databricks.spark.sql.perf.mllib

import java.util.{ArrayList => AL}

import scala.concurrent.duration._
import scala.collection.JavaConverters._
import scala.io.Source

import scala.reflect._
import scala.reflect.runtime.universe._
import scala.util.{Try => STry, Success, Failure}

import org.yaml.snakeyaml.Yaml

import com.databricks.spark.sql.perf.{MLParams}


/**
 * The configuration information generated from reading a YAML file.
 *
 * @param output the output direct
 */
case class YamlConfig(
 output: String = "/tmp/result",
 timeout: Duration = 20.minutes,
 runnableBenchmarks: Seq[MLTest])

object YamlConfig {

  /**
   * Reads a string (assumed to contain a yaml description) and returns the configuration.
   */
  def readString(s: String): YamlConfig = {
    println(s)
    val yaml = new Yaml()
    val m = dict(yaml.load(s))
    val common = m.get("common").map(dict).getOrElse(Map.empty)
    println("common")
    println(m)
    val exps = m("benchmarks")
      .asInstanceOf[AL[Map[String, Any]]].asScala.map(dict).toSeq
    println("exps:")
    println(exps)
    val experiments = exps.flatMap { sd =>
      val name = sd("name").toString
      val params = sd.get("params").map(dict).getOrElse(Map.empty)
      val expParams = cartesian(common ++ params)
      for (c <- expParams) yield name -> c
    }
    println("exp parsed")
    println(experiments)
    val e2 = experiments.map { case (n, e) =>
      val e2 = ccFromMap.fromMap[MLParams](e, strict=true)
      val s = ccFromMap.loadExperiment(n).getOrElse {
        throw new Exception(s"Cannot find algorithm $n in the standard benchmark algorithms")
      }
      MLTest(s, e2)
    }
    var c = YamlConfig(runnableBenchmarks = e2)
    for (output <- m.get("output")) {
      c = c.copy(output = output.toString)
    }
    for (x <- m.get("timeoutSeconds")) {
      c = c.copy(timeout = x.toString.toInt.seconds)
    }
    c
  }

  /**
   * Reads a file (assumed to contain a yaml config).
   */
  def readFile(filename: String): YamlConfig = {
    readString(Source.fromFile(filename).mkString)
  }

  // Converts a java dictionary to a scala map.
  private def dict[T](d: T): Map[String, Any] = {
    d.asInstanceOf[java.util.Map[String, Any]].asScala.toMap
  }

  /**
   * Given keys that may be lists, builds the cartesian product of all the values into defined
   * options.
   *
   * For example: {a: [1,2], b: [3,4]} -> {a: 1, b: 3}, {a: 1, b:4}, {a:2, b:3}, ...
   *
   * @return
   */
  private def cartesian(m: Map[String, Any]): Seq[Map[String, Any]] = {
    if (m.isEmpty) {
      Seq(m)
    } else {
      val k = m.keys.head
      val sub = m - k
      val l = cartesian(sub)
      m(k) match {
        case a: AL[_] =>
          for {
            x <- a.asScala.toSeq
            m2 <- l
          } yield {
            m2 ++ Map(k -> x.asInstanceOf[Any])
          }
        case _ =>
          val v = m(k)
          l.map { m => m ++ Map(k -> v) }
      }
    }
  }

}

// Some ugly internals to make simple constructs
object ccFromMap {
  // Builds a case class from a map.
  // (taken from stack overflow)
  // if strict, will report an error if some unknown arguments are passed to the constructor
  def fromMap[T: TypeTag: ClassTag](m: Map[String,_], strict: Boolean) = {
    scala.reflect.runtime.universe
    val rm = runtimeMirror(classTag[T].runtimeClass.getClassLoader)
    val classTest = typeOf[T].typeSymbol.asClass
    val classMirror = rm.reflectClass(classTest)
    val constructor = typeOf[T].declaration(nme.CONSTRUCTOR).asMethod
    val constructorMirror = classMirror.reflectConstructor(constructor)

    val constructorArgNames = constructor.paramss.flatten.map(_.name.toString).toSet
    val extraElements = m.keySet -- constructorArgNames
    if (extraElements.nonEmpty) {
      throw new Exception(s"Found extra arguments when instantiating an object of " +
        s"class ${classTest.asClass.toString}:" +
        s" ${extraElements.toSeq.sorted}")
    }

    val constructorArgs = constructor.paramss.flatten.map( (param: Symbol) => {
      val paramName = param.name.toString
      if(param.typeSignature <:< typeOf[Option[Long]])
        OptionImplicits.checkLong(m.get(paramName).asInstanceOf[Option[Long]])
      else if(param.typeSignature <:< typeOf[Option[Double]])
        OptionImplicits.checkDouble(m.get(paramName).asInstanceOf[Option[Double]])
      else if(param.typeSignature <:< typeOf[Option[Any]])
        m.get(paramName)
      else
        m.get(paramName).getOrElse(throw new IllegalArgumentException("Map is missing required parameter named " + paramName))
    })

    val res = constructorMirror(constructorArgs:_*).asInstanceOf[T]
    res
  }

  // TODO: handle scala.reflect.internal.MissingRequirementError
  private def load(name: String): STry[BenchmarkAlgorithm] = {
    val rm = runtimeMirror(getClass.getClassLoader)
    try {
      val module = rm.staticModule("com.databricks.spark.sql.perf.mllib." + name)
      val obj = rm.reflectModule(module)
      Success(obj.instance.asInstanceOf[BenchmarkAlgorithm])
    } catch {
      case x: scala.reflect.internal.MissingRequirementError =>
        Failure(x)
    }
  }

  val defaultPackages = Seq(
    "",
    "com.databricks.spark.sql.perf.mllib"
  )

  def loadExperiment(
      name: String,
      searchPackages: Seq[String] = defaultPackages): Option[BenchmarkAlgorithm] = {
    searchPackages.view.flatMap { p =>
      val n = if (p.isEmpty) name else s"$p.$name"
      load(n).toOption
    } .headOption
  }
}

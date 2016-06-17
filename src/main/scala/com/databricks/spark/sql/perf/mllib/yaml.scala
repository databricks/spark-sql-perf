package com.databricks.spark.sql.perf.mllib

import java.util.{ArrayList => AL}

import com.databricks.spark.sql.perf.{ExtraMLTestParameters, MLTestParameters}
import org.yaml.snakeyaml.Yaml

import scala.beans.BeanProperty
import scala.concurrent.duration._
import scala.collection.JavaConverters._
import scala.io.Source

import scala.reflect._
import scala.reflect.runtime.universe._


case class YamlConfig(
 output: String = "/tmp/result",
 timeout: Duration = 20.minutes,
 runnableBenchmarks: Seq[MLBenchmark])

package object ccFromMap {
  def fromMap[T: TypeTag: ClassTag](m: Map[String,_]) = {

    scala.reflect.runtime.universe
    val rm = runtimeMirror(classTag[T].runtimeClass.getClassLoader)
    val classTest = typeOf[T].typeSymbol.asClass
    val classMirror = rm.reflectClass(classTest)
    val constructor = typeOf[T].declaration(nme.CONSTRUCTOR).asMethod
    val constructorMirror = classMirror.reflectConstructor(constructor)

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

    constructorMirror(constructorArgs:_*).asInstanceOf[T]
  }

  // TODO: handle scala.reflect.internal.MissingRequirementError
  def loadExperiment(name: String): ClassificationPipelineDescription = {
    val rm = runtimeMirror(getClass.getClassLoader)
    val module = rm.staticModule("com.databricks.spark.sql.perf.mllib." + name)
    val obj = rm.reflectModule(module)
    obj.instance.asInstanceOf[ClassificationPipelineDescription]
  }
}

object YamlConfig {

  def dict[T](d: T): Map[String, Any] = d.asInstanceOf[java.util.Map[String, Any]].asScala.toMap

  def cartesian(m: Map[String, Any]): Seq[Map[String, Any]] = {
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
      val com = sd.get("common").map(dict).getOrElse(Map.empty)
      val extra = sd.get("extra").map(dict).getOrElse(Map.empty)
      val allCommons = cartesian(common ++ com)
      val allExtra = cartesian(extra)
      for {
        c <- allCommons
        e <- allExtra
      } yield (name, c, e)
    }
    println("exp parsed")
    println(experiments)
    val e2 = experiments.map { case (n, c, e) =>
      val c2 = ccFromMap.fromMap[MLTestParameters](c)
      val e2 = ccFromMap.fromMap[ExtraMLTestParameters](e)
      val s = ccFromMap.loadExperiment(n)
      MLBenchmark(s, c2, e2)
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

  def readFile(filename: String): YamlConfig = {
    readString(Source.fromFile(filename).mkString)
  }
}
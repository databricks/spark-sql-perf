package com.databricks.spark.sql.perf.mllib

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

/** Exposes methods to simplify implementation of classes like MLParams. */
private[perf] object ReflectionUtils {

  private def getConstructor[T: TypeTag: ClassTag](obj: T): MethodSymbol = {
    typeOf[T].declaration(nme.CONSTRUCTOR).asMethod
  }

  /**
   * Given an instance [[obj]] of a class whose constructor arguments are all of type Option[Any],
   * returns a map of key-value pairs (argName -> argValue) where argName is the name
   * of a constructor argument with a defined (not None) value and argValue is the corresponding
   * value.
   */
  def getConstructorArgs[T: TypeTag: ClassTag](obj: T): Map[String, Any] = {
    // Get constructor of passed-in instance
    val constructor = getConstructor(obj)
    // Include each constructor argument not equal to None in the output map
    constructor.paramss.flatten.flatMap { (param: Symbol) =>
      // Get name and value of the constructor argument
      val paramName = param.name.toString
      val getter = obj.getClass.getDeclaredField(paramName)
      getter.setAccessible(true)
      val paramValue = getter.get(obj)
      // If the constructor argument is defined, include it in our output map
      paramValue match {
        case value: Option[Any] => if (value.isDefined) Seq(paramName -> paramValue) else Seq.empty
        case _ => throw new UnsupportedOperationException("ReflectionUtils.getConstructorArgs " +
          "can only be called on instances of classes whose constructor arguments are all of " +
          s"type Option[Any]; constructor argument ${paramName} had invalid type.")
      }
    }.toMap
  }

}

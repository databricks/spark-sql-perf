package com.databricks.spark.sql.perf.mllib

import scala.language.implicitConversions

/**
 * Implicits to transparently convert some Option[X] to X and vice-versa.
 *
 * This is usually dangerous to do, but in our case, the config is expressed through Options and
 * it alleviates the need to manually box values.
 */
object OptionImplicits {
  // The following implicits are unrolled for safety:
  private def oX2X[A](x: Option[A]): A = x.get

  def checkLong(x: Option[Long]): Option[Long] = {
    x.asInstanceOf[Option[Any]] match {
      case Some(u: java.lang.Integer) => Some(u.toLong)
      case Some(u: java.lang.Long) => Some(u.toLong)
      case _ => x
    }
  }

  def checkDouble(x: Option[Double]): Option[Double] = {
    x.asInstanceOf[Option[Any]] match {
      case Some(u: java.lang.Integer) => Some(u.toDouble)
      case Some(u: java.lang.Long) => Some(u.toDouble)
      case Some(u: java.lang.Double) => Some(u.toDouble)
      case _ => x
    }
  }

  implicit def oD2D(x: Option[Double]): Double = oX2X(x)

  implicit def oS2S(x: Option[String]): String = oX2X(x)

  implicit def oI2I(x: Option[Int]): Int = oX2X(x)

  implicit def oL2L(x: Option[Long]): Long = oX2X(x)

  implicit def l2lo(x: Long): Option[Long] = checkLong(Option(x))
  implicit def i2lo(x: Int): Option[Long] = Option(x.toLong)
  implicit def i2io(x: Int): Option[Int] = Option(x)
  implicit def d2do(x: Double): Option[Double] = Option(x)
  implicit def i2do(x: Int): Option[Double] = Option(x)
}
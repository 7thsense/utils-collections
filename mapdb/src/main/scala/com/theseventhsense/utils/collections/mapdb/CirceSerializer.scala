package com.theseventhsense.utils.collections.mapdb

import java.io.{ObjectInput, _}

import cats.implicits._
import com.theseventhsense.utils.collections.mapdb.MapDBOffHeapIterator.SerializerProvider
import io.circe._
import org.mapdb.Serializer

/**
 * Created by erik on 3/29/16.
 */

object PlaceHolderSerializer extends Serializable {}

class CirceSerializer[A](implicit encoder: Encoder[A], decoder: Decoder[A]) extends Serializer[A] with Externalizable {

  override def readExternal(in: ObjectInput): Unit = {}

  override def writeExternal(out: ObjectOutput): Unit = {}

  override def fixedSize(): Int = -1

  def readLines(in: DataInput): String = {
    try {
      val inputLine = new StringBuffer()
      var tmp: String = in.readLine
      while (Option(tmp).isDefined) {
        inputLine.append(tmp)
        tmp = in.readLine
      }
      inputLine.toString
    } catch {
      case ioe: IOException ⇒ ""
    }
  }

  override def serialize(out: DataOutput, value: A): Unit = {
    out.writeUTF(encoder.apply(value).noSpaces)
  }

  override def deserialize(in: DataInput, available: Int): A = {
    val json = in.readUTF()
    parser.decode[A](json).getOrElse(throw new RuntimeException(s"Unable to decode $json"))
  }
}

object CirceSerializer {
  class CirceSerializerProvider[A](implicit encoder: Encoder[A], decoder: Decoder[A]) extends SerializerProvider[A] {
    def serializer: org.mapdb.Serializer[A] = new CirceSerializer[A]()
  }

  object Implicits {
    implicit def serializer[A](implicit encoder: Encoder[A], decoder: Decoder[A]): SerializerProvider[A] =
      new CirceSerializerProvider()
  }
}

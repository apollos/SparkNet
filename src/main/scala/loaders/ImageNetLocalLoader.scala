package loaders

import scala.collection.mutable._
import net.coobird.thumbnailator._
import java.io._
import javax.imageio.ImageIO

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Created by zak on 16-9-30.
  */
object Phase extends Enumeration {
  type Phase = Value
  val TRAIN,TEST = Value
}

import Phase._

class ImageNetLocalLoader(val imageNetDir : String) {
  val imageNetDir_ = imageNetDir
  var phase_ : Phase = null
  def setPhase(phase : Phase) = {
    phase_ = phase
  }

  def getLabels(labelsPath : String) : Map[String,Int] = {
    val labelsFile = new File(labelsPath)
    val labelsReader = new BufferedReader(new InputStreamReader(new FileInputStream(labelsFile)))
    val labelsMap : Map[String,Int] = Map[String,Int]()
    var line = labelsReader.readLine()
    while(line != null) {
      val Array(path,label) = line.split(" ")
      var filename:String = null
      if(phase_ == TRAIN) {
        filename = imageNetDir_.concat("train/").concat(path).concat(".JPEG")
      }
      else{
        filename = imageNetDir_.concat("val/").concat(path).concat(".JPEG")
      }
      labelsMap(filename) = label.toInt
      line = labelsReader.readLine()
    }
    labelsMap
  }

  def BufferedImageToByteArray(image: java.awt.image.BufferedImage) : Array[Byte] = {
    val height = image.getHeight()
    val width = image.getWidth()
    val pixels = image.getRGB(0, 0, width, height, null, 0, width)
    val result = new Array[Byte](3 * height * width)
    var row = 0
    while (row < height) {
      var col = 0
      while (col < width) {
        val rgb = pixels(row * width + col)
        result(0 * height * width + row * width + col) = ((rgb >> 16) & 0xFF).toByte
        result(1 * height * width + row * width + col) = ((rgb >> 8) & 0xFF).toByte
        result(2 * height * width + row * width + col) = (rgb & 0xFF).toByte
        col += 1
      }
      row += 1
    }
    result
  }

  def loadImages(labelsMap : Map[String,Int],height : Int,width : Int) :
  List[(Array[Byte],Int)] = {
    val byteLabelArr =
      labelsMap.map {
        case (filename, label) => {
          val file = new File(filename)
          val im = ImageIO.read(file)
          val resizedImage = Thumbnails.of(im).forceSize(width, height).asBufferedImage()
          val byteArr = BufferedImageToByteArray(resizedImage)
          (byteArr, label)
        }
      }
    byteLabelArr.toList
  }
  def loadRdd(sc : SparkContext,phase : Phase,height : Int = 256,width : Int = 256) : RDD[(Array[Byte],Int)] = {
    setPhase(phase)
    var labelsPath : String = null
    if(phase_ == TRAIN) {
      labelsPath = imageNetDir_.concat("train.txt")
    }
    else {
      labelsPath = imageNetDir_.concat("val.txt")
    }
    val labelsMap = getLabels(labelsPath)
    println(labelsMap.size)
    val byteLabelArr = loadImages(labelsMap,height,width)
    println(byteLabelArr.length)
    sc.parallelize(byteLabelArr)
  }
}

package main.scala

import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.Result
import java.text.SimpleDateFormat
import java.nio.ByteBuffer

case class StockType(date:java.sql.Date
    ,open:Double
    ,high:Double
    ,low:Double
    ,close:Double
    ,volume:Double
    ,adjclose:Double
    ,symbol:String){
 
  def toPut = {
    val put = new Put(Bytes.toBytes(date.toString() + " " + symbol))
    put.addColumn(StockType.cfInfo, StockType.colDate, Bytes.toBytes(date.toString()))
    put.addColumn(StockType.cfInfo, StockType.colOpen, Bytes.toBytes(open))
    put.addColumn(StockType.cfInfo, StockType.colClose, Bytes.toBytes(close))
    put.addColumn(StockType.cfInfo, StockType.colHigh, Bytes.toBytes(high))
    put.addColumn(StockType.cfInfo, StockType.colLow, Bytes.toBytes(low))
    put.addColumn(StockType.cfInfo, StockType.colAdjClose, Bytes.toBytes(adjclose))
    put.addColumn(StockType.cfInfo, StockType.colVol, Bytes.toBytes(volume))
    put.addColumn(StockType.cfInfo, StockType.colSymbol, Bytes.toBytes(symbol))
    put
  }
}

object StockType{  
  val cfInfo = Bytes.toBytes("info")
  val colDate = Bytes.toBytes("date")
  val colOpen = Bytes.toBytes("open")
  val colHigh = Bytes.toBytes("high")
  val colLow = Bytes.toBytes("low")
  val colClose = Bytes.toBytes("close")
  val colAdjClose = Bytes.toBytes("adjclose")
  val colVol = Bytes.toBytes("volume")
  val colSymbol = Bytes.toBytes("symbol")
  
  val sdf = new SimpleDateFormat("yyyy-MM-dd")
 
  
  def parse(result:Result):StockType = {

    val date = new java.sql.Date(sdf.parse(new String(result.getValue(cfInfo, colDate))).getTime)
    val open = ByteBuffer.wrap(result.getValue(cfInfo, colOpen)).getDouble
    val high = ByteBuffer.wrap(result.getValue(cfInfo, colHigh)).getDouble
    val low = ByteBuffer.wrap(result.getValue(cfInfo, colLow)).getDouble
    val close = ByteBuffer.wrap(result.getValue(cfInfo, colClose)).getDouble
    val adjClose = ByteBuffer.wrap(result.getValue(cfInfo, colAdjClose)).getDouble
    val volume = ByteBuffer.wrap(result.getValue(cfInfo, colVol)).getDouble
    val symbol = new String(result.getValue(cfInfo, colSymbol))
    
    StockType(date, open, high, low, close, adjClose, volume, symbol)
  }

}
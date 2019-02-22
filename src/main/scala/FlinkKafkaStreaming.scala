import java.util.Properties

import scala.collection.mutable.ListBuffer
import scala.io.Source

import org.apache.flink.annotation.Public
import org.apache.flink.annotation.PublicEvolving
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.createTypeInformation
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.flink.util.Collector
import org.apache.flink.api.common.io.OutputFormat
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.Put
import scala.util.Random
import java.io.ByteArrayOutputStream
import java.io.DataOutputStream
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem
import java.net.URI
import java.io.BufferedReader
import java.io.InputStreamReader
import java.text.SimpleDateFormat
import java.util.Calendar
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult
import org.apache.flink.streaming.api.windowing.triggers.Trigger.TriggerContext
import org.apache.flink.streaming.api.windowing.triggers.Trigger
import org.apache.flink.streaming.api.windowing.windows.Window
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows
import org.apache.flink.streaming.api.windowing.triggers.PurgingTrigger
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.apache.flink.api.common.state.ValueState
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.common.typeutils.base.StringSerializer

//import org.apache.hadoop.conf.Configuration

case class foldresult(filename: String, metadata: ListBuffer[FileMetadata], length_buffer: Int)
case class FileMetadata(Elapsed_Time: Double, Section: String, Chamber_1_Temp_mv: Double, Chamber_1_Temp_C: Double, Chamber_2_Temp_mv: Double,
  Chamber_2_Temp_C: Double, Vaporizer_Temp_mv: Double, Vaporizer_Temp_C: Double, Door_Temp_mv: Double, Door_Temp_C: Double, Condenser_Temp_mv: Double,
  Condenser_Temp_C: Double, Chamber_High_mv: Double, Chamber_High_Torr: Double, Chamber_Pressure_mv: Double, Chamber_Pressure_Torr: Double,
  Vaporizer_Press_mv: Double, Vaporizer_Press_Torr: Double, Plasma_Power_mv: Double, Plasma_Power_watts: Double, H2O2_Monitor: Double,
  Absorbtion: Double, H2O2_Concentration: Double, ATM_Switch: String, Vent_Valve: String, Vent_Sensor: String, Vacuum_Pump: String,
  Vacuum_Valve: String, Vac_ctrl_sensor: String, Chamber_1_Heater: String, Door_Heater: String, Vaporizer_Heater: String,
  Chamber_Heater_HI_LO: String, Plasma_Power: String, Main_Fan_Sensor: String, Door_Fan_Sensor: String, Door_Fan: String,
  Door_Position: String, Door_Lock_Condition: String, Door_Lock_Solenoid: String, Inlet_Valve: String, Inlet_Valve_Sensor: String,
  Transfer_valve: String, Transfer_valve_Sensor: String, H2O2_Lamp: String, Heartbeat: String, Backlight_Consrv: String, Alarm: String,
  Pic_Reset: String, IMS_Reset: String, CassetteCommand: String, CondenserCommand: String, Oil_Return_Valve: String,
  Oil_Return_Valve_Sensor: String, Condenser_Fan: String, Condenser_Fan_Sensor: String, Delivery_Valve_Sensor: String,
  Air_Pump_Sensor: String)
case class PumpdownPhase(filename: String, chamberpumpdown_record: List[FileMetadata])
case class FileSummaryAndDetailInfo(Type: String, KeyField: String, Value: String)
case class CompleteRecord(filename: String, record: FileSummaryAndDetailInfo)
object FlinkKafkaStreaming {
  val df = new SimpleDateFormat("dd/MM/yy HH:mm:ss");
  val CF_COLUMN_NAME = Bytes.toBytes("ASP")
  def customread(dirname: String): ListBuffer[(String, String, Boolean)] =
    {
      
      /*val filerootpath = "D:\\Users\\vignesh.i\\Documents\\sparkfiletest\\"
      val dummy = new ListBuffer[(String, String,Boolean)]
      val filecontent = new StringBuilder
      for (filename <- new java.io.File(filerootpath + dirname).listFiles) {
        println("filename ", filename)
        val filename_alone = filename.toString().substring(filename.toString().lastIndexOf("\\") + 1)
        var isprevlinelast_chamberpumpdown1=true
        var prev_line=""
        for (line <- Source.fromFile(filename).getLines) {

          //filecontent.append(line)
          //val filename_alone = filename.toString().substring(filename.toString().lastIndexOf("\\") + 1)

          try
          {
          if(line.split("\t")(1).trim.equals("Chamber Pumpdown 1"))
            isprevlinelast_chamberpumpdown1=false
          if(!line.split("\t")(1).trim.equals("Chamber Pumpdown 1") && !isprevlinelast_chamberpumpdown1)
          {
            isprevlinelast_chamberpumpdown1=true
            dummy += ((filename_alone,prev_line.trim(),true))
          }
        }

        catch
        {
          case e:Exception => ""
        }

        dummy += ((filename_alone, line.trim(),false))
        prev_line=line

        //dummy += ((filename_alone,emptyrecord,true ))
      }
      }
      dummy
      */
      val filecontent = new ListBuffer[(String, String,Boolean)]
      val conf: org.apache.hadoop.conf.Configuration = new org.apache.hadoop.conf.Configuration()
      conf.addResource(new Path("core-site.xml"))
      conf.addResource(new Path("hdfs-site.xml"))
      val fs = FileSystem.get(new URI("hdfs://clouderavm01.centralindia.cloudapp.azure.com:8020/"), conf)
      val fileStatus = fs.listStatus(new Path("hdfs://clouderavm01.centralindia.cloudapp.azure.com:8020/user/hdadmin/" + dirname));
      var bufferedreader: BufferedReader = null
      for (status <- fileStatus) {
        val filepath = status.getPath().toString()
        var isprevlinelast_chamberpumpdown1=true
        var prev_line=""
        println(df.format(Calendar.getInstance().getTime()) + " " + "filepath " + filepath)
        val filename_alone = filepath.substring(filepath.lastIndexOf("\\") + 1)
        val path = new Path(filepath)
        val in = fs.open(path)
        bufferedreader = new BufferedReader(new InputStreamReader(in))
        Stream.continually(bufferedreader.readLine()).takeWhile(_ ne null) foreach { line =>
          if (!line.trim().isEmpty()) {
              try
          {
          if(line.split("\t")(1).trim.equals("Chamber Pumpdown 1"))
            isprevlinelast_chamberpumpdown1=false
          if(!line.split("\t")(1).trim.equals("Chamber Pumpdown 1") && !isprevlinelast_chamberpumpdown1)
          {
            isprevlinelast_chamberpumpdown1=true
            filecontent += ((filename_alone,prev_line.trim(),true))
          }
        }

        catch
        {
          case e:Exception => ""
        }

        filecontent += ((filename_alone, line.trim(),false))
        prev_line=line
        //filecontent += ((filename_alone, line.trim()))
          }
        }

      }
      filecontent
    }

  def PrepareRowKey(saltkey:Byte,diag_req_id: Int, diag_req_fileid: Int, RevTime: Long): Array[Byte] = {
    
    
    val Key1 = diag_req_id

    val Key2 = diag_req_fileid

    val Key3 = RevTime

    val byte1 = new ByteArrayOutputStream();
    val data = new DataOutputStream(byte1);
    
    data.writeByte(saltkey)
    data.writeInt(Key1)
    data.writeInt(Key2)
    data.writeLong(Key3)

    val p = byte1.toByteArray();
    p
  }
  class CustomFlatMapper extends FlatMapFunction[(String, FileMetadata), (String, String)] {

    override def flatMap(value: (String, FileMetadata), out: Collector[(String, String)]): Unit = {
      out.collect("", "")

    }
  }

  class FileAggregateTrigger[W <: Window] extends Trigger[(String, FileMetadata,Boolean), W] {

    override def onElement(element: (String, FileMetadata,Boolean), timestamp: Long, window: W, ctx: TriggerContext): TriggerResult = {

      if(element._3)
        TriggerResult.FIRE
      else
        TriggerResult.CONTINUE
    }

    override def onProcessingTime(time: Long, window: W, ctx: TriggerContext): TriggerResult = {
      TriggerResult.CONTINUE
    }
    override def onEventTime(time: Long, window: W, ctx: TriggerContext): TriggerResult = {
      TriggerResult.CONTINUE
    }

    override def clear(window: W, ctx: TriggerContext) = ???
  }

  class HBaseOutputFormat extends OutputFormat[CompleteRecord] {

    var conf: org.apache.hadoop.conf.Configuration = null;
    var table: HTable = null;

    def configure(parameters: Configuration) {
      val hbaseconffile = new Path("hbase-site.xml")
      conf = HBaseConfiguration.create()
      conf.addResource(hbaseconffile)
      
    }

    def open(taskNumber: Int, numTasks: Int) {
      table = new HTable(conf, "RESULT_DATA");

    }

    def writeRecord(record: CompleteRecord) {
      val reverse_timestamp = Long.MaxValue - System.currentTimeMillis()
      val randomnumber = Random.nextInt(3)

      val rowkey = PrepareRowKey(Random.nextInt(9).toByte,Random.nextInt(3), 123, reverse_timestamp)
      val put = new Put(rowkey)
      //put.add(Bytes.toBytes("entry"), Bytes.toBytes("entry"),Bytes.toBytes());
      put.addColumn(Bytes.toBytes("BDA"), Bytes.toBytes(record.record.KeyField), Bytes.toBytes(record.record.Value))
      table.put(put)
    }

    def close() {
      table.flushCommits();
      table.close();
    }

  }

  class HBaseOutputFormat1 extends OutputFormat[Array[String]] {

    var conf: org.apache.hadoop.conf.Configuration = null;
    var table: HTable = null;

    def configure(parameters: Configuration) {
      val hbaseconffile = new Path("hbase-site.xml")
      conf = HBaseConfiguration.create()
      conf.addResource(hbaseconffile)
      //conf = HBaseConfiguration.create();
    }

    def open(taskNumber: Int, numTasks: Int) {
      table = new HTable(conf, "TEST_DATA1");

    }

    def writeRecord(mydata: Array[String]) {
      //val mydata=record._2
      val reverse_timestamp = Long.MaxValue - System.currentTimeMillis()
      val randomnumber = Random.nextInt(3)

      val rowkey = PrepareRowKey(Random.nextInt(9).toByte,Random.nextInt(3), 123, reverse_timestamp)
      val p = new Put(rowkey)
      //put.add(Bytes.toBytes("entry"), Bytes.toBytes("entry"),Bytes.toBytes());
      p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("SECTION_NAME"), Bytes.toBytes(mydata(1).trim()))
      p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("CHAMBER_1_TEMP"), Bytes.toBytes(mydata(3).trim().toFloat))
      p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("CHAMBER_2_TEMP"), Bytes.toBytes(mydata(5).trim().toFloat))
      p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("VAPORIZER_TEMP"), Bytes.toBytes(mydata(7).trim().toFloat))
      p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("DOOR_TEMP"), Bytes.toBytes(mydata(9).trim().toFloat))
      p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("CONDENSER_TEMP"), Bytes.toBytes(mydata(11).trim().toFloat))
      p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("CHAMBER_HIGH"), Bytes.toBytes(mydata(12).trim().toDouble))
      p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("CHAMBER_PRESSURE"), Bytes.toBytes(mydata(14).trim().toDouble))
      p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("VAPORIZER_PRESSURE"), Bytes.toBytes(mydata(16).trim().toDouble))
      p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("PLASMA_POWER"), Bytes.toBytes(mydata(18).trim().toDouble))
      p.addColumn(CF_COLUMN_NAME, Bytes.toBytes("H2O2_MONITOR"), Bytes.toBytes(mydata(20).trim().toDouble))
      table.put(p)
    }

    def close() {
      table.flushCommits();
      table.close();
    }

  }

  

  class ReduceToList extends ProcessFunction[(String, FileMetadata), (String, ListBuffer[FileMetadata])] {
    val mylist = new ListBuffer[FileMetadata]
    override def processElement(value: (String, FileMetadata), ctx: ProcessFunction[(String, FileMetadata), (String, ListBuffer[FileMetadata])]#Context, out: Collector[(String, ListBuffer[FileMetadata])]): Unit = {
      mylist += value._2
      out.collect(value._1, mylist)
    }
  }

  def main(args: Array[String]) {
    val regex = "^\\d+(\\.\\d+)?".r
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val parameters = new Configuration()
    parameters.setBoolean("recursive.file.enumeration", true)
    val properties = new Properties();
    //properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("bootstrap.servers", "clouderavm01.centralindia.cloudapp.azure.com:9092,clouderavm02.centralindia.cloudapp.azure.com:9092,clouderavm03.centralindia.cloudapp.azure.com:9092")
    properties.setProperty("group.id", "test-consumer-group")
    val stream = env
      .addSource(new FlinkKafkaConsumer010[String]("test13", new SimpleStringSchema(), properties))
    val filecontent = stream.flatMap(x => customread(x)).map(fun => (fun._1, fun._2.split("\t"),fun._3))

    val filecontent_onlydigits = filecontent.filter { x => regex.pattern.matcher(x._2(0).trim()).matches() }
    val writedata = filecontent_onlydigits

    val whole_data = filecontent_onlydigits.map(func => {
      val filename = func._1
      val metadata = func._2
      val last = func._3
      //(filename, FileMetadata(metadata(0).toDouble, metadata(1).trim(), metadata(2).toDouble, metadata(3).toDouble, metadata(4).toDouble, metadata(5).toDouble, metadata(6).toDouble, metadata(7).toDouble, metadata(8).toDouble, metadata(9).toDouble, metadata(10).toDouble, metadata(11).toDouble, metadata(12).toDouble, metadata(13).toDouble, metadata(14).toDouble, metadata(15).toDouble, metadata(16).toDouble, metadata(17).toDouble, metadata(18).toDouble, metadata(19).toDouble, metadata(20).toDouble, metadata(21).toDouble, metadata(22).toDouble, metadata(23), metadata(24), metadata(25), metadata(26), metadata(27), metadata(28), metadata(29), metadata(30), metadata(31), metadata(32), metadata(33), metadata(34), metadata(35), metadata(36), metadata(37), metadata(38), metadata(39), metadata(40), metadata(41), metadata(42), metadata(43), metadata(44), metadata(45), metadata(46), metadata(47), metadata(48), metadata(49), metadata(50), metadata(51), metadata(52), metadata(53), metadata(54), metadata(55), metadata(56), metadata(57)))
      (filename, FileMetadata(metadata(0).toDouble, metadata(1).trim(), metadata(2).toDouble, metadata(3).toDouble, metadata(4).toDouble, metadata(5).toDouble, metadata(6).toDouble, metadata(7).toDouble, metadata(8).toDouble, metadata(9).toDouble, metadata(10).toDouble, metadata(11).toDouble, metadata(12).toDouble, metadata(13).toDouble, metadata(14).toDouble, metadata(15).toDouble, metadata(16).toDouble, metadata(17).toDouble, metadata(18).toDouble, metadata(19).toDouble, metadata(20).toDouble, metadata(21).toDouble, metadata(22).toDouble, metadata(23), metadata(24), metadata(25), metadata(26), metadata(27), metadata(28), metadata(29), metadata(30), metadata(31), metadata(32), metadata(33), metadata(34), metadata(35), metadata(36), metadata(37), metadata(38), metadata(39), metadata(40), metadata(41), metadata(42), metadata(43), metadata(44), metadata(45), metadata(46), metadata(47), metadata(48), metadata(49), metadata(50), metadata(51), metadata(52), metadata(53), metadata(54), metadata(55), metadata(56), metadata(57)),last)
    })
    //whole_data.print()
    val emptylistbuffer = new ListBuffer[FileMetadata]
    //val chamb_pumpdown11 = whole_data.filter(x => x._2.Section.equals("Chamber Pumpdown 1")).keyBy(0).window(TumblingProcessingTimeWindows.of(Time.seconds(args(0).toLong)))
    val chamb_pumpdown11 = whole_data.filter(x => x._2.Section.equals("Chamber Pumpdown 1")).keyBy(0).window(GlobalWindows.create()).trigger(PurgingTrigger.of(new FileAggregateTrigger[GlobalWindow]()))
      .fold(("", emptylistbuffer)) { case ((outputbuffer), b) => { (b._1, outputbuffer._2 += b._2) } }.map(x => new PumpdownPhase(x._1, x._2.toList))
    //chamb_pumpdown11.print()
    //.groupBy("_1")
    //.agg(functions.collect_list("_2") as "chamberpumpdown_record").withColumnRenamed("_1", "filename").as[PumpdownPhase]

    //    val c=chamb_pumpdown11.keyBy(0).window(TumblingProcessingTimeWindows.of(Time.seconds(1)))
    //   c.fold(("",emptylistbuffer)){case((outputbuffer),b) => {(b._1,outputbuffer._2+=b._2)}}.map(x => new PumpdownPhase(x._1,x._2.toList)).print()

    val FileSummaryList1 = scala.collection.mutable.MutableList[(String, FileSummaryAndDetailInfo)]()
    val chamb_pumpdown11_res = chamb_pumpdown11.map { pumpdownphase =>
      val firstrow_chamb_pumpdown1 = pumpdownphase.chamberpumpdown_record.head //First row of chamber pumpdown1
      val chamb_pumpdown1_timestart = firstrow_chamb_pumpdown1.Elapsed_Time //Starting time of chamber pumpdown1
      val lastrow_chamb_pumpdown1 = pumpdownphase.chamberpumpdown_record.last //Last row of chamber pumpdown1
      val chamb_pumpdown1_timeend = lastrow_chamb_pumpdown1.Elapsed_Time //End time of chamber pumpdown1
      //Total Chamber pumpdown time calculation
      val chamb_pumpdown1_time = BigDecimal(chamb_pumpdown1_timeend - chamb_pumpdown1_timestart).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble //Total chamber  pumpdown1 time

      //TE_Rate_1 Calculation
      //VpStart and TStart calculation for TERate1
      //VpStart is the vaporizer pressure when the chamber pumpdown1 starts
      //TStart is the vaporizer pressure when the chamber pumpdown1 starts
      val TStart = chamb_pumpdown1_timestart
      val VpStart = firstrow_chamb_pumpdown1.Vaporizer_Press_Torr
      //If during chamber pumpdown1 stage if the vaporizer pressure crosses 28Torr then that would be VpEnd and that time would be TEnd
      //If there is no vaporizer pressure exceeding 28Torr during chamber pumpdown1 stage then VpEnd will be end vaporizer pressure when the chamber pumpdown1 ends\
      //and TEnd will be the time when chamber pumpdown1 ends
      val VpEnd_greater28 = pumpdownphase.chamberpumpdown_record.filter { x => x.Vaporizer_Press_Torr > 28 }

      var VpEnd = 0d
      var TEnd = 0d
      if (VpEnd_greater28.isEmpty) {
        VpEnd = lastrow_chamb_pumpdown1.Vaporizer_Press_Torr
        TEnd = lastrow_chamb_pumpdown1.Elapsed_Time
      } else {
        VpEnd = VpEnd_greater28(0).Vaporizer_Press_Torr
        TEnd = VpEnd_greater28(0).Elapsed_Time
      }
      val TE_Rate_1_beforeround = (60 * (VpEnd - VpStart) / (TEnd - TStart))
      println(df.format(Calendar.getInstance().getTime()) + " " + "TE_Rate_1_beforeround " + TE_Rate_1_beforeround)
      val TE_Rate_1 = BigDecimal(TE_Rate_1_beforeround).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble

      if (TE_Rate_1 > 15) {
        FileSummaryList1 += ((pumpdownphase.filename, FileSummaryAndDetailInfo("Summary", "Suspect_Component_Failure", "Yes")))
        val inference = "Vaporizer Pressure Rise Rate during <strong>Chamber Pumpdown 1</strong> is <strong>" + TE_Rate_1 + "</strong>"
        FileSummaryList1 += ((pumpdownphase.filename, FileSummaryAndDetailInfo("Algorithm", "DiagnosticReqFiles.$.DiagnosticReqAnalysisParams.3.", inference)))
      }
      //Adding required fields for mongo insertion
      FileSummaryList1 += ((pumpdownphase.filename, FileSummaryAndDetailInfo("Summary", "Chamber_Pumpdown_1_Time", chamb_pumpdown1_time.toString())))
      FileSummaryList1 += ((pumpdownphase.filename, FileSummaryAndDetailInfo("Summary", "TE_Rate_1", TE_Rate_1.toString())))
    }
    val chamb_pumpdown11_listres = chamb_pumpdown11_res.flatMap { x => x }.map(values => CompleteRecord(values._1, values._2))
    val resultgrouped_iffailure = chamb_pumpdown11_listres.filter(x => x.record.KeyField.equals("Suspect_Component_Failure")).rebalance

    resultgrouped_iffailure.writeUsingOutputFormat(new HBaseOutputFormat)
    //resultgrouped_iffailure.print()
    writedata.map(fun => fun._2).rebalance.writeUsingOutputFormat(new HBaseOutputFormat1)
   
    env.execute("FlinkKafkaStreaming")
  }
}

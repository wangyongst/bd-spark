package com.myweb.spark

import org.apache.spark.sql.{DataFrame, SQLContext}

object Procedure {

  def PROC_UP_HDP(t_DOWNPLANCODE: String, t_result: Int, sqlContext: SQLContext): Unit = {
  }

  def PROC_DOWN_STS_20180118(t_DOWNPLANCODE: String, t_result: Int, sqlContext: SQLContext): Int = {
    var v_where: String = " and 1=1 "
    var sql = "select a.comcode,a.dataprovince,a.datacity,a.listtype,a.infotype,a.gettype,a.isptninput,a.isselfcreate,a.datesource,a.downcount,a.smakedate,a.emakedate,a.slatestcontactdate,a.elatestcontactdate,a.sbuydate,a.ebuydate,a.ssenddate,a.esenddate,a.inputdate,a.qxflag,a.labeltyd,a.labelfor,a.string4,a.string5,a.string6,a.string7,a.string8,a.string9,a.string10,a.string11,a.string12,a.date1,a.date1l,a.date2,a.date2l,a.string13,a.string14 from dp_down_plan a where a.downplancode = t_DOWNPLANCODE";
    var selectOut: DataFrame = sqlContext.sql(sql)
    if (selectOut.select("labelfor").first().get(0) != null) {
      v_where = v_where + " and a.phone = " + selectOut.select("labelfor").first().get(0)
    }
    if (selectOut.select("comcode").first().get(0) != null) {
      v_where = v_where + " and a.comcode = " + selectOut.select("comcode").first().get(0)
    }
    if (selectOut.select("infotype").first().get(0) != null) {
      v_where = v_where + " and a.infotype = " + selectOut.select("infotype").first().get(0)
    }
    if (selectOut.select("gettype").first().get(0) != null) {
      v_where = v_where + " and a.gettype = " + selectOut.select("gettype").first().get(0)
    }
    if (selectOut.select("dataprovince").first().get(0) != null) {
      v_where = v_where + " and a.dataprovince = " + selectOut.select("dataprovince").first().get(0)
    }
    if (selectOut.select("datacity").first().get(0) != null) {
      v_where = v_where + " and a.datacity = " + selectOut.select("datacity").first().get(0)
    }
    if (selectOut.select("isptninput").first().get(0) != null) {
      v_where = v_where + " and a.isptninput = " + selectOut.select("isptninput").first().get(0)
    }
    if (selectOut.select("isselfcreate").first().get(0) != null) {
      v_where = v_where + " and a.isselfcreate = " + selectOut.select("isselfcreate").first().get(0)
    }
    if (selectOut.select("labelfir").first().get(0) != null) {
      v_where = v_where + " and a.labelfir = " + selectOut.select("isselfcreate").first().get(0)
    }
    if (selectOut.select("string4").first().get(0) != null) {
      v_where = v_where + " and a.guidenum = " + selectOut.select("string4").first().get(0)
    }
    if (selectOut.select("string5").first().get(0) != null) {
      v_where = v_where + " and a.string3 = " + selectOut.select("string5").first().get(0)
    }
    if (selectOut.select("string9").first().get(0) != null) {
      v_where = v_where + " and a.string7 = " + selectOut.select("string9").first().get(0)
    }
    if (selectOut.select("string6").first().get(0) != null) {
      v_where = v_where + " and a.string6 = " + selectOut.select("string6").first().get(0)
    }
    if (selectOut.select("string8").first().get(0) != null) {
      v_where = v_where + " and a.string10 = " + selectOut.select("string8").first().get(0)
    }
    if (selectOut.select("string9").first().get(0) != null) {
      v_where = v_where + " and a.string9 = " + selectOut.select("string9").first().get(0)
    }
    if (selectOut.select("string10").first().get(0) != null) {
      v_where = v_where + " and a.string25 = " + selectOut.select("string10").first().get(0)
    }
    if (selectOut.select("string11").first().get(0) != null) {
      v_where = v_where + " and a.string14 = " + selectOut.select("string11").first().get(0)
    }
    if (selectOut.select("string12").first().get(0) != null) {
      v_where = v_where + " and a.string26 = " + selectOut.select("string12").first().get(0)
    }
    if (selectOut.select("string13").first().get(0) != null) {
      v_where = v_where + " and a.string43 = " + selectOut.select("string13").first().get(0)
    }
    if (selectOut.select("string14").first().get(0) != null) {
      v_where = v_where + " and a.string42 = " + selectOut.select("string14").first().get(0)
    }
    sql = "select count(*) from dp_list_cust a where a.isblack='N'  and isptninput='N'  and a.zyoutdate is null and a.downstate='0' and a.string1='N'" + v_where
    var out1: DataFrame = sqlContext.sql(sql)
    out1.show()
    println(sql)

  }


    def PROC_INPUT_AGILE_MAIN(t_guidenum: String, t_result: Int, sqlContext: SQLContext): Unit = {

    println("查询入库批次对应操作人员和文件存储路径")
    var v_goto: String = "R"
    if (v_goto.equals("R")) {
      var sql = "select count(*) from dp_input_opt where uplpadstate = '1' and guidenum = '" + t_guidenum + "'"
      var v_countg: DataFrame = sqlContext.sql(sql)
      if (v_countg.first().get(0) > (0)) {
        var sql = "select filepath, operator,csvmodel,datapurpose into v_filepath, v_operator,v_csvmodel,v_datapurpose from dp_input_opt where uplpadstate = '1' and guidenum ='" + t_guidenum + "'"
        var dataFrame: DataFrame = sqlContext.sql(sql)
        println("不为空则创建表")
        if (dataFrame.first().get(0) != null) {
          if (dataFrame.first().get(3).equals("CC")) {
            sql = "drop table temp_duplicate_removal_csv_" + dataFrame.first().get(3).toString
            sqlContext.sql(sql)
            sql = "CREATE  TABLE temp_duplicate_removal_csv_" + dataFrame.first().get(3).toString + "(citycode STRING ,mphone STRING ,ptnname STRING ,PRIMARY KEY(citycode) ) PARTITION BY HASH PARTITIONS 4 STORED AS KUDU TBLPROPERTIES ('kudu.table_name' = 'temp_duplicate_removal_csv_',     'kudu.master_addresses' = 'ZHS-1:7051' );"
            sqlContext.sql(sql)
          }
        }
      }
    }
  }
}

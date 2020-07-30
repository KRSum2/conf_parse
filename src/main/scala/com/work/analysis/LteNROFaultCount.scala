package com.work.analysis

import com.conf.parse.common.{FSUtils, MiscUtils, RemoveFileUtil, TaskStartTime}
import com.conf.parse.config.Config
import com.work.common.{FSUtils, MiscUtils, RemoveFileUtil, TaskStartTime}
import com.work.config.Config
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext


class LteNROFaultCount extends Analysis {

	val log = MiscUtils.getLogger(classOf[LteNROFaultCount])


	/**
		* 运行分析程序
		*
		* @param fs
		* @param savePath 保存路径，已经加了"/"后缀，但是不包含表名
		* @param tst      任务开始时间，只到小时
		* @throws Exception
		*/
	override def run(fs: FileSystem, sqlContext: HiveContext, savePath: String, tst: TaskStartTime, jsc: JavaSparkContext): Unit = {
		val dayPartitionName = tst.dateStr

		// 初始化,读取数据
		val sc = JavaSparkContext.toSparkContext(jsc)
		//val sc = sqlContext.sparkContext
		//测试集群路径
		val path_nrt = Config.conf_parse_agg_lnnrwhole + "/LNNRWHOLE.csv"
		//邻区表数据(以后数据动态时需要修改)
		//val path_nrt = Config.conf_parse_agg_lnnrwhole + "/day=" + tst.dateStr + "/LNNRWHOLE.csv"
		val nrt_all=sc.textFile(path_nrt).map(a=>{
			val r=a.split("\\,",-1)
			Array(r(0), r(1), r(2), r(3), r(4), r(5), r(6), r(7), r(8), r(9), r(10), r(11), r(12), r(13), r(14), r(15), r(16), r(17), r(18), r(19), r(20), r(21), r(22))
		})
		//PCI混淆结果
		val pciPath = Config.conf_parse_agg_pcifault + "/day=" + dayPartitionName
		val rdd_pci = same_fcn_pci(nrt_all)
		writeRDD(fs, rdd_pci, pciPath)
		
		//PCI参数异常结果
		val argsPath = Config.conf_parse_agg_argsfault + "/day=" + dayPartitionName
		val rdd_conf = abnormal_args(nrt_all)
		writeRDD(fs, rdd_conf, argsPath)

		//整合新增问题
		/*val fault_rdd = rdd_pci.union(rdd_conf)
		fault_rdd.repartition(1).saveAsTextFile(path)*/
	}

	/**
	 * PCI混淆
	 * 需要数据：rdd_all
	 */
	def same_fcn_pci(nrt_all: RDD[Array[String]]): RDD[String] = {
			val rdd_1 = nrt_all.filter(r=>r(22)=="INTRA")		//筛选同频数据
					val rdd1=rdd_1.map(r=>((r(0),r(2),r(15)),1))
					.reduceByKey(_+_)								              	//计算(sc_ENODEBID,sc_CELLID,nc_PHYCELLID)相同数据行数
					.filter(r=>(r._2>1&&r._2<3))
					.map(r=>((r._1._1,r._1._2),r._1._3))

					val rdd_sc1 = rdd1.map(r=>((r._1._1,r._1._2),1))
					.reduceByKey(_+_)								              	//计算(sc_ENODEBID,sc_LOCALCELLID)相同数据行数
					.map(a=>(a._1,a._2))
					.filter(r=>(r._2<2))
					.leftOuterJoin(rdd1)
					.filter(r=>r._2._2.nonEmpty)
					.map(r=>((r._1._1,r._1._2,r._2._2.get),1))

					val rdd_check = rdd_1.map(r=>((r(0), r(2), r(15)),(r(0), r(1), r(2), r(3), r(5), r(6),r(7), r(8), r(12), r(13), r(14), r(15), r(16), r(17),r(18))))

					val rdd_pci1 = rdd_sc1.leftOuterJoin(rdd_check)
					.filter(r=>r._2._2.get!= null)
					.map(r=>r._2._2.get)
					.map(r=>((r._1,r._3,r._9,r._12),1))
					.reduceByKey(_+_)
					.filter(r=>r._2<2)    //剔除nc_ENODEBID相同的数据

					val rdd_pci2 = rdd_pci1.map(r=>(r._1._1,1))
					.reduceByKey(_+_)
					.filter(r=>r._2<6)

					val rdd_pci3 = rdd_pci1.map(r=>(r._1._1,r._1))
					.leftOuterJoin(rdd_pci2)
					.filter(r=>r._2._2.nonEmpty)
					.map(r=>(r._2._1,1))

					val rdd_check2 = rdd_1.map(r=>((r(0), r(2), r(12),r(15)), (r(0), r(1), r(2), r(3), r(5), r(6),r(7), r(8), r(12), r(13), r(14), r(15), r(16), r(17),r(18))))		//((sc_ENODEBID,sc_LOCALCELLID,nc_ENODEBID,nc_PHYCELLID),r)

					val rdd_pci = rdd_pci3.leftOuterJoin(rdd_check2)	//((sc_ENODEBID,sc_LOCALCELLID,nc_ENODEBID,nc_PHYCELLID),(1,r))
					.filter(r=>r._2._2.nonEmpty)				        		//去除空值
					.map(r=>r._2._2.get)
					.map(r=>((r._1,r._3,r._12),r))
					.sortByKey()
					.map(r=>r._2)
					.map(r=>((r._1,r._2,r._3,r._4,r._5,r._6,r._7,r._8),(r._9,r._10,r._11,r._12,r._13,r._14,r._15)))
					.groupByKey()
					.map(r=>{
						var nc = ""
								var nc1 = ""
								var nc2 = ""
								var nc3 = ""
								var nc4 = ""
								var nc5 = ""
								var nc6 = ""
								var nc7 = ""
								r._2.foreach(r=>{
									nc1 = r._1
											nc2 = r._2
											nc3 = r._3
											nc4 = r._4
											nc5 = r._5
											nc6 = r._6
											nc7 = r._7
											nc= nc+","+nc1+","+nc2+","+nc3+","+nc4+","+nc5+","+nc6+","+nc7
								})
								r._1._1+","+r._1._2+","+r._1._3+","+r._1._4+","+r._1._5+","+r._1._6+","+r._1._7+","+r._1._8+",PCI混淆"+ nc
					})
					rdd_pci

	}

	/**
		* PCI配置异常
		* 需要数据：rdd_all
		*/
	def abnormal_args(nrt_all: RDD[Array[String]]): RDD[String] = {

		val rdd_sc = nrt_all.map(r=>(r(0), r(2), r(5))).distinct()			//主服务小区配置数据
			.map(r=>((r._1,r._2),r._3))

		val rdd_nc = nrt_all.map(r=>(r(12), r(14), r(15))).distinct()		//邻接小区配置数据
			.filter(r=>(!(r.toString().contains("\\N"))))										  //去除含有空值的数据
			.map(r=>((r._1,r._2),r._3))

		val nrt_nc = nrt_all.map(r=>((r(12), r(14),r(15)),(r(0), r(1))))
		val nrt_sc = nrt_all.map(r=>((r(0),r(2),r(5)),(r(0),r(1),r(2),r(3),r(5),r(6),r(7),r(8))))

		//以邻接小区为基准，列出所有(enodeBID,cellID)对应的本地小区和邻接小区的参数，去除null值
		val abnormal_args = rdd_nc.leftOuterJoin(rdd_sc)
			.filter(r=>r._2._2.nonEmpty)
			.filter(r=>r._2._1!=r._2._2.get)
			.map(r=>((r._1._1,r._1._2,r._2._1),r._2._2.get))
			.leftOuterJoin(nrt_nc)
			.filter(r=>r._2._2.nonEmpty)
			.map(r=>((r._1._1,r._1._2,r._1._3,r._2._1),r._2._2.get))
			.groupByKey()
			.map(r=>{
				val a1 = r._1._1
				val a2 = r._1._2
				val a3 = r._1._3
				val a4 = r._1._4
				var sc = ""
				var enodebid=""
				var enodebname=""
				r._2.foreach(r=>{
					enodebid = r._1
					enodebname = r._2
					sc= sc+"("+enodebid+"_"+enodebname+")"
				})
				((a1,a2,a4),(a3,sc))
			})
			.leftOuterJoin(nrt_sc)
			.distinct()
			.filter(r=>r._2._2.nonEmpty)
			.map(r=>{
				val sc = r._2._2.get
				sc._1+","+sc._2+","+sc._3+","+sc._4+","+r._2._1._1+","+sc._6+","+sc._7+","+sc._8+",PCI配置异常,"+sc._5+","+r._2._1._2
			})
		abnormal_args
	}

	//将字符串的rdd写出
	def writeRDD(fs: FileSystem, rdd: RDD[String], path: String) = {
		//将数据输出到文件
		var filterPath = new Path(path)
		if (FSUtils.exists(fs, path)) {
			//本地删除文件
			//RemoveFileUtil.rm(fs, filterPath, true, true)
			//集群删除文件
			RemoveFileUtil.rmByExecCmd(path)
		}
		rdd.repartition(1).saveAsTextFile(path)
	}


}
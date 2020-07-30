package com.conf.parse.common;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

/**
 * 计算频点
 * @author 赖书恒
 *
 */
public class CountDlearfcn {
	
	//装载固定值字典
	/*freBandInd：1、3、5
	对应的a1、a2：
	1---2110---0
	3---1805---1200
	5---869---2400*/
	public static Map<Integer, Value> map = new HashMap<Integer, Value>();
	
	public static String count(int freqBandInd, double earfcnDlText){
		//加载数据
		loadData(freqBandInd);
		//计算指标
		if(map.get(freqBandInd).a1 == 0){
			
		}
		BigDecimal b = new BigDecimal((earfcnDlText - map.get(freqBandInd).a1)/0.1 + map.get(freqBandInd).a2);   
		String sc_DLEARFCN = b.setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue() + "";
		return sc_DLEARFCN;
	}
	  
	public static void loadData(int freqBandInd){
		Value value = new Value();
		if(freqBandInd == 1){
			value.a1 = 2110;
			value.a2 = 0;
			map.put(freqBandInd, value);
		}else if(freqBandInd == 3){
			value.a1 = 1805;
			value.a2 = 1200;
			map.put(freqBandInd, value);
		}else if(freqBandInd == 5){
			value.a1 = 869;
			value.a2 = 2400;
			map.put(freqBandInd, value);
		}else if(freqBandInd == 26){
			value.a1 = 859;
			value.a2 = 8690;
			map.put(freqBandInd, value);
		}
	}
	
}


class Value{
	public double a1;
	public double a2;
}
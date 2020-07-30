package com.work.common;

public class DistanceUtil {
    private static double EARTH_RADIUS = 6370996.81;// 地球半径
    /// <param name="lat1">第一点纬度</param>
    /// <param name="lng1">第一点经度</param>
    /// <param name="lat2">第二点纬度</param>
    /// <param name="lng2">第二点经度</param>

    //返回的距离单位是M
    public static double GetDistance(double lat1, double lng1, double lat2, double lng2) {
        double radLat1 = Rad(lat1);
        double radLng1 = Rad(lng1);
        double radLat2 = Rad(lat2);
        double radLng2 = Rad(lng2);
        double a = radLat1 - radLat2;
        double b = radLng1 - radLng2;
        double result = 2 * Math.asin(Math.sqrt(Math.pow(Math.sin(a / 2), 2) + Math.cos(radLat1) * Math.cos(radLat2) * Math.pow(Math.sin(b / 2), 2))) * EARTH_RADIUS;
        return result;
    }
    private static double Rad(double d) {
        return d * Math.PI / 180.0;
    }

}

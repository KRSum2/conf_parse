package com.work.etl.ericehandler;

import com.conf.parse.etl.obj.*;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import java.util.ArrayList;
import java.util.List;

public class EriceHadler extends DefaultHandler {

    //保存基站信息，服务小区表，邻区小区表，邻区关系表的数据list集合
    List<Function> functionList = new ArrayList<>();            //存放基站list集合
    List<LocalCell> localCellList = new ArrayList<>();          //服务小区list集合
    List<AdjacencyCell> adjacencyCellList = new ArrayList<>();  //邻接小区list结婚
    List<NeighborhoodRelationship> neighborhoodRelationshipList = new ArrayList<>();    //邻区关系list结


    int vsDataType;                     //1表示基站 2表示服务小区  3表示邻接小区频点   4表示邻接关系
    String tagName;                     //标签名
    StringBuilder stringBuilder;        //存放标签的内容
    StringBuilder stringBuilder1;
    String vsDataContainerID;           //存放vsDataContainer标签的id属性的值
    String physicalLayerCellIdGroup;    //用于计算物理标识号
    String physicalLayerSubCellId;      //用于计算物理标识号
    String nc_DLEARFCN;                 //暂时存放邻接小区频点
    int i = 0;                          //计算何时把相应的基站类,服务小区类,邻接小区类,临街关系类添加到对应的list中,
    String MCC;
    String MNC;
    String TAC;


    Function function;                  //基站实体类
    LocalCell localCell;                //本地小区实体类
    AdjacencyCell adjacencyCell;        //邻接小区实体类
    NeighborhoodRelationship neighborhoodRelationship;  //邻接关系实体类

    public EriceHadler(List<Function> functionList, List<LocalCell> localCellList, List<AdjacencyCell> adjacencyCellList, List<NeighborhoodRelationship> neighborhoodRelationshipList) {
        this.functionList = functionList;
        this.localCellList = localCellList;
        this.adjacencyCellList = adjacencyCellList;
        this.neighborhoodRelationshipList = neighborhoodRelationshipList;
    }


//解析某个节点调用的方法

    @Override
    public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
        tagName = qName;
        stringBuilder = new StringBuilder();
        stringBuilder1 = new StringBuilder();

        //拿领接小区的频点
        if (qName.equals("xn:VsDataContainer")) {
            vsDataContainerID = attributes.getValue("id");
        }
    }

    //解析节点结束后调用的方法
    @Override
    public void endElement(String uri, String localName, String qName) throws SAXException {
        String str = stringBuilder.toString();

        switch (vsDataType) {
            case 1:
                if (tagName.equals("es:userLabel")) {
                    //基站的名称
                    function.sc_ENODEBFUNCTIONNAME = str;
                    i++;
                } else if (tagName.equals("es:sctpRef")) {
                    //基站地市
                    String temp1 = str;
                    String[] strings = temp1.split(",");
                    if (strings.length > 1) {
                        String[] strings1 = strings[1].split("=");
                        function.sc_LOCATIONNAME = getChineseName(strings1[1]);
                    }
                    i++;
                } else if (tagName.equals("es:eNBId")) {
                    //基站号
                    function.sc_ENODEBID = str;
                    i++;
                    //基站的厂家
                    function.sc_VENDORNAME = "ERICE";
                    i++;

                }else if(tagName.equals("es:mcc")){
                    MCC = str;
                    i++;
                }else if(tagName.equals("es:mnc")){
                    MNC = str;
                    i++;
                }
                if (i == 6) {
                    if(function.sc_ENODEBID==null || function.sc_ENODEBID.length()<6){
                        vsDataType = 0;
                        i = 0;
                        break;
                    }
                    functionList.add(function);
                    vsDataType = 0;
                    i = 0;
                }
                break;
            case 2:
                if (tagName.equals("es:userLabel")) {
                    //服务小区名字
                    localCell.sc_CELLNAME = vsDataContainerID;
                    i++;
                } else if (tagName.equals("es:earfcndl")) {
                    //服务小区频点
                    localCell.sc_DLEARFCN = str;
                    i++;
                } else if (tagName.equals("es:cellId")) {
                    //服务小区编号
                    localCell.sc_LOCALCELLID = str;
                    i++;
                } else if (tagName.equals("es:physicalLayerCellIdGroup")) {
                    physicalLayerCellIdGroup = str;
                    i++;
                } else if (tagName.equals("es:physicalLayerSubCellId")) {
                    physicalLayerSubCellId = str;
                    //服务小区物理标识号
                    //服务小区制式
                    localCell.sc_FDDTDDIND = "FDD";
                    //服务小区基站号
                    localCell.sc_ENODEBID = function.sc_ENODEBID;
                    i++;
                }else if (tagName.equals("es:tac")){
                    localCell.sc_TAC = str;
                    localCell.sc_MCC = MCC;
                    localCell.sc_MNC = MNC;
                    i++;
                }
                if (i == 6) {
                    //添加到list中
                    localCell.sc_PHYCELLID = ((new Integer(physicalLayerCellIdGroup)) * 3 + new Integer(physicalLayerSubCellId)) + "";
                    if(localCell.sc_ENODEBID==null || localCell.sc_ENODEBID.length()<6){
                        i = 0;
                        vsDataType = 0;
                        break;
                    }
                    localCellList.add(localCell);
                    i = 0;
                    vsDataType = 0;
                }
                break;
            case 3:
                //邻接小区频点
                nc_DLEARFCN = vsDataContainerID;
                vsDataType = 0;
                break;
            case 4:
                adjacencyCell.nc_DLEARFCN = nc_DLEARFCN;
                String[] strings = vsDataContainerID.split("-");
                //邻接小区基站号

                if (strings.length > 2) {
                    adjacencyCell.nc_ENODEBID = strings[1];
                    adjacencyCell.nc_CELLID = strings[2];
                }


                //添加到list中
                if(adjacencyCell.nc_ENODEBID==null || adjacencyCell.nc_ENODEBID.length()<6){
                    vsDataType = 0;
                    break;
                }
                adjacencyCellList.add(adjacencyCell);
                //这里重复添加的都是同一个adjacencyCell类  改变数据之后 前面的也会一起改变  想想有什么办法修改

                //邻接关系服务小区基站号
                neighborhoodRelationship.sc_ENODEBID = localCell.sc_ENODEBID;
                //邻接关系服务小区小区号
                neighborhoodRelationship.sc_LOCALCELLID = localCell.sc_LOCALCELLID;


                if (strings.length > 2) {
                    //邻接关系邻接小区基站号
                    neighborhoodRelationship.nc_ENODEBID = strings[1];
                    //邻接关系邻接小区小区号
                    neighborhoodRelationship.nc_CELLID = strings[2];
                }


                //邻接关系添加到list中
                if(neighborhoodRelationship.nc_ENODEBID==null || neighborhoodRelationship.sc_ENODEBID==null || neighborhoodRelationship.nc_ENODEBID.length()<6 || neighborhoodRelationship.sc_ENODEBID.length()<6){
                    vsDataType = 0;
                    break;
                }
                neighborhoodRelationshipList.add(neighborhoodRelationship);

                vsDataType = 0;

                break;
            default:
                break;
        }

    }


    @Override
    public void characters(char[] ch, int start, int length) throws SAXException {
        stringBuilder1.append(ch,start,length);

        if (stringBuilder1.toString().equals("vsDataENodeBFunction")) {
            vsDataType = 1;     //基站
            function = new Function();
        } else if (stringBuilder1.toString().equals("vsDataEUtranCellFDD")) {
            vsDataType = 2;     //本地小区
            localCell = new LocalCell();
        } else if (stringBuilder1.toString().equals("vsDataEUtranFreqRelation")) {
            vsDataType = 3;     //邻接频点
        } else if (stringBuilder1.toString().equals("vsDataEUtranCellRelation")) {
            vsDataType = 4;     //邻接小区关系
            adjacencyCell = new AdjacencyCell();
            neighborhoodRelationship = new NeighborhoodRelationship();
        }


        switch (vsDataType) {
            case 1:
                if (tagName.equals("es:userLabel") || tagName.equals("es:sctpRef") || tagName.equals("es:eNBId") || tagName.equals("es:mcc") || tagName.equals("es:mnc")) {
                    stringBuilder.append(ch, start, length);
                }
                break;
            case 2:
                if (tagName.equals("es:earfcndl") || tagName.equals("es:cellId") || tagName.equals("es:physicalLayerCellIdGroup") || tagName.equals("es:physicalLayerSubCellId") || tagName.equals("es:tac")) {
                    stringBuilder.append(ch, start, length);
                }
                break;
            case 3:
                break;
            case 4:
                break;
            default:
                break;
        }
    }

    /**
     * 得到地市中文名
     */
    public static String getChineseName(String locationName)
    {
        String lowerCase = locationName.toLowerCase();
        switch(lowerCase)
        {
            case "shenzhen":
                return "深圳";
            case "guangzhou":
                return "广州";
            case "foshan":
                return "佛山";
            case "dongguan":
                return "东莞";
            case "zhongshan":
                return "中山";
            case "huizhou":
                return "惠州";
            case "jiangmen":
                return "江门";
            case "shantou":
                return "汕头";
            case "zhuhai":
                return "珠海";
            case "jieyang":
                return "揭阳";
            case "zhanjiang":
                return "湛江";
            case "maoming":
                return "茂名";
            case "zhaoqing":
                return "肇庆";
            case "qingyuan":
                return "清远";
            case "chaozhou":
                return "潮州";
            case "meizhou":
                return "梅州";
            case "shaoguan":
                return "韶关";
            case "heyuan":
                return "河源";
            case "yangjiang":
                return "阳江";
            case "shanwei":
                return "汕尾";
            case "yunfu":
                return "云浮";
        }
        return "";

    }



}

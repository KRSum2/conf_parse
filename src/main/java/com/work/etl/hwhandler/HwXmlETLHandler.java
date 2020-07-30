package com.work.etl.hwhandler;

import com.conf.parse.etl.hwobjects.*;
import com.conf.parse.etl.obj.*;
import com.work.etl.obj.AdjacencyCell;
import com.work.etl.obj.Function;
import com.work.etl.obj.NeighborhoodRelationship;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import java.util.ArrayList;

public class HwXmlETLHandler extends DefaultHandler
{
    private ArrayList<Function> functionList;
    private ArrayList<HwLocalCell> hwlocalCellList;
    private ArrayList<BTS3900CELLOP> BTS3900CELLOPList;
    private ArrayList<BTS3900CNOPERATORTA> BTS3900CNOPERATORTAList;
    private ArrayList<BTS3900CNOPERATOR> BTS3900CNOPERATORList;
    private ArrayList<AdjacencyCell> adjacencyCellList;
    private ArrayList<NeighborhoodRelationship> relationshiplist;

    private ArrayList<HwLocalCell> hwlocal_list=new ArrayList<>();
    private ArrayList<BTS3900CELLOP> bts3900CELLOP_List=new ArrayList<>();
    private ArrayList<BTS3900CNOPERATORTA> bts3900cnoperatorta_List=new ArrayList<>();
    private ArrayList<BTS3900CNOPERATOR> bts3900cnoperator_List=new ArrayList<>();
    private ArrayList<NeighborhoodRelationship> relationship_List=new ArrayList<>();
    /**
     * 构造函数
     * @param functionList
     * @param bTS3900CELLOPList
     * @param bTS3900CNOPERATORTAList
     * @param bTS3900CNOPERATORList
     * @param adjacencyCellList
     * @param relationshiplist
     */
    public HwXmlETLHandler(ArrayList<Function> functionList, ArrayList<HwLocalCell> hwlocalCellList,
                           ArrayList<BTS3900CELLOP> bTS3900CELLOPList, ArrayList<BTS3900CNOPERATORTA> bTS3900CNOPERATORTAList,
                           ArrayList<BTS3900CNOPERATOR> bTS3900CNOPERATORList,
                           ArrayList<AdjacencyCell> adjacencyCellList, ArrayList<NeighborhoodRelationship> relationshiplist) {
        this.functionList = functionList;
        this.hwlocalCellList = hwlocalCellList;
        BTS3900CELLOPList = bTS3900CELLOPList;
        BTS3900CNOPERATORTAList = bTS3900CNOPERATORTAList;
        BTS3900CNOPERATORList = bTS3900CNOPERATORList;
        this.adjacencyCellList = adjacencyCellList;
        this.relationshiplist = relationshiplist;
    }

    /**
     * 标签对应的常量值
     */
    private static final int TAG_NONE 	= 0;
    private static final int TAG_MO 	= 1;
    private static final int TAG_ATTR   = 2;


    /**
     * 属性值对应的常量值
     */
    private static final int ATTR_NONE 	= 0;
    private static final int ATTR_BTS3900NE 	= 1;
    private static final int ATTR_locationName   = 2;
    private static final int ATTR_vendorName   = 3;
    private static final int ATTR_BTS3900ENODEBFUNCTION=4;
    private static final int ATTR_ENODEBFUNCTIONNAME   = 5;
    private static final int ATTR_ENODEBID=6;
    private static final int ATTR_BTS3900CELL=7;
    private static final int ATTR_CELLID 	= 8;
    private static final int ATTR_LOCALCELLID 	= 9;
    private static final int ATTR_CELLNAME   = 10;
    private static final int ATTR_FDDTDDIND   = 11;
    private static final int ATTR_PHYCELLID=12;
    private static final int ATTR_DLEARFCN   = 13;
    private static final int ATTR_BTS3900CELLOP   = 14;
    private static final int ATTR_TRACKINGAREAID=15;
    private static final int ATTR_BTS3900CNOPERATORTA=16;
    private static final int ATTR_CNOPERATORID=17;
    private static final int ATTR_TAC=18;
    private static final int ATTR_BTS3900CNOPERATOR=19;
    private static final int ATTR_MCC=20;
    private static final int ATTR_MNC=21;
    private static final int ATTR_BTS3900EUTRANEXTERNALCELL=22;
    private static final int ATTR_BTS3900EUTRANINTRAFREQNCELL=23;
    private static final int ATTR_BTS3900EUTRANINTERFREQNCELL=24;

    /**
     * 当前标签，在 startElement 时赋值，在 characters 中使用
     */
    private int currentTag = TAG_NONE;
    private int currentAttr= ATTR_NONE;
    private int parentAttr=ATTR_NONE;

    private static int getTagId(String tag) {
        switch (tag) {
            case "MO":
                return TAG_MO;
            case "attr":
                return TAG_ATTR;
        }
        return TAG_NONE;
    }

    private static int getAtrrValue(String attr) {
        switch (attr) {
            case "BTS3900NE":
                return ATTR_BTS3900NE;
            case "locationName":
                return ATTR_locationName;
            case "vendorName":
                return ATTR_vendorName;
            case "BTS3900ENODEBFUNCTION":
                return ATTR_BTS3900ENODEBFUNCTION;
            case "ENODEBFUNCTIONNAME":
                return ATTR_ENODEBFUNCTIONNAME;
            case "ENODEBID":
                return ATTR_ENODEBID;
            case "BTS3900CELL":
                return ATTR_BTS3900CELL;
            case "CELLID":
                return ATTR_CELLID;
            case "LOCALCELLID":
                return ATTR_LOCALCELLID;
            case "CELLNAME":
                return ATTR_CELLNAME;
            case "FDDTDDIND":
                return ATTR_FDDTDDIND;
            case "PHYCELLID":
                return ATTR_PHYCELLID;
            case "DLEARFCN":
                return ATTR_DLEARFCN;
            case "BTS3900CELLOP":
                return ATTR_BTS3900CELLOP;
            case "TRACKINGAREAID":
                return ATTR_TRACKINGAREAID;
            case "BTS3900CNOPERATORTA":
                return ATTR_BTS3900CNOPERATORTA;
            case "CNOPERATORID":
                return ATTR_CNOPERATORID;
            case "TAC":
                return ATTR_TAC;
            case "BTS3900CNOPERATOR":
                return ATTR_BTS3900CNOPERATOR;
            case "MCC":
                return ATTR_MCC;
            case "MNC":
                return ATTR_MNC;
            case "BTS3900EUTRANEXTERNALCELL":
                return ATTR_BTS3900EUTRANEXTERNALCELL;
            case "BTS3900EUTRANINTERFREQNCELL":
                return ATTR_BTS3900EUTRANINTERFREQNCELL;
            case "BTS3900EUTRANINTRAFREQNCELL":
                return ATTR_BTS3900EUTRANINTRAFREQNCELL;
        }

        return TAG_NONE;
    }



    private Function function=null;
    private HwLocalCell hwlocalCell=null;
    private BTS3900CELLOP bts3900cellop=null;
    private BTS3900CNOPERATORTA bts3900cnoperatorta=null;
    private BTS3900CNOPERATOR bts3900cnoperator=null;
    private AdjacencyCell adjacencyCell=null;
    private NeighborhoodRelationship neighborhoodRelationship=null;

    private StringBuilder sb=new StringBuilder();

    /**
     * 开始解析xml文档
     */
    @Override
    public void startDocument() throws SAXException {

    }

    /**
     * 每当遇到开始标签时执行
     */
    @Override
    public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {

        currentTag=getTagId(qName);
        if(!qName.equals("MOTree"))
        {
            currentAttr=getAtrrValue(attributes.getValue(0));
        }
        sb.setLength(0);
        switch (currentTag) {
            case TAG_MO:
                parentAttr=ATTR_NONE;
                switch (currentAttr) {
                    case ATTR_BTS3900NE:
                        function=new Function();
                        parentAttr=currentAttr;
                        break;
                    case ATTR_BTS3900ENODEBFUNCTION:
                        parentAttr=currentAttr;
                        break;
                    case ATTR_BTS3900CELL:
                        hwlocalCell=new HwLocalCell();
                        parentAttr=currentAttr;
                        break;
                    case ATTR_BTS3900CELLOP:
                        bts3900cellop=new BTS3900CELLOP();
                        parentAttr=currentAttr;
                        break;
                    case ATTR_BTS3900CNOPERATORTA:
                        bts3900cnoperatorta=new BTS3900CNOPERATORTA();
                        parentAttr=currentAttr;
                        break;
                    case ATTR_BTS3900CNOPERATOR:
                        bts3900cnoperator=new BTS3900CNOPERATOR();
                        parentAttr=currentAttr;
                        break;
                    case ATTR_BTS3900EUTRANEXTERNALCELL:
                        adjacencyCell=new AdjacencyCell();
                        parentAttr=currentAttr;
                        break;
                    case ATTR_BTS3900EUTRANINTERFREQNCELL:
                        neighborhoodRelationship=new NeighborhoodRelationship();
                        parentAttr=currentAttr;
                        break;
                    case ATTR_BTS3900EUTRANINTRAFREQNCELL:
                        neighborhoodRelationship=new NeighborhoodRelationship();
                        parentAttr=currentAttr;
                        break;
                    default:
                        break;
                }
                break;
            case TAG_ATTR:
                switch (currentAttr) {
                    case ATTR_locationName:
                        break;

                    default:
                        break;
                }
            default:
                break;
        }

    }

    /**
     * 每当遇到标签中内容时执行
     */
    @Override
    public void characters(char[] ch, int start, int length) throws SAXException {
        sb.append(ch,start,length);
    }

    /**
     * 每当遇到结束标签时执行
     */
    @Override
    public void endElement(String uri, String localName, String qName) throws SAXException {
        currentTag = TAG_NONE;
        switch (getTagId(qName)) {
            case TAG_MO:
                switch (parentAttr) {
                    case ATTR_BTS3900ENODEBFUNCTION:
                        functionList.add(function);
                        break;
                    case ATTR_BTS3900CELL:
                        hwlocal_list.add(hwlocalCell);
                        break;
                    case ATTR_BTS3900CELLOP:
                        bts3900CELLOP_List.add(bts3900cellop);
                        break;
                    case ATTR_BTS3900CNOPERATORTA:
                        bts3900cnoperatorta_List.add(bts3900cnoperatorta);
                        break;
                    case ATTR_BTS3900CNOPERATOR:
                        bts3900cnoperator_List.add(bts3900cnoperator);
                        break;
                    case ATTR_BTS3900EUTRANEXTERNALCELL:
                        adjacencyCellList.add(adjacencyCell);
                        break;
                    case ATTR_BTS3900EUTRANINTERFREQNCELL:
                        relationship_List.add(neighborhoodRelationship);
                        break;
                    case ATTR_BTS3900EUTRANINTRAFREQNCELL:
                        relationship_List.add(neighborhoodRelationship);
                        break;
                    default:
                        break;
                }
            case TAG_ATTR:
                switch (currentAttr) {
                    case ATTR_locationName:
                        if(parentAttr==ATTR_BTS3900NE&&function!=null)
                        {
                            function.sc_LOCATIONNAME=sb.toString();
                        }
                        break;
                    case ATTR_vendorName:
                        if(parentAttr==ATTR_BTS3900NE&&function!=null)
                        {
                            function.sc_VENDORNAME=sb.toString();
                        }
                        break;
                    case ATTR_ENODEBFUNCTIONNAME:
                        if(parentAttr==ATTR_BTS3900ENODEBFUNCTION&&function !=null)
                        {
                            function.sc_ENODEBFUNCTIONNAME=sb.toString();
                        }
                        break;
                    case ATTR_ENODEBID:
                        if(parentAttr==ATTR_BTS3900ENODEBFUNCTION&&function !=null)
                        {
                            function.sc_ENODEBID=sb.toString();
                        }
                        if(parentAttr==ATTR_BTS3900EUTRANEXTERNALCELL&&adjacencyCell!=null)
                        {
                            adjacencyCell.nc_ENODEBID=sb.toString();
                        }
                        if(parentAttr==ATTR_BTS3900EUTRANEXTERNALCELL&&adjacencyCell!=null)
                        {
                            adjacencyCell.nc_ENODEBID=sb.toString();
                        }
                        if(parentAttr==ATTR_BTS3900EUTRANINTERFREQNCELL&&neighborhoodRelationship!=null)
                        {
                            neighborhoodRelationship.nc_ENODEBID=sb.toString();
                        }
                        if(parentAttr==ATTR_BTS3900EUTRANINTRAFREQNCELL&&neighborhoodRelationship!=null)
                        {
                            neighborhoodRelationship.nc_ENODEBID=sb.toString();
                        }
                        break;
                    case ATTR_CELLID:
                        if(parentAttr==ATTR_BTS3900CELL&&hwlocalCell!=null)
                        {
                            hwlocalCell.sc_CELLID=sb.toString();
                        }
                        if(parentAttr==ATTR_BTS3900EUTRANEXTERNALCELL&&adjacencyCell!=null)
                        {
                            adjacencyCell.nc_CELLID=sb.toString();
                        }
                        if(parentAttr==ATTR_BTS3900EUTRANINTERFREQNCELL&&neighborhoodRelationship!=null)
                        {
                            neighborhoodRelationship.nc_CELLID=sb.toString();
                        }
                        if(parentAttr==ATTR_BTS3900EUTRANINTRAFREQNCELL&&neighborhoodRelationship!=null)
                        {
                            neighborhoodRelationship.nc_CELLID=sb.toString();
                        }
                        break;
                    case ATTR_LOCALCELLID:
                        if(parentAttr==ATTR_BTS3900CELL&&hwlocalCell!=null)
                        {
                            hwlocalCell.sc_LOCALCELLID=sb.toString();
                        }
                        if(parentAttr==ATTR_BTS3900CELLOP&&bts3900cellop!=null)
                        {
                            bts3900cellop.sc_LOCALCELLID=sb.toString();
                        }
                        if(parentAttr==ATTR_BTS3900EUTRANINTERFREQNCELL&&neighborhoodRelationship!=null)
                        {
                            neighborhoodRelationship.sc_LOCALCELLID=sb.toString();
                        }
                        if(parentAttr==ATTR_BTS3900EUTRANINTRAFREQNCELL&&neighborhoodRelationship!=null)
                        {
                            neighborhoodRelationship.sc_LOCALCELLID=sb.toString();
                        }
                        break;
                    case ATTR_CELLNAME:
                        if(parentAttr==ATTR_BTS3900CELL&&hwlocalCell!=null)
                        {
                            hwlocalCell.sc_CELLNAME=sb.toString();
                        }
                        break;
                    case ATTR_FDDTDDIND:
                        if(parentAttr==ATTR_BTS3900CELL&&hwlocalCell!=null)
                        {
                            hwlocalCell.sc_FDDTDDIND=sb.toString();
                        }
                        break;
                    case ATTR_PHYCELLID:
                        if(parentAttr==ATTR_BTS3900CELL&&hwlocalCell!=null)
                        {
                            hwlocalCell.sc_PHYCELLID=sb.toString();
                        }
                        if(parentAttr==ATTR_BTS3900EUTRANEXTERNALCELL&&adjacencyCell!=null)
                        {
                            adjacencyCell.nc_PHYCELLID=sb.toString();
                        }
                        break;
                    case ATTR_DLEARFCN:
                        if(parentAttr==ATTR_BTS3900CELL&&hwlocalCell!=null)
                        {
                            hwlocalCell.sc_DLEARFCN=sb.toString();
                        }
                        if(parentAttr==ATTR_BTS3900EUTRANEXTERNALCELL&&adjacencyCell!=null)
                        {
                            adjacencyCell.nc_DLEARFCN=sb.toString();
                        }
                        break;
                    case ATTR_TRACKINGAREAID:
                        if(parentAttr==ATTR_BTS3900CELLOP&&bts3900cellop!=null)
                        {
                            bts3900cellop.TRACKINGAREAID=sb.toString();
                        }
                        if(parentAttr==ATTR_BTS3900CNOPERATORTA&&bts3900cnoperatorta!=null)
                        {
                            bts3900cnoperatorta.TRACKINGAREAID=sb.toString();
                        }
                        break;
                    case ATTR_CNOPERATORID:
                        if(parentAttr==ATTR_BTS3900CNOPERATORTA&&bts3900cnoperatorta!=null)
                        {
                            bts3900cnoperatorta.CNOPERATORID=sb.toString();
                        }
                        if(parentAttr==ATTR_BTS3900CNOPERATOR&&bts3900cnoperator!=null)
                        {
                            bts3900cnoperator.CNOPERATORID=sb.toString();
                        }
                        break;
                    case ATTR_TAC:
                        if(parentAttr==ATTR_BTS3900CNOPERATORTA&&bts3900cnoperatorta!=null)
                        {
                            bts3900cnoperatorta.sc_TAC=sb.toString();
                        }
                        if(parentAttr==ATTR_BTS3900EUTRANEXTERNALCELL&&adjacencyCell!=null)
                        {
                            adjacencyCell.nc_TAC=sb.toString();
                        }
                        break;
                    case ATTR_MCC:
                        if(parentAttr==ATTR_BTS3900CNOPERATOR&&bts3900cnoperator!=null)
                        {
                            bts3900cnoperator.sc_MCC=sb.toString();
                        }
                        if(parentAttr==ATTR_BTS3900EUTRANEXTERNALCELL&&adjacencyCell!=null)
                        {
                            adjacencyCell.nc_MCC=sb.toString();
                        }
                        break;
                    case ATTR_MNC:
                        if(parentAttr==ATTR_BTS3900CNOPERATOR&&bts3900cnoperator!=null)
                        {
                            bts3900cnoperator.sc_MNC=sb.toString();
                        }
                        if(parentAttr==ATTR_BTS3900EUTRANEXTERNALCELL&&adjacencyCell!=null)
                        {
                            adjacencyCell.nc_MNC=sb.toString();
                        }
                        break;
                    default:
                        break;
                }
                break;

            default:
                break;
        }
    }

    /**
     * 结束解析文档
     */
    @Override
    public void endDocument() throws SAXException {
        for(int i=0;i<hwlocal_list.size();i++)
        {
            hwlocal_list.get(i).sc_ENODEBID=function.sc_ENODEBID;
        }
        hwlocalCellList.addAll(hwlocal_list);

        for(int i=0;i<bts3900CELLOP_List.size();i++)
        {
            bts3900CELLOP_List.get(i).sc_ENODEBID=function.sc_ENODEBID;
        }
        BTS3900CELLOPList.addAll(bts3900CELLOP_List);

        for(int i=0;i<bts3900cnoperatorta_List.size();i++)
        {
            bts3900cnoperatorta_List.get(i).sc_ENODEBID=function.sc_ENODEBID;
        }
        BTS3900CNOPERATORTAList.addAll(bts3900cnoperatorta_List);

        for(int i=0;i<bts3900cnoperator_List.size();i++)
        {
            bts3900cnoperator_List.get(i).sc_ENODEBID=function.sc_ENODEBID;
        }
        BTS3900CNOPERATORList.addAll(bts3900cnoperator_List);


        for(int i=0;i<relationship_List.size();i++)
        {
            relationship_List.get(i).sc_ENODEBID=function.sc_ENODEBID;
        }
        relationshiplist.addAll(relationship_List);

    }

}

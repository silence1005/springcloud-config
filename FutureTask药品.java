package com.ufgov.yonyou.service.impl;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.collection.ListUtil;
import cn.hutool.core.date.DateField;
import cn.hutool.core.date.DatePattern;
import cn.hutool.core.date.DateUtil;
import cn.hutool.core.util.StrUtil;
import com.ufgov.yonyou.constant.Constant;
import com.ufgov.yonyou.domain.dto.ErpStatData;
import com.ufgov.yonyou.domain.entity.*;
import com.ufgov.yonyou.repository.*;
import com.ufgov.yonyou.service.WarningDataGenService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import tk.mybatis.mapper.entity.Example;
import tk.mybatis.mapper.weekend.WeekendSqls;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

@Slf4j
@Transactional
@Service
public class WarningDataGenServiceImpl implements WarningDataGenService {

    @Autowired
    private WarningMonitorMapper warningMonitorMapper;

    @Autowired
    private ErpDataMapper erpDataMapper;

    @Autowired
    private TaskMapper taskMapper;

    @Autowired
    YbTransRecordMapper ybTransRecordMapper;

    @Autowired
    private ErrorDataMapper errorDataMapper;

    @Autowired
    private GoodsMapper goodsMapper;

    private Date startDate;

    private Date endDate;

    private String startDateStr;

    private String endDateStr;

    Map<Long,List<Goods>> goodsMap;

    //分组：机构 药品 业务类型 ， 求sl
    List<ErpStatData> erpSumMap;
    //期末库存
    Map<String, List<ErpData>> endStockMap;
    //医保中心结算数据
    Map<String,List<YbTransDetail>> medicalCenters;
    @Autowired
    private ThreadPoolExecutor threadPoolExecutor;

    //总入口
    @Override
    @Transactional
    public void startStat() throws Exception{
        long start = System.currentTimeMillis();
        log.info("******预警任务开始******");
        log.info("******预警任务开始******");
        log.info("******预警任务开始******");

        try {
            //数据组装
            loadDataByDB();

            ExecutorService executorService = Executors.newCachedThreadPool();

            //删除  errorData.  t月删除  t-1月时间范围的预警数据
//            List<ErrorData> needDeleteErrorDatas = errorDataMapper.selectByExample(Example.builder(ErrorData.class).select("id").where(
//                    WeekendSqls.<ErrorData>custom().andEqualTo(ErrorData::getStartDate, this.startDateStr)
//            ).build());

            Example example = Example.builder(ErrorData.class).select("id").build();
            Example.Criteria criteria = example.createCriteria().andEqualTo("startDate", startDateStr);
            List<ErrorData> needDeleteErrorDatas = errorDataMapper.selectByExample(example);

            //删除  warningData.  删除当月的预警
            warningMonitorMapper.deleteByExample(Example.builder(WarningMonitor.class).where(
                    WeekendSqls.<WarningMonitor>custom().andBetween(WarningMonitor::getYjsj,DateUtil.offset(this.startDate,DateField.MONTH,1),
                            DateUtil.offset(endDate,DateField.MONTH,1) )
            ).build());

            //加载任务数据
            List<Task> taskList = taskMapper.select(new Task().setZt(1));

            //跑规则, 生成errorData
            for (Task task : taskList) {
                List<ErrorData> errorDataList = commonRuleDeal(task);
                log.info("正在处理{}任务...",task.getRwmc());
                if (CollectionUtils.isNotEmpty(errorDataList)) {

                    AtomicInteger threadStartCount = new AtomicInteger(0);
                    AtomicInteger threadEndCount = new AtomicInteger(0);

                    List<List<ErrorData>> split = ListUtil.split(errorDataList, 10000);
                    log.info("一共拆分了{}个List",split.size());
                    CountDownLatch latch = new CountDownLatch(split.size());

                    long start2 = System.currentTimeMillis();
                    ArrayList<FutureTask> futureTaskList = new ArrayList<>();
                    for (List<ErrorData> list : split) {
                        FutureTask futureTask = new FutureTask(new saveMonitorCallable(list,latch));
                        futureTaskList.add(futureTask);
                        threadPoolExecutor.execute(futureTask);
                    }
                    latch.await();
                    log.info(task.getRwmc() +"处理完毕. 疑点数据入库耗时: "+ (System.currentTimeMillis() - start2)/1000d+" second, 数据量:{}",errorDataList.size());
                }
            }

            //删除
            if(CollUtil.isNotEmpty(needDeleteErrorDatas)){
                log.info("正在删除历史疑点数据...");
                List<String> idList = needDeleteErrorDatas.stream().map(x -> x.getId()+"").collect(Collectors.toList());
                List<List<String>> split = ListUtil.split(idList,10000);
                log.info("一共拆分了:"+split.size()+"个List");
                for (List<String> strings : split) {
                    //List转Str
                    String idStr = String.join(",", strings);
                    errorDataMapper.deleteByIds(idStr);
                }
            }

            //根据疑点数据 生成预警数据
            log.info("正在生成预警数据...");
            genWanringMonitor();
        } catch (Exception e){
            e.printStackTrace();
            throw new RuntimeException("跑规则失败: "+e);
        }finally {
            log.info("**************************************************");
            log.info("*****************  所有规则运行完毕,累计耗时:{}  *****",(System.currentTimeMillis() - start)/1000l + "秒");
            log.info("**************************************************");

            //this.erpSumMap = null;
        }
    }
    public ErrorData buildErrorData(double offset, Task task, ErpStatData erpData, double doubtfulNum) {
        //偏离值小于等于低风险
        if (offset <= task.getDfxyz()) {
            return null;
        }
        //ErrorData errorData = new ErrorData();
        ErrorData.ErrorDataBuilder builder = ErrorData.builder()
                .cfsj(new Date())
                .dj(erpData.getDj())
                .startDate(startDate)
                .endDate(endDate)
                .rwid(task.getId())
                .cfsj(DateUtil.offset(startDate,DateField.MONTH, 1))
                .spid(Long.valueOf(erpData.getSpid()))
                .jgid(Long.valueOf(erpData.getJgid()))
                .wgsl(doubtfulNum)
                .wgzje(doubtfulNum * erpData.getDj()) //违规总金额 = 违规数量 * 单价
                .jsz(String.valueOf(offset));
        if (offset > task.getGfxyz()) {
            builder.fxjb((Constant.RISK_LEVEL_HIGH));
        } else if (offset > task.getZfxyz()) {
            builder.fxjb((Constant.RISK_LEVEL_MIDDLE));
        } else if (offset > task.getDfxyz()) {
            builder.fxjb((Constant.RISK_LEVEL_LOW));
        }
        return builder.build();
    }


    // //A1	采购//A2	销售退货//A3	调拨入库//A4	报溢//A5	医保销售//A6	自费销售//A7	采购退货//A8	报损//A9	调拨出库//A10	当期库存
    public List<ErrorData> commonRuleDeal(Task task) {
        List<ErrorData> errorDataList = new ArrayList<>();
        for (ErpStatData erpStat: erpSumMap) {
            double doubtfulCount = 0;//疑点数
            double offset = 0; //偏离值

            //报损 报溢 走同一套规则
            if(task.getGzdm().equals(Constant.RULE_REPORT_OVERFLOW_DRINK)
            ||task.getGzdm().equals(Constant.RULE_REPORT_OVERFLOW_EAT)
            ||task.getGzdm().equals(Constant.RULE_REPORT_LOSS_DRINK)
            ||task.getGzdm().equals(Constant.RULE_REPORT_LOSS_EAT)
            ){
                //计算报损时: 药品总数量=期初库存+采购+调入+销售退货-采购退货-调出+报溢
                //计算报溢时: 药品总数量=期初库存+采购+调入+销售退货-采购退货-调出-报损
                //offset = x / 药品总数量

                double baseCount = erpStat.getInitStock() + erpStat.getPurchase() + erpStat.getPutStorage() + erpStat.getSaleReturn() + erpStat.getMedicalSale() - erpStat.getPurchaseReturn()
                        - erpStat.getOutStorage();
                double numerator = 0;//分子
                double denominator = 0;//分母

                //报溢 - 非饮片
                if((task.getGzdm().equals(Constant.RULE_REPORT_OVERFLOW_EAT))){
                    if(!(",1,2,".indexOf(erpStat.getZxybz()) >= 0)){
                        continue;
                    }
                    numerator = erpStat.getReportOverflow();
                    denominator = baseCount - erpStat.getReportLoss();
                }
                //报溢 - 饮片
                else if((task.getGzdm().equals(Constant.RULE_REPORT_OVERFLOW_DRINK))){
                    if(!"3".equals(erpStat.getZxybz())){
                        continue;
                    }
                    numerator = erpStat.getReportOverflow();
                    denominator = baseCount - erpStat.getReportLoss();
                }

                //报损 非饮片
                else if((task.getGzdm().equals(Constant.RULE_REPORT_LOSS_EAT))){
                    if(!(",1,2,".indexOf(erpStat.getZxybz()) >= 0)){
                        continue;
                    }
                    numerator = erpStat.getReportLoss();
                    denominator = baseCount + erpStat.getReportOverflow();
                }
                //报损 - 饮片
                else if((task.getGzdm().equals(Constant.RULE_REPORT_LOSS_DRINK))){
                    if(!"3".equals(erpStat.getZxybz())){
                        continue;
                    }
                    numerator = erpStat.getReportLoss();
                    denominator = baseCount + erpStat.getReportOverflow();
                }

                //分子为空,continue
                if(numerator <= 0){
                    continue;
                }
                //分子不为空, 分母为空 , 分母=分子
                if(numerator > 0 &&  denominator <=0 ){
                    denominator = numerator;
                }

                offset = numerator / denominator;
                doubtfulCount = numerator;
            }

            //库存数量异常 |绝对值|
            else if(task.getGzdm().equals(Constant.RULE_STOCK)){
                //A1采购//A2销售退货//A3调拨入库//A4报溢//A5医保销售//A6自费销售//A7采购退货//A8报损//A9调拨出库//A10当期库存
                //偏离值 = 初期库存+采购 +销售退货 +报溢 +调拨入库 -  (调拨出库 + 期末库存 + 采购退货 + 医保销售 + 自费销售 + 报损 )
                //绝对值!=0 判定为异常！！
//                List<ErpData> erpDataList = endStockMap.get(orgGoodsId);

//                if(CollUtil.isNotEmpty(erpDataList) ){
//                    endStock = erpDataList.get(0).getKc() == null ? 0 : erpDataList.get(0).getKc();
//                }
                offset = erpStat.getInitStock() + erpStat.getPurchase() + erpStat.getSaleReturn() + erpStat.getReportOverflow()+erpStat.getPutStorage()
   - ( erpStat.getOutStorage() +erpStat.getEndStock() + erpStat.getPurchaseReturn() + erpStat.getMedicalSale()+erpStat.getSelfSale() - erpStat.getReportLoss() );
                        //getSlByType(erpStatData, BusinessConstant.INIT_STOCK,BusinessConstant.PURCHASE,BusinessConstant.SALE_RETURN,BusinessConstant.REPORT_OVERFLOW,BusinessConstant.OUT_STORAGE)
                        //- getSlByType(erpStatData,BusinessConstant.PUT_STORAGE,BusinessConstant.MEDICAL_SALE,BusinessConstant.SELF_SALE,BusinessConstant.REPORT_LOSS + endStock);
                offset = Math.abs(offset);
                doubtfulCount = offset;
            }

            //自费、医保柜台销售数据对比   |自费销售 - 医保销售| > 0    绝对值
            //ybfw	药品是否医保范围	Boolean		Y	例：0是，1否
            else if (task.getGzdm().equals(Constant.RULE_SELF_VS_MEDICAL_COMPARE)) {
//                Long spid = erpStatData.get(0).getSpid();
//                List<Goods> goods = goodsMap.get(spid);
//                if(goods !=null && goods.get(0).getYb() == 0 ){//0代表是医保范围
                if(!(erpStat.getYb() == 0)){
                    continue;
                }
                offset = Math.abs( erpStat.getSelfSale() - erpStat.getMedicalSale() );
                doubtfulCount = offset;
            }
//            医保结算异常  (ERP医保销售 - 医保结算) > 0 视为异常  |绝对值|
//            ybfw	药品是否医保范围	Boolean		Y	例：0是，1否
            else if (task.getGzdm().equals(Constant.RULE_MEDICAL_INSURANCE_SETTLE)) {
//                if(medicalCenters == null){
//                    return null;//规则结束
//                }
//                Long spid = erpStatData.get(0).getSpid();
//                List<Goods> goods = goodsMap.get(spid);
                                    //医保范围                and        上海码/统编代码不为空
                //if(goods !=null && goods.get(0).getYb() == 0 &&  !"0".equals(goods.get(0).getTbdm())){
                                                                        //这个>=5是随便给的一个稍微合法的值
                if(erpStat.getYb() == 0 && StrUtil.isNotEmpty(erpStat.getTbdm()) && erpStat.getTbdm().length() >= 5){
                   /*double orgSale = getSlByType(erpStatData, BusinessConstant.MEDICAL_SALE);
                    List<YbTransDetail> settlements = medicalCenters.get(orgGoodsId);

                    //偏离值默认等于药店销量
                    offset = orgSale;
                    if ( CollUtil.isNotEmpty(settlements) ) {
                        offset = Math.abs( settlements.get(0).getSl().subtract(new BigDecimal(orgSale)).doubleValue() );
                    }*/
                    offset = erpStat.getMedicalSale() - erpStat.getRealYbJs();
                    doubtfulCount = Math.abs(offset);
                }else{
                    continue;
                }
            }

            ErrorData errorData = buildErrorData(offset, task, erpStat, doubtfulCount);
            if(errorData != null){
                errorDataList.add(errorData);
            }
        }
        return errorDataList;
    }


    //数据组装
    private void loadDataByDB() {
        long start = System.currentTimeMillis();

        //上个月
        this.startDate = DateUtil.beginOfMonth(DateUtil.lastMonth());;
        this.endDate = DateUtil.endOfMonth(DateUtil.lastMonth()).offset(DateField.MILLISECOND,-999);;

        //上上个月
        this.startDate = DateUtil.offset(startDate,DateField.MONTH,-1);
        this.endDate = DateUtil.offset(endDate,DateField.MONTH,-1);

        this.startDateStr = startDate.toString();
        this.endDateStr = endDate.toString();

        //得到下个月的表名
        String nextTableName = "erp_data_"+DateUtil.parse(startDateStr).offset(DateField.MONTH,1).toString(DatePattern.SIMPLE_MONTH_PATTERN);
        String nextYbTableName = "yb_trans_detail_"+DateUtil.parse(startDateStr).toString(DatePattern.SIMPLE_MONTH_PATTERN);


        //加载所有业务类型分组总和
        if(erpSumMap==null) //测试 记得删除
        erpSumMap = erpDataMapper.queryGroupDataByTimeAndType(startDateStr,endDateStr,null,null,nextTableName,nextYbTableName);

        System.out.println((System.currentTimeMillis() - start) / 1000d + "加载上个月数据分组求和 second");
        //查出每个机构下 每个药品 每个业务类型 数量总和
//        erpSumMap = erpDataList.stream().collect(Collectors.groupingBy(x->x.getJgid()+"_"+x.getSpid()));

        //加载期末库存
//        Example example = new Example(ErpData.class);
//        DateTime nextMonthStart = DateUtil.offset(startDate, DateField.MONTH, 1);
//        DateTime nextMonthEnd = DateUtil.offset(nextMonthStart, DateField.DAY_OF_MONTH, 2);
//        example.selectProperties("jgid","spid","ywlx","dj","sl").createCriteria().andBetween("businessTime",nextMonthStart,nextMonthEnd).andEqualTo("ywlx","A10");
//        long start1 = System.currentTimeMillis();
//        endStockMap = erpDataMapper.selectByExample(example).stream().collect(Collectors.groupingBy(x -> x.getJgid() + "_" + x.getSpid()));
//        long end1 = System.currentTimeMillis();
//        log.info("加载期末库存耗时： " + (end1 - start1)  + " 毫秒");

        //加载医保中心结算数据   medicalCenter
//        List<YbTransDetail> medicals = ybTransRecordMapper.findYBTransSumSLGroupBy(startDateStr, endDateStr);
//        if (CollUtil.isNotEmpty(medicals)) {
//            medicalCenters = medicals.stream().collect(Collectors.groupingBy(x -> x.getJgid() + "_" + x.getSpid()));
//        }
//
//        //加载药品
//        //goodsMap = goodsMapper.selectByExample(Example.builder(Goods.class).select("id","zxybz" ,"yb" ,"id").build()).stream().collect(Collectors.groupingBy(Goods::getId));
//        goodsMap = RedisTask.getGoods().stream().collect(Collectors.groupingBy(x->x.getId()));
        System.out.println("loadDataByDB 共耗时: "+(System.currentTimeMillis() - start)/1000d+" second. ");
    }

    //所有统计走完后, 查error_Data 得到预警数据
    private void genWanringMonitor() throws Exception{
        // 高指数=高异常条数*1
//        中指数=中异常条数*0.5
//        低指数=低异常条数*0.1

//        风险值=高+中+低(指数)

//        红：风险值>800
//        橙：风险值>=400
//        黄：风险值<400

        //条数*0.1/0.5/1 = 指数
        //风险值 = 高+中+低(指数)

        //查询 每个机构下的 违规数量
        List<Map> errorDataByOrgGroup = errorDataMapper.getErrorDataByOrgGroup(startDateStr, endDateStr);
        Map<Object, List<Map>> jgidToJgMap = errorDataByOrgGroup.stream().collect(Collectors.groupingBy(x -> x.get("JGID")));
        ArrayList<WarningMonitor> wmList = new ArrayList<>();
        //erpSumMap = erpDataList.stream().collect(Collectors.groupingBy(ErpStatData::getYwlx));;
        for (Object o : jgidToJgMap.keySet()) {
            List<Map> maps = jgidToJgMap.get(o);
            double riskValue = 0d;
            int riskLevel;
            //指数 累加
            Double highIndex = 0d;
            Double midIndex = 0d;
            Double lowIndex = 0d;
            for (Map map : maps) {
                double sl = (double) map.get("SL");
                //风险级别 2高，1中，0低
                int fxjb = (Integer) map.get("FXJB");

                if (fxjb == 2) {//高
                    highIndex += Double.valueOf(sl * 1);
                } else if (fxjb == 1) {//中
                    midIndex += Double.valueOf(sl * 0.5);
                } else if (fxjb == 0) {//低
                    lowIndex += Double.valueOf(sl * 0.1);
                }
            }
            //一个机构所有的风险级别全部遍历完毕 , 插入该机构的预警数据
            //风险级别
            riskValue = lowIndex + midIndex + highIndex;
            WarningMonitor wm = new WarningMonitor();
            Long jgid = (Long) o;
            wm.setJgid(jgid.longValue());
            wm.setFxz(riskValue);//风险值
            wm.setGfx(highIndex);//高风险指数
            wm.setZfx(midIndex);//中风险指数
            wm.setDfx(lowIndex);//中风险指数
            wm.setYjsj(DateUtil.offset(startDate, DateField.MONTH, 1));//erp时间的下个月
            wm.setYjdj(riskValue >= 800 ? 2 : riskValue >= 400 ? 1 : 0); //预警等级 0黄;1橙;2红
            wm.setStatus(1);//任务状态 1未处理 2待确认 3待复核 4已复核
            wm.setClzt(0);//处理状态 (0新增 1暂不处理,2建议检查,3重点关注
            wm.setErpTimeStart(startDateStr);
            wm.setErpTimeEnd(endDateStr);
            wm.setLastModifiedVersion(0);
            wmList.add(wm);
            //warningMonitorMapper.insert(wm);
        }
        if (CollectionUtils.isEmpty(wmList)) {
            return;
        }
        //List<WarningMonitor> collect = wmList.stream().sorted(Comparator.comparing(WarningMonitor::getFxz)).collect(Collectors.toList());
        wmList.sort(Comparator.comparing(x->x.getFxz()));
        int i = wmList.size();
        for (WarningMonitor warningMonitor : wmList) {
            warningMonitor.setTop(i--);
        }
        warningMonitorMapper.insertListByMySql(wmList);
    }

    //获取某个业务类型的数量
//    public static double getSlByType(List<ErpStatData> list,String... types){
//        BigDecimal count = new BigDecimal(0);
//        for (ErpStatData erpStatData : list) {
//            for (String type : types) {
//                if(erpStatData.getYwlx().equals(type)){
//                    count = count.add(BigDecimal.valueOf(erpStatData.getSl()));
//                }
//            }
//        }
//        return count.doubleValue();
//    }

}

class saveMonitorCallable implements Callable<Integer>{
    List<ErrorData> list;
    CountDownLatch latch;
    public saveMonitorCallable(List<ErrorData> list,CountDownLatch latch){
        this.list = list;
        this.latch = latch;
    }

    @Autowired
    ErrorDataMapper monitoringRecordMapper;

    @Override
    public Integer call() throws Exception {
        try{
            return monitoringRecordMapper.insertList(list);
        }finally {
            latch.countDown();
        }
    }
}

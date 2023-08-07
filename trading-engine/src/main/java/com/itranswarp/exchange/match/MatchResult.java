package com.itranswarp.exchange.match;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import com.itranswarp.exchange.model.trade.OrderEntity;

/**
 * 撮合结果
 * 一个taker单可能对应多个maker单
 */
public class MatchResult {

    public final OrderEntity takerOrder; //吃单
    public final List<MatchDetailRecord> matchDetails = new ArrayList<>(); // 撮合的匹配记录列表

    public MatchResult(OrderEntity takerOrder) {
        this.takerOrder = takerOrder;
    }

    /**
     * 写入一条撮合的匹配记录
     * @param price 价格
     * @param matchedQuantity 数量
     * @param makerOrder 挂单
     */
    public void add(BigDecimal price, BigDecimal matchedQuantity, OrderEntity makerOrder) {
        matchDetails.add(new MatchDetailRecord(price, matchedQuantity, this.takerOrder, makerOrder));
    }

    @Override
    public String toString() {
        if (matchDetails.isEmpty()) {
            return "no matched.";
        }
        return matchDetails.size() + " matched: "
                + String.join(", ", this.matchDetails.stream().map(MatchDetailRecord::toString).toArray(String[]::new));
    }
}

package com.itranswarp.exchange.clearing;

import java.math.BigDecimal;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.itranswarp.exchange.assets.AssetService;
import com.itranswarp.exchange.assets.Transfer;
import com.itranswarp.exchange.enums.AssetEnum;
import com.itranswarp.exchange.match.MatchDetailRecord;
import com.itranswarp.exchange.match.MatchResult;
import com.itranswarp.exchange.model.trade.OrderEntity;
import com.itranswarp.exchange.order.OrderService;
import com.itranswarp.exchange.support.LoggerSupport;

/**
 * 清算系统
 */
@Component
public class ClearingService extends LoggerSupport {

    // 引用资产和订单系统
    final AssetService assetService;

    final OrderService orderService;

    public ClearingService(@Autowired AssetService assetService, @Autowired OrderService orderService) {
        this.assetService = assetService;
        this.orderService = orderService;
    }

    /**
     * 清算撮合引擎的输出
     * @param result
     */
    public void clearMatchResult(MatchResult result) {
        OrderEntity taker = result.takerOrder;
        // taker分为买和卖两种
        switch (taker.direction) {
        case BUY -> {
            // 买入时，按Maker的价格成交：
            // 遍历撮合结果的所有匹配记录（taker对应的所有maker）
            for (MatchDetailRecord detail : result.matchDetails) {
                // debug日志
                if (logger.isDebugEnabled()) {
                    logger.debug(
                            "clear buy matched detail: price = {}, quantity = {}, takerOrderId = {}, makerOrderId = {}, takerUserId = {}, makerUserId = {}",
                            detail.price(), detail.quantity(), detail.takerOrder().id, detail.makerOrder().id,
                            detail.takerOrder().userId, detail.makerOrder().userId);
                }
                // 从匹配记录中读卖家挂单
                OrderEntity maker = detail.makerOrder();
                // 数量
                BigDecimal matched = detail.quantity();
                /** 【注意】：对Taker买入成交的订单，成交价格是按照Maker的报价成交的，而Taker冻结的金额是按照Taker订单的报价冻结的
                     因此，解冻后，部分差额要退回至Taker可用余额
                 **/
                if (taker.price.compareTo(maker.price) > 0) {
                    // 实际买入价比报价低，部分USD退回账户:
                    BigDecimal unfreezeQuote = taker.price.subtract(maker.price).multiply(matched);
                    logger.debug("unfree extra unused quote {} back to taker user {}", unfreezeQuote, taker.userId);
                    assetService.unfreeze(taker.userId, AssetEnum.USD, unfreezeQuote);
                }
                // 买家出USD，换卖家的BTC
                // 买方USD转入卖方账户:
                assetService.transfer(Transfer.FROZEN_TO_AVAILABLE, taker.userId, maker.userId, AssetEnum.USD,
                        maker.price.multiply(matched));
                // 卖方BTC转入买方账户:
                assetService.transfer(Transfer.FROZEN_TO_AVAILABLE, maker.userId, taker.userId, AssetEnum.BTC, matched);
                // 删除完全成交的Maker:
                if (maker.unfilledQuantity.signum() == 0) {
                    orderService.removeOrder(maker.id);
                }
            }
            // 删除完全成交的Taker:
            if (taker.unfilledQuantity.signum() == 0) {
                orderService.removeOrder(taker.id);
            }
        }
        case SELL -> {
            // taker是卖单，以maker的价格成交
            for (MatchDetailRecord detail : result.matchDetails) {
                if (logger.isDebugEnabled()) {
                    logger.debug(
                            "clear sell matched detail: price = {}, quantity = {}, takerOrderId = {}, makerOrderId = {}, takerUserId = {}, makerUserId = {}",
                            detail.price(), detail.quantity(), detail.takerOrder().id, detail.makerOrder().id,
                            detail.takerOrder().userId, detail.makerOrder().userId);
                }
                // maker是买家挂单
                OrderEntity maker = detail.makerOrder();
                BigDecimal matched = detail.quantity();
                // 卖家出BTC，换买家的USD
                /**【注意】这里不用考虑差价，因为taker是卖单时，以买单的maker.price成交。
                    设计下单时，卖单冻结的是BTC，换成USD直接按挂单价maker.price换算即可。
                    并不关心taker.price
                    前面买单考虑差价是因为冻结的是USD，最终交易的也是USD，即taker.price与maker.price存在差价
                 **/
                // 卖方BTC转入买方账户:
                assetService.transfer(Transfer.FROZEN_TO_AVAILABLE, taker.userId, maker.userId, AssetEnum.BTC, matched);
                // 买方USD转入卖方账户:
                assetService.transfer(Transfer.FROZEN_TO_AVAILABLE, maker.userId, taker.userId, AssetEnum.USD,
                        maker.price.multiply(matched));
                // 删除完全成交的Maker:
                if (maker.unfilledQuantity.signum() == 0) {
                    orderService.removeOrder(maker.id);
                }
            }
            // 删除完全成交的Taker:
            if (taker.unfilledQuantity.signum() == 0) {
                orderService.removeOrder(taker.id);
            }
        }
        default -> throw new IllegalArgumentException("Invalid direction.");
        }
    }

    public void clearCancelOrder(OrderEntity order) {
        switch (order.direction) {
        case BUY -> {
            // 解冻USD = 价格 x 未成交数量
            assetService.unfreeze(order.userId, AssetEnum.USD, order.price.multiply(order.unfilledQuantity));
        }
        case SELL -> {
            // 解冻BTC = 未成交数量
            assetService.unfreeze(order.userId, AssetEnum.BTC, order.unfilledQuantity);
        }
        default -> throw new IllegalArgumentException("Invalid direction.");
        }
        // 从OrderService中删除订单:
        orderService.removeOrder(order.id);
    }
}

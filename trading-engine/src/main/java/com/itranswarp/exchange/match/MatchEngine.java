package com.itranswarp.exchange.match;

import java.math.BigDecimal;

import org.springframework.stereotype.Component;

import com.itranswarp.exchange.bean.OrderBookBean;
import com.itranswarp.exchange.enums.Direction;
import com.itranswarp.exchange.enums.OrderStatus;
import com.itranswarp.exchange.model.trade.OrderEntity;

/**
 * 撮合引擎
 */
@Component
public class MatchEngine {

    public final OrderBook buyBook = new OrderBook(Direction.BUY); // 买盘
    public final OrderBook sellBook = new OrderBook(Direction.SELL); // 卖盘
    public BigDecimal marketPrice = BigDecimal.ZERO; // 最新市场价
    private long sequenceId; // 上次处理的Sequence ID

    /**
     * 撮合
     * @param sequenceId
     * @param order
     * @return
     */
    public MatchResult processOrder(long sequenceId, OrderEntity order) {
        return switch (order.direction) {
            // 买单与sellBook匹配，（如果还有剩）最后放入buyBook:
        case BUY -> processOrder(sequenceId, order, this.sellBook, this.buyBook);
        // 卖单与buyBook匹配，（如果还有剩）最后放入sellBook:
        case SELL -> processOrder(sequenceId, order, this.buyBook, this.sellBook);
        default -> throw new IllegalArgumentException("Invalid direction.");
        };
    }

    /**
     * @param sequenceId 定序id
     * @param takerOrder  当前正在处理的订单
     * @param makerBook   尝试匹配成交的OrderBook（对手盘）
     * @param anotherBook 未能完全成交后挂单的OrderBook
     * @return 成交结果
     */
    private MatchResult processOrder(long sequenceId, OrderEntity takerOrder, OrderBook makerBook,
            OrderBook anotherBook) {
        this.sequenceId = sequenceId;
        long ts = takerOrder.createdAt;
        MatchResult matchResult = new MatchResult(takerOrder);
        BigDecimal takerUnfilledQuantity = takerOrder.quantity;
        // 一直循环，每次取maker（对手盘）第一个，目标是将taker完全匹配完
        for (;;) {
            OrderEntity makerOrder = makerBook.getFirst();
            if (makerOrder == null) {
                // 对手盘不存在:
                break;
            }
            if (takerOrder.direction == Direction.BUY && takerOrder.price.compareTo(makerOrder.price) < 0) {
                // 买入订单价格比卖盘第一档价格低:
                break;
            } else if (takerOrder.direction == Direction.SELL && takerOrder.price.compareTo(makerOrder.price) > 0) {
                // 卖出订单价格比买盘第一档价格高:
                break;
            }
            // 以Maker价格成交,则市场价更新
            this.marketPrice = makerOrder.price;
            // 待成交数量为两者较小值:
            BigDecimal matchedQuantity = takerUnfilledQuantity.min(makerOrder.unfilledQuantity);
            // 成交记录（价格、数量、maker）写入撮合匹配列表
            matchResult.add(makerOrder.price, matchedQuantity, makerOrder);
            // 更新成交后的订单数量 = taker数量 减去 待成交数量
            takerUnfilledQuantity = takerUnfilledQuantity.subtract(matchedQuantity);
            BigDecimal makerUnfilledQuantity = makerOrder.unfilledQuantity.subtract(matchedQuantity);
            // 对手盘完全成交后（即看成交后的订单数是否为0），从订单簿中删除:
            if (makerUnfilledQuantity.signum() == 0) {
                makerOrder.updateOrder(makerUnfilledQuantity, OrderStatus.FULLY_FILLED, ts);
                makerBook.remove(makerOrder);
            } else {
                // 对手盘部分成交:
                makerOrder.updateOrder(makerUnfilledQuantity, OrderStatus.PARTIAL_FILLED, ts);
            }
            // Taker订单完全成交后，退出循环:
            if (takerUnfilledQuantity.signum() == 0) {
                takerOrder.updateOrder(takerUnfilledQuantity, OrderStatus.FULLY_FILLED, ts);
                break;
            }
        } // end for
        // Taker订单未完全成交时，放入买/卖订单簿:
        if (takerUnfilledQuantity.signum() > 0) {
            takerOrder.updateOrder(takerUnfilledQuantity,
                    takerUnfilledQuantity.compareTo(takerOrder.quantity) == 0 ? OrderStatus.PENDING
                            : OrderStatus.PARTIAL_FILLED,
                    ts);
            anotherBook.add(takerOrder);
        }
        return matchResult;
    }

    public void cancel(long ts, OrderEntity order) {
        OrderBook book = order.direction == Direction.BUY ? this.buyBook : this.sellBook;
        if (!book.remove(order)) {
            throw new IllegalArgumentException("Order not found in order book.");
        }
        OrderStatus status = order.unfilledQuantity.compareTo(order.quantity) == 0 ? OrderStatus.FULLY_CANCELLED
                : OrderStatus.PARTIAL_CANCELLED;
        order.updateOrder(order.unfilledQuantity, status, ts);
    }

    public OrderBookBean getOrderBook(int maxDepth) {
        return new OrderBookBean(this.sequenceId, this.marketPrice, this.buyBook.getOrderBook(maxDepth),
                this.sellBook.getOrderBook(maxDepth));
    }

    public void debug() {
        System.out.println("---------- match engine ----------");
        System.out.println(this.sellBook);
        System.out.println("  ----------");
        System.out.println("  " + this.marketPrice);
        System.out.println("  ----------");
        System.out.println(this.buyBook);
        System.out.println("---------- // match engine ----------");
    }
}

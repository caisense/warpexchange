package com.itranswarp.exchange.sequencer;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.itranswarp.exchange.message.event.AbstractEvent;
import com.itranswarp.exchange.messaging.MessageTypes;
import com.itranswarp.exchange.model.trade.EventEntity;
import com.itranswarp.exchange.model.trade.UniqueEventEntity;
import com.itranswarp.exchange.support.AbstractDbService;

/**
 * Process events as batch.
 */
@Component
@Transactional(rollbackFor = Throwable.class)
public class SequenceHandler extends AbstractDbService {

    private long lastTimestamp = 0;

    /**
     * Set sequence for each message, persist into database as batch.
     * 是真正写入Sequence ID并落库的过程
     * @return Sequenced messages.
     */
    public List<AbstractEvent> sequenceMessages(final MessageTypes messageTypes, final AtomicLong sequence,
            final List<AbstractEvent> messages) throws Exception {
        final long t = System.currentTimeMillis();
        // 防止时钟回退
        if (t < this.lastTimestamp) {
            logger.warn("[Sequence] current time {} is turned back from {}!", t, this.lastTimestamp);
        } else {
            this.lastTimestamp = t;
        }
        // UniqueEventEntity列表，利用它去重
        List<UniqueEventEntity> uniques = null;
        Set<String> uniqueKeys = null;
        // 结果集
        List<AbstractEvent> sequencedMessages = new ArrayList<>(messages.size());
        // 入库的eventList
        List<EventEntity> events = new ArrayList<>(messages.size());
        // 主循环：遍历每条消息
        for (AbstractEvent message : messages) {
            UniqueEventEntity unique = null;
            // uniqueId是消息中的全局唯一标识
            final String uniqueId = message.uniqueId;
            // check uniqueId:
            // 在【内存】或【数据库】中查找uniqueId，看是否存在
            // 存在则跳过
            // （外层加了synchronized，因此没有并发问题）
            if (uniqueId != null) {
                if ((uniqueKeys != null && uniqueKeys.contains(uniqueId))
                        || db.fetch(UniqueEventEntity.class, uniqueId) != null) {
                    logger.warn("ignore processed unique message: {}", message);
                    continue;
                }
                unique = new UniqueEventEntity();
                unique.uniqueId = uniqueId;
                unique.createdAt = message.createdAt;
                if (uniques == null) {
                    uniques = new ArrayList<>();
                }
                uniques.add(unique);
                if (uniqueKeys == null) {
                    uniqueKeys = new HashSet<>();
                }
                uniqueKeys.add(uniqueId);
                logger.info("unique event {} sequenced.", uniqueId);
            }

            // 生成上次定序id
            final long previousId = sequence.get();
            // 生成本次定序id，自增1
            final long currentId = sequence.incrementAndGet();

            // 先设置message的sequenceId / previouseId / createdAt，再序列化并落库:
            message.sequenceId = currentId;
            message.previousId = previousId;
            message.createdAt = this.lastTimestamp;

            // 如果此消息关联了UniqueEvent，给UniqueEvent加上相同的sequenceId：
            if (unique != null) {
                unique.sequenceId = message.sequenceId;
            }

            // create AbstractEvent and save to db later:
            // 准备【批量】入库的eventList
            EventEntity event = new EventEntity();
            event.previousId = previousId;
            event.sequenceId = currentId;

            // 将数据json序列化,格式:【类型】+ "#" +【json】
            event.data = messageTypes.serialize(message);
            event.createdAt = this.lastTimestamp; // same as message.createdAt
            events.add(event);

            // will send later:
            sequencedMessages.add(message);
        }

        // UniqueEvent 和 Event 批量入库
        if (uniques != null) {
            db.insert(uniques);
        }
        db.insert(events);
        return sequencedMessages;
    }

    public long getMaxSequenceId() {
        // 获取当前库里最大的id
        EventEntity last = db.from(EventEntity.class).orderBy("sequenceId").desc().first();
        if (last == null) {
            logger.info("no max sequenceId found. set max sequenceId = 0.");
            return 0;
        }
        this.lastTimestamp = last.createdAt;
        logger.info("find max sequenceId = {}, last timestamp = {}", last.sequenceId, this.lastTimestamp);
        return last.sequenceId;
    }
}

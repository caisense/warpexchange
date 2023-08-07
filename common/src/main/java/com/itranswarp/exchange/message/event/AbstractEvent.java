package com.itranswarp.exchange.message.event;

import org.springframework.lang.Nullable;

import com.itranswarp.exchange.message.AbstractMessage;

public class AbstractEvent extends AbstractMessage {

    /**
     * Message id, set after sequenced.
     * 定序后的sequenceId
     */
    public long sequenceId;

    /**
     * Previous message sequence id.
     * 定序后的Previous sequenceId
     */
    public long previousId;

    /**
     * Unique ID or null if not set.
     * 可选的全局唯一标识
     */
    @Nullable
    public String uniqueId;
}

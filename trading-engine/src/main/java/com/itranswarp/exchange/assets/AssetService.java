package com.itranswarp.exchange.assets;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.springframework.stereotype.Component;

import com.itranswarp.exchange.enums.AssetEnum;
import com.itranswarp.exchange.support.LoggerSupport;

// 对AssetService进行写操作必须是单线程，不支持多线程调用tryTransfer()。

@Component
public class AssetService extends LoggerSupport {

    // 使用ConcurrentMap并不是为了让多线程并发写入，因为AssetService中并没有任何同步锁。
    // UserId -> Map(AssetEnum -> Assets[available/frozen])
    final ConcurrentMap<Long, ConcurrentMap<AssetEnum, Asset>> userAssets = new ConcurrentHashMap<>();

    /**
     * 根据用户id和资产id查资产
     * @param userId
     * @param assetId
     * @return
     */
    public Asset getAsset(Long userId, AssetEnum assetId) {
        ConcurrentMap<AssetEnum, Asset> assets = userAssets.get(userId);
        if (assets == null) {
            return null;
        }
        return assets.get(assetId);
    }

    /**
     * 根据用户id查所有类型资产
     * @param userId
     * @return
     */
    public Map<AssetEnum, Asset> getAssets(Long userId) {
        Map<AssetEnum, Asset> assets = userAssets.get(userId);
        if (assets == null) {
            return Map.of();
        }
        return assets;
    }

    public ConcurrentMap<Long, ConcurrentMap<AssetEnum, Asset>> getUserAssets() {
        return this.userAssets;
    }

    /**
     * 冻结
     * @param userId
     * @param assetId
     * @param amount
     * @return
     */
    public boolean tryFreeze(Long userId, AssetEnum assetId, BigDecimal amount) {
        boolean ok = tryTransfer(Transfer.AVAILABLE_TO_FROZEN, userId, userId, assetId, amount, true);
        if (ok && logger.isDebugEnabled()) {
            logger.debug("freezed user {}, asset {}, amount {}", userId, assetId, amount);
        }
        return ok;
    }

    /**
     * 解冻
     * @param userId
     * @param assetId
     * @param amount
     */
    public void unfreeze(Long userId, AssetEnum assetId, BigDecimal amount) {
        if (!tryTransfer(Transfer.FROZEN_TO_AVAILABLE, userId, userId, assetId, amount, true)) {
            throw new RuntimeException(
                    "Unfreeze failed for user " + userId + ", asset = " + assetId + ", amount = " + amount);
        }
        if (logger.isDebugEnabled()) {
            logger.debug("unfreezed user {}, asset {}, amount {}", userId, assetId, amount);
        }
    }

    /**
     * 转账
     * （只有清算时才 调用）
     * @param type
     * @param fromUser
     * @param toUser
     * @param assetId
     * @param amount
     */
    public void transfer(Transfer type, Long fromUser, Long toUser, AssetEnum assetId, BigDecimal amount) {
        if (!tryTransfer(type, fromUser, toUser, assetId, amount, true)) {
            throw new RuntimeException("Transfer failed for " + type + ", from user " + fromUser + " to user " + toUser
                    + ", asset = " + assetId + ", amount = " + amount);
        }
        if (logger.isDebugEnabled()) {
            logger.debug("transfer asset {}, from {} => {}, amount {}", assetId, fromUser, toUser, amount);
        }
    }

    /**
     * 尝试转账（注意保证所有用户资产的各余额总和为0
     * @param type 转账类型
     * @param fromUser 源账户
     * @param toUser 目标账户
     * @param assetId 资产类型
     * @param amount 数额
     * @param checkBalance 是否检查余额
     * @return
     */
    public boolean tryTransfer(Transfer type, Long fromUser, Long toUser, AssetEnum assetId, BigDecimal amount,
            boolean checkBalance) {
        if (amount.signum() == 0) {
            return true;
        }
        // 转账金额不能为负:
        if (amount.signum() < 0) {
            throw new IllegalArgumentException("Negative amount");
        }
        // 获取源用户资产:
        Asset fromAsset = getAsset(fromUser, assetId);
        if (fromAsset == null) {
            // 资产不存在时初始化用户资产:
            fromAsset = initAssets(fromUser, assetId);
        }
        // 获取目标用户资产:
        Asset toAsset = getAsset(toUser, assetId);
        if (toAsset == null) {
            // 资产不存在时初始化用户资产:
            toAsset = initAssets(toUser, assetId);
        }
        // 直接return，用switch表达式
        // 新语法不用break
        return switch (type) {
        case AVAILABLE_TO_AVAILABLE -> {
            // 需要检查余额且余额不足:
            if (checkBalance && fromAsset.available.compareTo(amount) < 0) {
                // 转账失败
                yield false;
            }
            // 源用户的可用资产减少:
            fromAsset.available = fromAsset.available.subtract(amount);
            // 目标用户的可用资产增加:
            toAsset.available = toAsset.available.add(amount);
            yield true;
        }
        case AVAILABLE_TO_FROZEN -> {
            // 需要检查余额且余额不足:
            if (checkBalance && fromAsset.available.compareTo(amount) < 0) {
                yield false;
            }
            fromAsset.available = fromAsset.available.subtract(amount);
            toAsset.frozen = toAsset.frozen.add(amount);
            yield true;
        }
        case FROZEN_TO_AVAILABLE -> {
            // 需要检查余额且余额不足:
            if (checkBalance && fromAsset.frozen.compareTo(amount) < 0) {
                yield false;
            }
            fromAsset.frozen = fromAsset.frozen.subtract(amount);
            toAsset.available = toAsset.available.add(amount);
            yield true;
        }
        default -> {
            throw new IllegalArgumentException("invalid type: " + type);
        }
        };
    }

    private Asset initAssets(Long userId, AssetEnum assetId) {
        ConcurrentMap<AssetEnum, Asset> map = userAssets.get(userId);
        if (map == null) {
            map = new ConcurrentHashMap<>();
            userAssets.put(userId, map);
        }
        Asset zeroAsset = new Asset();
        map.put(assetId, zeroAsset);
        return zeroAsset;
    }

    public void debug() {
        System.out.println("---------- assets ----------");
        List<Long> userIds = new ArrayList<>(userAssets.keySet());
        Collections.sort(userIds);
        for (Long userId : userIds) {
            System.out.println("  user " + userId + " ----------");
            Map<AssetEnum, Asset> assets = userAssets.get(userId);
            List<AssetEnum> assetIds = new ArrayList<>(assets.keySet());
            Collections.sort(assetIds);
            for (AssetEnum assetId : assetIds) {
                System.out.println("    " + assetId + ": " + assets.get(assetId));
            }
        }
        System.out.println("---------- // assets ----------");
    }
}
